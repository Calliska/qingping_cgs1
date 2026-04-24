"""Server-side handler for Qingping CGDN1 private MQTT protocol."""
from __future__ import annotations

import asyncio
import json
import logging
import time
from datetime import timedelta
from typing import Any

from homeassistant.components import mqtt
from homeassistant.config_entries import ConfigEntry
from homeassistant.const import CONF_MAC, CONF_MODEL, CONF_NAME, UnitOfTemperature
from homeassistant.core import HomeAssistant
from homeassistant.helpers.event import async_track_time_interval

from .const import (
    CONF_BLE_DEVICES,
    CONF_CO2_SAMPLING_INTERVAL,
    CONF_COLLECT_INTERVAL,
    CONF_CONFIGURE_GATEWAY_ON_RECONNECT,
    CONF_CONFIGURE_GATEWAY_ON_START,
    CONF_PM_SAMPLING_INTERVAL,
    CONF_REPORT_INTERVAL,
    CONF_TEMPERATURE_UNIT,
    CONF_TEMPORARY_REPORT_DURATION,
    CONF_TEMPORARY_REPORT_INTERVAL,
    CONF_TEMPORARY_REPORT_REFRESH,
    CONF_TIMEZONE,
    DEFAULT_TEMPORARY_REPORT_DURATION,
    DEFAULT_TEMPORARY_REPORT_INTERVAL,
    DEFAULT_TEMPORARY_REPORT_REFRESH,
    DOMAIN,
    SENSOR_DATA_STALE_SECONDS,
)
from .qingping_private import (
    build_diagnostic_report_payload,
    build_type_11_payload,
    build_type_12_payload,
    build_type_17_gateway_settings_payload,
    build_type_18_ack_payload,
    build_type_26_payload,
    down_topic,
    maybe_normalize_mac,
    normalize_mac,
    result_description,
)

_LOGGER = logging.getLogger(__name__)
MQTT_READY_RETRIES = 5
MQTT_READY_DELAY = 1


def _merged_config(config_entry: ConfigEntry) -> dict[str, Any]:
    """Return config entry data overlaid with options."""
    return {**dict(config_entry.data), **dict(config_entry.options)}


def _parse_ble_devices(value: Any) -> list[dict[str, str]]:
    """Parse BLE device config from list or JSON/comma string."""
    if not value:
        return []
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return []
        try:
            value = json.loads(text)
        except json.JSONDecodeError:
            value = [{"mac": item.strip()} for item in text.split(",") if item.strip()]
    if not isinstance(value, list):
        return []

    devices: list[dict[str, str]] = []
    for item in value:
        if isinstance(item, str):
            item = {"mac": item}
        if not isinstance(item, dict):
            continue
        mac = maybe_normalize_mac(item.get("mac"))
        if not mac:
            _LOGGER.warning("Ignoring invalid CGDK2 MAC in BLE device config: %s", item)
            continue
        devices.append({"mac": mac, "name": str(item.get("name") or f"CGDK2 {mac[-4:]}")})
    return devices


def get_configured_ble_devices(hass: HomeAssistant, config_entry: ConfigEntry) -> list[dict[str, str]]:
    """Return BLE devices configured for a CGDN1 gateway."""
    config = _merged_config(config_entry)
    explicit = _parse_ble_devices(config.get(CONF_BLE_DEVICES))
    if explicit:
        return explicit

    devices: list[dict[str, str]] = []
    seen: set[str] = set()
    for entry in hass.config_entries.async_entries(DOMAIN):
        if entry.entry_id == config_entry.entry_id:
            continue
        entry_config = {**dict(entry.data), **dict(entry.options)}
        if entry_config.get(CONF_MODEL) != "CGDK2":
            continue
        mac = maybe_normalize_mac(entry_config.get(CONF_MAC))
        if mac and mac not in seen:
            seen.add(mac)
            devices.append({"mac": mac, "name": str(entry_config.get(CONF_NAME) or f"CGDK2 {mac[-4:]}")})

    discovered_cache = hass.data.get(DOMAIN, {}).get("discovered_devices_cache", {})
    if isinstance(discovered_cache, dict):
        for cached_mac, info in discovered_cache.items():
            mac = maybe_normalize_mac(cached_mac)
            if not mac or mac in seen:
                continue
            name = f"CGDK2 {mac[-4:]}"
            if isinstance(info, dict):
                cached_name = str(info.get("name") or "")
                if "BLE Device" in cached_name or "CGDK2" in cached_name:
                    name = cached_name
            seen.add(mac)
            devices.append({"mac": mac, "name": name})
    return devices


class QingpingPrivateGatewayHandler:
    """Handle CGDN1 private MQTT server-side messages and commands."""

    def __init__(self, hass: HomeAssistant, config_entry: ConfigEntry) -> None:
        self.hass = hass
        self.config_entry = config_entry
        self.gateway_mac = normalize_mac(config_entry.data[CONF_MAC])
        self.topic = down_topic(self.gateway_mac)
        self.last_seen_gateway: int | None = None
        self.last_sensor_data_at: int | None = None
        self.last_ble_adv_at: dict[str, int] = {}
        self.last_gateway_settings: dict[str, Any] = {}
        self._message_id = int(time.time())
        self._remove_refresh = None

    @property
    def ble_devices(self) -> list[dict[str, str]]:
        """Return configured BLE devices."""
        return get_configured_ble_devices(self.hass, self.config_entry)

    async def async_start(self) -> None:
        """Start periodic refresh and initialize gateway."""
        refresh_every = int(_merged_config(self.config_entry).get(
            CONF_TEMPORARY_REPORT_REFRESH, DEFAULT_TEMPORARY_REPORT_REFRESH
        ))
        self._remove_refresh = async_track_time_interval(
            self.hass,
            self._async_periodic_refresh,
            timedelta(seconds=refresh_every),
        )
        await self.async_initialize_gateway(reason="startup")

    def async_stop(self) -> None:
        """Stop periodic refresh."""
        if self._remove_refresh:
            self._remove_refresh()
            self._remove_refresh = None

    async def _async_periodic_refresh(self, *_: Any) -> None:
        await self.async_enable_temporary_report()

    async def async_publish(self, payload: dict[str, Any]) -> None:
        """Publish JSON payload to the gateway down topic."""
        for _ in range(MQTT_READY_RETRIES):
            if mqtt.is_connected(self.hass):
                break
            await asyncio.sleep(MQTT_READY_DELAY)
        if not mqtt.is_connected(self.hass):
            _LOGGER.error("MQTT is not connected, cannot publish CGDN1 type=%s", payload.get("type"))
            return
        _LOGGER.debug("Publishing CGDN1 private payload to %s: type=%s", self.topic, payload.get("type"))
        await mqtt.async_publish(self.hass, self.topic, json.dumps(payload))

    async def async_publish_ble_devices(self) -> None:
        """Send type 11 BLE device list."""
        payload = build_type_11_payload(self.ble_devices)
        _LOGGER.debug("Sending CGDN1 type 11 BLE list with %s devices", len(payload["dev_list"]))
        await self.async_publish(payload)

    async def async_publish_ble_devices_with_names(self) -> None:
        """Send type 26 BLE device list with names."""
        payload = build_type_26_payload(self.ble_devices)
        _LOGGER.debug(
            "Sending CGDN1 type 26 BLE list with %s devices",
            len(payload["homekit_dev_list"]),
        )
        await self.async_publish(payload)

    async def async_enable_temporary_report(
        self,
        interval: int | None = None,
        duration: int | None = None,
    ) -> None:
        """Enable temporary report mode."""
        config = _merged_config(self.config_entry)
        interval = int(interval or config.get(CONF_TEMPORARY_REPORT_INTERVAL, DEFAULT_TEMPORARY_REPORT_INTERVAL))
        duration = int(duration or config.get(CONF_TEMPORARY_REPORT_DURATION, DEFAULT_TEMPORARY_REPORT_DURATION))
        _LOGGER.debug("Sending CGDN1 type 12 temporary report interval=%s duration=%s", interval, duration)
        await self.async_publish(build_type_12_payload(interval, duration))

    async def async_enable_diagnostic_report(self) -> None:
        """Enable short diagnostic report mode."""
        _LOGGER.debug("Sending CGDN1 diagnostic type 12 temporary report")
        await self.async_publish(build_diagnostic_report_payload())

    async def async_configure_gateway_settings(self) -> None:
        """Send persistent gateway settings command."""
        config = _merged_config(self.config_entry)
        self._message_id += 1
        temp_unit = config.get(CONF_TEMPERATURE_UNIT)
        if not temp_unit:
            temp_unit = "F" if self.hass.config.units.temperature_unit == UnitOfTemperature.FAHRENHEIT else "C"
        payload = build_type_17_gateway_settings_payload(
            self._message_id,
            report_interval=int(config.get(CONF_REPORT_INTERVAL, DEFAULT_TEMPORARY_REPORT_INTERVAL)),
            collect_interval=int(config.get(CONF_COLLECT_INTERVAL, 10)),
            co2_sampling_interval=int(config.get(CONF_CO2_SAMPLING_INTERVAL, 10)),
            pm_sampling_interval=int(config.get(CONF_PM_SAMPLING_INTERVAL, 10)),
            temperature_unit=temp_unit,
            timezone=int(config.get(CONF_TIMEZONE, 20)),
        )
        _LOGGER.debug("Sending CGDN1 type 17 gateway settings id=%s", payload["id"])
        await self.async_publish(payload)

    async def async_initialize_gateway(self, reason: str) -> None:
        """Send the CGDN1 startup/reconnect initialization sequence."""
        _LOGGER.info("Initializing CGDN1 private MQTT gateway %s (%s)", self.gateway_mac, reason)
        await self.async_publish_ble_devices()
        await self.async_publish_ble_devices_with_names()
        await self.async_enable_temporary_report()
        config = _merged_config(self.config_entry)
        if reason == "startup" and config.get(CONF_CONFIGURE_GATEWAY_ON_START, False):
            await self.async_configure_gateway_settings()
        if reason == "reconnect" and config.get(CONF_CONFIGURE_GATEWAY_ON_RECONNECT, False):
            await self.async_configure_gateway_settings()

    async def async_handle_message(self, payload: dict[str, Any]) -> bool:
        """Handle a parsed CGDN1 JSON message.

        Returns True if the message was fully handled by the private handler.
        """
        message_type = str(payload.get("type", ""))
        now = int(time.time())
        _LOGGER.debug("Received CGDN1 private MQTT message type=%s", message_type or "<none>")

        if "result" in payload:
            _LOGGER.warning(
                "CGDN1 result %s: %s",
                payload.get("result"),
                result_description(payload.get("result")),
            )

        if message_type == "10":
            await self.async_publish_ble_devices()
            return True

        if message_type == "25":
            await self.async_publish_ble_devices_with_names()
            return True

        if message_type == "13":
            self.last_seen_gateway = now
            wifi_info = payload.get("wifi_info")
            sw_version = payload.get("sw_version") or payload.get("version")
            coordinator_data = self.hass.data[DOMAIN][self.config_entry.entry_id]
            coordinator_data["last_seen_gateway"] = now
            coordinator_data["gateway_wifi_info"] = wifi_info
            coordinator_data["gateway_sw_version"] = sw_version
            _LOGGER.debug("CGDN1 heartbeat: wifi_info=%s sw_version=%s", wifi_info, sw_version)
            if self._sensor_data_is_stale(now):
                await self.async_initialize_gateway(reason="stale_sensor_data")
            return True

        if message_type == "14":
            await self.async_initialize_gateway(reason="reconnect")
            return True

        if message_type == "28":
            settings = payload.get("setting")
            if isinstance(settings, dict):
                self.last_gateway_settings = settings
                self.hass.data[DOMAIN][self.config_entry.entry_id]["gateway_settings_state"] = settings
                _LOGGER.info("CGDN1 settings received: %s", settings)
            return False

        if message_type == "9":
            ble_mac = maybe_normalize_mac(payload.get("mac"))
            if ble_mac:
                self.last_ble_adv_at[ble_mac] = now
                self.hass.data[DOMAIN][self.config_entry.entry_id].setdefault("last_ble_adv_at", {})[ble_mac] = now
            _LOGGER.debug("CGDN1 BLE broadcast mac=%s rssi=%s", ble_mac, payload.get("rssi"))
            return False

        sensor_data = payload.get("sensorData")
        if isinstance(sensor_data, list) and sensor_data:
            self.last_sensor_data_at = now
            self.hass.data[DOMAIN][self.config_entry.entry_id]["last_sensor_data_at"] = now
            _LOGGER.debug("CGDN1 sensorData received at %s", now)
            if message_type == "17" and payload.get("id") is not None:
                await self.async_ack_sensor_data(payload)
            elif payload.get("need_ack") == 1 and payload.get("id") is not None and message_type != "12":
                await self.async_ack_sensor_data(payload)
            return False

        if message_type and message_type not in {"12", "17"}:
            _LOGGER.debug("Unhandled CGDN1 private MQTT message type=%s", message_type)
        return False

    async def async_ack_sensor_data(self, payload: dict[str, Any]) -> None:
        """ACK sensor/history data when required."""
        ack_payload = build_type_18_ack_payload(payload["id"])
        _LOGGER.debug("Sending CGDN1 type 18 ACK for id=%s", payload["id"])
        await self.async_publish(ack_payload)

    def _sensor_data_is_stale(self, now: int) -> bool:
        if self.last_sensor_data_at is None:
            return True
        return now - self.last_sensor_data_at > SENSOR_DATA_STALE_SECONDS
