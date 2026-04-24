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
    MQTT_TOPIC_PREFIX,
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

    gateway_mac = maybe_normalize_mac(config_entry.data.get(CONF_MAC))
    discovered_cache = hass.data.get(DOMAIN, {}).get("discovered_devices_cache", {})
    if isinstance(discovered_cache, dict):
        for cached_mac, info in discovered_cache.items():
            mac = maybe_normalize_mac(cached_mac)
            if not mac or mac == gateway_mac or mac in seen:
                continue
            name = ""
            if isinstance(info, dict):
                cached_name = str(info.get("name") or "")
                if "BLE Device" in cached_name or "CGDK2" in cached_name:
                    name = cached_name
            if not name:
                continue
            seen.add(mac)
            devices.append({"mac": mac, "name": name})
    if not devices:
        model_counts: dict[str, int] = {}
        for entry in hass.config_entries.async_entries(DOMAIN):
            entry_config = {**dict(entry.data), **dict(entry.options)}
            model = str(entry_config.get(CONF_MODEL) or "<unset>")
            model_counts[model] = model_counts.get(model, 0) + 1
        _LOGGER.warning(
            "CGDN1 BLE device list is empty for gateway %s. Configure %s explicitly or ensure BLE entries are model CGDK2. Models seen: %s",
            config_entry.data.get(CONF_MAC),
            CONF_BLE_DEVICES,
            model_counts,
        )
    return devices


class QingpingPrivateGatewayHandler:
    """Handle CGDN1 private MQTT server-side messages and commands."""

    def __init__(self, hass: HomeAssistant, config_entry: ConfigEntry) -> None:
        self.hass = hass
        self.config_entry = config_entry
        self.gateway_mac = normalize_mac(config_entry.data[CONF_MAC])
        self.topics = [down_topic(self.gateway_mac)]
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

    async def async_start(self, initialize: bool = True) -> None:
        """Start periodic refresh and initialize gateway."""
        refresh_every = int(_merged_config(self.config_entry).get(
            CONF_TEMPORARY_REPORT_REFRESH, DEFAULT_TEMPORARY_REPORT_REFRESH
        ))
        self._remove_refresh = async_track_time_interval(
            self.hass,
            self._async_periodic_refresh,
            timedelta(seconds=refresh_every),
        )
        if initialize:
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
        await self.async_publish_to_topics(payload, self.topics)

    async def async_publish_to_topics(self, payload: dict[str, Any], topics: list[str]) -> None:
        """Publish JSON payload to one or more gateway down topics."""
        for _ in range(MQTT_READY_RETRIES):
            if mqtt.is_connected(self.hass):
                break
            await asyncio.sleep(MQTT_READY_DELAY)
        if not mqtt.is_connected(self.hass):
            _LOGGER.error("MQTT is not connected, cannot publish CGDN1 type=%s", payload.get("type"))
            return
        payload_json = json.dumps(payload)
        for topic in dict.fromkeys(topics):
            log = _LOGGER.info if payload.get("type") in {"11", "12", "18", "26"} else _LOGGER.debug
            log("Publishing CGDN1 private payload to %s: type=%s", topic, payload.get("type"))
            await mqtt.async_publish(self.hass, topic, payload_json)

    async def async_publish_ble_devices(
        self,
        topics: list[str] | None = None,
        request_id: Any = None,
        ble_devices: list[dict[str, str]] | None = None,
    ) -> None:
        """Send type 11 BLE device list."""
        devices = ble_devices if ble_devices is not None else self.ble_devices
        payload = build_type_11_payload(devices, request_id=request_id)
        _LOGGER.debug("Sending CGDN1 type 11 BLE list with %s devices", len(payload["dev_list"]))
        await self.async_publish_to_topics(payload, topics or self.topics)

    async def async_publish_ble_devices_with_names(
        self,
        topics: list[str] | None = None,
        request_id: Any = None,
        ble_devices: list[dict[str, str]] | None = None,
    ) -> None:
        """Send type 26 BLE device list with names."""
        devices = ble_devices if ble_devices is not None else self.ble_devices
        payload = build_type_26_payload(devices, request_id=request_id)
        _LOGGER.debug(
            "Sending CGDN1 type 26 BLE list with %s devices",
            len(payload["homekit_dev_list"]),
        )
        await self.async_publish_to_topics(payload, topics or self.topics)

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

    async def async_initialize_gateway(self, reason: str, response_topics: list[str] | None = None) -> None:
        """Send the CGDN1 startup/reconnect initialization sequence."""
        topics = response_topics or self.topics
        _LOGGER.info("Initializing CGDN1 private MQTT gateway %s (%s)", self.gateway_mac, reason)
        await self.async_publish_ble_devices(topics)
        await self.async_publish_ble_devices_with_names(topics)
        await self.async_enable_temporary_report()
        config = _merged_config(self.config_entry)
        if reason == "startup" and config.get(CONF_CONFIGURE_GATEWAY_ON_START, False):
            await self.async_configure_gateway_settings()
        if reason == "reconnect" and config.get(CONF_CONFIGURE_GATEWAY_ON_RECONNECT, False):
            await self.async_configure_gateway_settings()

    async def async_handle_message(self, payload: dict[str, Any], topic: str | None = None) -> bool:
        """Handle a parsed CGDN1 JSON message.

        Returns True if the message was fully handled by the private handler.
        """
        message_type = str(payload.get("type", ""))
        response_topics = self._response_topics(topic)
        now = int(time.time())
        _LOGGER.debug("Received CGDN1 private MQTT message type=%s", message_type or "<none>")

        if "result" in payload:
            _LOGGER.warning(
                "CGDN1 result %s: %s",
                payload.get("result"),
                result_description(payload.get("result")),
            )

        if message_type == "10":
            ble_devices = self.ble_devices
            _LOGGER.info(
                "CGDN1 requested BLE device list on %s; replying with %s devices to %s",
                topic,
                len(ble_devices),
                response_topics,
            )
            await self.async_publish_ble_devices(
                response_topics,
                request_id=payload.get("id"),
                ble_devices=ble_devices,
            )
            return True

        if message_type == "25":
            ble_devices = self.ble_devices
            _LOGGER.info(
                "CGDN1 requested BLE named device list on %s; replying with %s devices to %s",
                topic,
                len(ble_devices),
                response_topics,
            )
            await self.async_publish_ble_devices_with_names(
                response_topics,
                request_id=payload.get("id"),
                ble_devices=ble_devices,
            )
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
                await self.async_initialize_gateway(reason="stale_sensor_data", response_topics=response_topics)
            return True

        if message_type == "14":
            await self.async_initialize_gateway(reason="reconnect", response_topics=response_topics)
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
                await self.async_ack_sensor_data(payload, response_topics)
            elif payload.get("need_ack") == 1 and payload.get("id") is not None and message_type != "12":
                await self.async_ack_sensor_data(payload, response_topics)
            return False

        if message_type and message_type not in {"12", "17"}:
            _LOGGER.debug("Unhandled CGDN1 private MQTT message type=%s", message_type)
        return False

    async def async_ack_sensor_data(self, payload: dict[str, Any], topics: list[str] | None = None) -> None:
        """ACK sensor/history data when required."""
        ack_payload = build_type_18_ack_payload(payload["id"])
        _LOGGER.debug("Sending CGDN1 type 18 ACK for id=%s", payload["id"])
        await self.async_publish_to_topics(ack_payload, topics or self.topics)

    def _response_topics(self, topic: str | None) -> list[str]:
        """Prefer the matching down topic for the observed incoming up topic."""
        if not topic:
            return self.topics
        parts = topic.split("/")
        if len(parts) == 3 and parts[0] == MQTT_TOPIC_PREFIX and parts[2] == "up":
            return [f"{MQTT_TOPIC_PREFIX}/{parts[1]}/down"]
        return self.topics

    def _sensor_data_is_stale(self, now: int) -> bool:
        if self.last_sensor_data_at is None:
            return True
        return now - self.last_sensor_data_at > SENSOR_DATA_STALE_SECONDS
