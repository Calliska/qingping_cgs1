"""MQTT commands, reporting state and BLE membership for one CGDN1."""

from __future__ import annotations

import asyncio
import json
import logging
import time
from datetime import timedelta
from typing import Any

from homeassistant.components import mqtt
from homeassistant.config_entries import ConfigEntry
from homeassistant.const import UnitOfTemperature
from homeassistant.core import HomeAssistant, callback
from homeassistant.exceptions import HomeAssistantError
from homeassistant.helpers.event import async_track_time_interval

from .const import (
    CONF_CO2_SAMPLING_INTERVAL,
    CONF_COLLECT_INTERVAL,
    CONF_CONFIGURE_GATEWAY_ON_RECONNECT,
    CONF_CONFIGURE_GATEWAY_ON_START,
    CONF_GATEWAY_DOWN_TOPIC,
    CONF_GATEWAY_UP_TOPIC,
    CONF_MAC,
    CONF_OBSERVED_UP_TOPIC,
    CONF_PM_SAMPLING_INTERVAL,
    CONF_REPORT_INTERVAL,
    CONF_TEMPERATURE_UNIT,
    CONF_TEMPORARY_REPORT,
    CONF_TEMPORARY_REPORT_DURATION,
    CONF_TEMPORARY_REPORT_INTERVAL,
    CONF_TEMPORARY_REPORT_REFRESH,
    CONF_TIMEZONE,
    DEFAULT_TEMPORARY_REPORT_DURATION,
    DEFAULT_TEMPORARY_REPORT_INTERVAL,
    DEFAULT_TEMPORARY_REPORT_REFRESH,
    DIAGNOSTIC_REPORT_DURATION,
    DIAGNOSTIC_REPORT_INTERVAL,
    DOMAIN,
    SENSOR_DATA_STALE_SECONDS,
)
from .qingping_private import (
    build_type_11_payload,
    build_type_12_payload,
    build_type_17_gateway_settings_payload,
    build_type_18_ack_payload,
    build_type_26_payload,
    down_topic,
    normalize_mac,
    result_description,
)
from .topology import configured_ble_devices, entry_settings, gateway_entries

_LOGGER = logging.getLogger(__name__)
_TICK = 15


class QingpingPrivateGatewayHandler:
    """Manage one gateway and its MQTT protocol state."""

    def __init__(self, hass: HomeAssistant, entry: ConfigEntry) -> None:
        self.hass = hass
        self.config_entry = entry
        self.gateway_mac = normalize_mac(entry.data[CONF_MAC])
        self.last_seen_gateway: int | None = None
        self._last_contact: float | None = None
        self.last_sensor_data_at: int | None = None
        self.last_gateway_settings: dict[str, Any] = {}
        self.last_result: str | None = None
        self.requested_settings: dict[str, Any] = {}
        self.requested_interval: int | None = None
        self.observed_interval: int | None = None
        self.report_status = "waiting"
        self.observed_up_topic = entry.data.get(CONF_OBSERVED_UP_TOPIC)
        self._last_live_report: float | None = None
        self._matching_intervals = 0
        self._verify_after = 0.0
        self._diagnostic_until = 0.0
        self._renew_at = 0.0
        self._next_recovery = 0.0
        self._next_sync = 0.0
        self._reconcile_after_contact = True
        self._message_id = int(time.time() * 1000)
        self._published_membership: tuple[tuple[str, str], ...] | None = None
        self._remove_tick = None
        self._stopped = False
        self._lock = asyncio.Lock()

    @property
    def settings(self) -> dict[str, Any]:
        return entry_settings(self.config_entry)

    @property
    def topic(self) -> str:
        """Use explicit configuration or the exact topic seen on the broker."""
        if configured := self.settings.get(CONF_GATEWAY_DOWN_TOPIC):
            return configured
        up_topic = self.settings.get(CONF_GATEWAY_UP_TOPIC) or self.observed_up_topic
        if up_topic and up_topic.endswith("/up"):
            return f"{up_topic[:-3]}/down"
        return down_topic(self.gateway_mac)

    @property
    def ble_devices(self) -> list[dict[str, str]]:
        return configured_ble_devices(
            self.gateway_mac,
            self.hass.config_entries.async_entries(DOMAIN),
            gateway_entries(self.hass),
        )

    @property
    def interval(self) -> int:
        return max(1, int(self.settings.get(CONF_TEMPORARY_REPORT_INTERVAL, DEFAULT_TEMPORARY_REPORT_INTERVAL)))

    @property
    def timeout(self) -> int:
        """Allow missed reports and the normal heartbeat even in historic mode."""
        return max(
            SENSOR_DATA_STALE_SECONDS,
            3 * (self.requested_interval or self.interval) + _TICK,
        )

    @property
    def online(self) -> bool:
        return self._last_contact is not None and time.monotonic() - self._last_contact <= self.timeout

    @property
    def settings_confirmed(self) -> bool:
        return bool(self.requested_settings) and all(
            self.last_gateway_settings.get(key) == value for key, value in self.requested_settings.items()
        )

    async def async_start(self, initialize: bool = True) -> None:
        """Start timers once after MQTT subscriptions have been registered."""
        if self._stopped or self._remove_tick is not None:
            return
        self._remove_tick = async_track_time_interval(self.hass, self._async_tick, timedelta(seconds=_TICK))
        self.config_entry.async_on_unload(
            mqtt.async_subscribe_connection_status(self.hass, self._async_mqtt_connection_changed)
        )
        if initialize:
            await self.async_initialize_gateway("startup")

    def async_stop(self) -> None:
        self._stopped = True
        if self._remove_tick is not None:
            self._remove_tick()
            self._remove_tick = None

    @callback
    def _async_mqtt_connection_changed(self, connected: bool) -> None:
        if self._stopped:
            return
        if not connected:
            self._published_membership = None
            self._last_contact = None
            self._reconcile_after_contact = False
            self.report_status = "waiting"
            return
        self.config_entry.async_create_background_task(
            self.hass, self.async_initialize_gateway("broker_reconnect"), "Reconnect Qingping gateway MQTT"
        )

    async def async_publish(self, payload: dict[str, Any], topic: str | None = None) -> bool:
        if self._stopped or not mqtt.is_connected(self.hass):
            return False
        destination = topic or self.topic
        try:
            await mqtt.async_publish(self.hass, destination, json.dumps(payload), qos=0, retain=False)
        except HomeAssistantError as err:
            _LOGGER.warning("CGDN1 %s: failed to send type %s: %s", self.gateway_mac, payload.get("type"), err)
            return False
        _LOGGER.debug("CGDN1 %s sent type %s to %s", self.gateway_mac, payload.get("type"), destination)
        return True

    async def async_sync_devices(self, force: bool = False) -> bool:
        """Reconcile the desired list only after a confirmed topology change."""
        async with self._lock:
            return await self._async_sync_devices(force)

    async def _async_sync_devices(self, force: bool = False) -> bool:
        devices = self.ble_devices
        membership = tuple((device["mac"], device["name"]) for device in devices)
        self._next_sync = time.monotonic() + 60
        if not force and membership == self._published_membership:
            return True
        destination = self.topic
        if not await self.async_publish(build_type_11_payload(devices), destination):
            return False
        if not await self.async_publish(build_type_26_payload(devices), destination):
            return False
        self._published_membership = membership
        return True

    async def async_enable_temporary_report(
        self, interval: int | None = None, duration: int | None = None
    ) -> bool:
        async with self._lock:
            return await self._async_enable_temporary_report(interval, duration)

    async def _async_enable_temporary_report(
        self, interval: int | None = None, duration: int | None = None
    ) -> bool:
        settings = self.settings
        interval = self.interval if interval is None else interval
        duration = (
            max(1, int(settings.get(CONF_TEMPORARY_REPORT_DURATION, DEFAULT_TEMPORARY_REPORT_DURATION)))
            if duration is None else duration
        )
        if interval >= duration:
            self.report_status = "invalid_configuration"
            return False
        if not await self.async_publish(build_type_12_payload(interval, duration)):
            self.report_status = "send_failed"
            return False
        now = time.monotonic()
        self.requested_interval = interval
        self.observed_interval = None
        self._last_live_report = None
        self._matching_intervals = 0
        self.report_status = "pending"
        self._verify_after = now + min(duration, max(60, 4 * interval))
        refresh = max(1, int(settings.get(CONF_TEMPORARY_REPORT_REFRESH, DEFAULT_TEMPORARY_REPORT_REFRESH)))
        self._renew_at = now + min(refresh, max(1, duration - _TICK - 1), max(1, int(duration * 0.8)))
        return True

    async def async_enable_diagnostic_report(self) -> bool:
        async with self._lock:
            sent = await self._async_enable_temporary_report(DIAGNOSTIC_REPORT_INTERVAL, DIAGNOSTIC_REPORT_DURATION)
            if sent:
                self._diagnostic_until = time.monotonic() + DIAGNOSTIC_REPORT_DURATION
            return sent

    async def async_configure_gateway_settings(self) -> bool:
        async with self._lock:
            return await self._async_configure_gateway_settings()

    async def _async_configure_gateway_settings(self) -> bool:
        settings = self.settings
        temp_unit = settings.get(CONF_TEMPERATURE_UNIT) or (
            "F" if self.hass.config.units.temperature_unit == UnitOfTemperature.FAHRENHEIT else "C"
        )
        self._message_id += 1
        payload = build_type_17_gateway_settings_payload(
            self._message_id,
            report_interval=int(settings.get(CONF_REPORT_INTERVAL, DEFAULT_TEMPORARY_REPORT_INTERVAL)),
            collect_interval=int(settings.get(CONF_COLLECT_INTERVAL, 10)),
            co2_sampling_interval=int(settings.get(CONF_CO2_SAMPLING_INTERVAL, 10)),
            pm_sampling_interval=int(settings.get(CONF_PM_SAMPLING_INTERVAL, 10)),
            temperature_unit=temp_unit,
            timezone=int(settings["timezone_wire"]) if "timezone_wire" in settings else (
                int(self.config_entry.options[CONF_TIMEZONE]) if CONF_TIMEZONE in self.config_entry.options else None
            ),
        )
        if not await self.async_publish(payload):
            return False
        self.last_gateway_settings = {}
        self.requested_settings = payload["setting"]
        return await self.async_publish({"type": "28"})

    async def async_read_settings(self) -> bool:
        return await self.async_publish({"type": "28"})

    async def async_set_setting(self, key: str, value: Any) -> bool:
        """Use the same topic and command envelope for existing HA controls."""
        async with self._lock:
            self._message_id += 1
            payload = {
                "type": "17", "id": self._message_id, "need_ack": 1,
                "desc": "change settings", "timestamp": int(time.time()), "setting": {key: value},
            }
            if not await self.async_publish(payload):
                return False
            self.last_gateway_settings = {}
            self.requested_settings = payload["setting"]
            return await self.async_publish({"type": "28"})

    async def async_initialize_gateway(self, reason: str) -> None:
        async with self._lock:
            await self._async_initialize_gateway(reason)

    async def _async_initialize_gateway(self, reason: str) -> None:
        self._next_recovery = time.monotonic() + 60
        await self._async_sync_devices(force=True)
        if self.settings.get(CONF_TEMPORARY_REPORT, True) and not self._diagnostic_until:
            await self._async_enable_temporary_report()
        if (
            reason == "startup" and self.settings.get(CONF_CONFIGURE_GATEWAY_ON_START)
            or reason == "reconnect" and self.settings.get(CONF_CONFIGURE_GATEWAY_ON_RECONNECT)
        ):
            await self._async_configure_gateway_settings()
        elif reason in {"startup", "reconnect", "broker_reconnect"}:
            await self.async_read_settings()

    async def _async_tick(self, *_: Any) -> None:
        if self._stopped or not mqtt.is_connected(self.hass):
            self._published_membership = None
            self._last_contact = None
            self._reconcile_after_contact = True
            return
        now = time.monotonic()
        async with self._lock:
            if now >= self._next_sync:
                await self._async_sync_devices()
            if self.report_status == "pending" and now >= self._verify_after:
                self.report_status = "unverified"
            if self._diagnostic_until:
                if now < self._diagnostic_until:
                    return
                self._diagnostic_until = 0
                self._renew_at = 0
            if self.settings.get(CONF_TEMPORARY_REPORT, True) and now >= max(self._renew_at, self._next_recovery):
                self._next_recovery = now + 60
                await self._async_enable_temporary_report()

    def _observe_report_interval(self) -> None:
        now = time.monotonic()
        if self._last_live_report is not None and self.requested_interval:
            elapsed = now - self._last_live_report
            # Reports redelivered very close together do not describe the cadence.
            if elapsed < min(1, self.requested_interval / 2):
                return
            self.observed_interval = round(elapsed)
            if abs(elapsed - self.requested_interval) <= max(2, self.requested_interval * 0.1):
                self._matching_intervals += 1
                if self._matching_intervals >= 2:
                    self.report_status = "observed"
            else:
                self._matching_intervals = 0
                if self.report_status == "observed":
                    self.report_status = "unverified"
        self._last_live_report = now

    async def async_handle_message(
        self, payload: dict[str, Any], topic: str, *, own_report: bool = False
    ) -> None:
        """Respond to gateway requests and acknowledge documented history."""
        kind = str(payload.get("type", ""))
        if kind not in {"9", "10", "12", "13", "14", "17", "25", "28"} and "result" not in payload:
            return
        route_changed = self.observed_up_topic != topic
        self.observed_up_topic = topic
        if route_changed:
            self.hass.config_entries.async_update_entry(
                self.config_entry, data={**self.config_entry.data, CONF_OBSERVED_UP_TOPIC: topic}
            )
            self._published_membership = None
            self._reconcile_after_contact = True
        self.last_seen_gateway = int(time.time())
        self._last_contact = time.monotonic()
        reply_topic = self.topic
        if "result" in payload:
            code = str(payload["result"])
            self.last_result = result_description(code) if code.lstrip("-").isdigit() else "Invalid result code"
        if kind in {"10", "25"}:
            builder = build_type_11_payload if kind == "10" else build_type_26_payload
            await self.async_publish(builder(self.ble_devices, request_id=payload.get("id")), reply_topic)
        elif kind == "14":
            self._reconcile_after_contact = False
            await self.async_initialize_gateway("reconnect")
        elif kind == "28" and isinstance(payload.get("setting"), dict):
            self.last_gateway_settings = payload["setting"]
        elif kind in {"12", "17"}:
            if own_report:
                self.last_sensor_data_at = self.last_seen_gateway
                if kind == "12":
                    self._observe_report_interval()
            if kind == "17" and payload.get("id") is not None and isinstance(payload.get("sensorData"), list):
                await self.async_publish(build_type_18_ack_payload(payload["id"]), reply_topic)

        if kind == "13":
            now = time.monotonic()
            stale = self.last_sensor_data_at is None or time.time() - self.last_sensor_data_at > self.timeout
            if self._reconcile_after_contact:
                self._reconcile_after_contact = False
                await self.async_initialize_gateway("contact")
            elif stale and now >= self._next_recovery and self.settings.get(CONF_TEMPORARY_REPORT, True):
                self._next_recovery = now + min(600, self.timeout)
                if not self._diagnostic_until:
                    await self.async_enable_temporary_report()
