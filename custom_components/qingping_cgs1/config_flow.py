"""Native setup, discovery and configuration for Qingping devices."""

from __future__ import annotations

import asyncio
import time
from datetime import datetime
from typing import Any

import voluptuous as vol
from homeassistant import config_entries
from homeassistant.components import mqtt
from homeassistant.core import callback
from homeassistant.exceptions import HomeAssistantError
from homeassistant.util import dt as dt_util

from .const import (
    ADV_MODELS,
    BLE_CANDIDATE_TTL,
    CONF_AUTO_ADD_BLE,
    CONF_BLE_DEVICES,
    CONF_BLE_TIMEOUT,
    CONF_CO2_SAMPLING_INTERVAL,
    CONF_COLLECT_INTERVAL,
    CONF_CONFIGURE_GATEWAY_ON_RECONNECT,
    CONF_CONFIGURE_GATEWAY_ON_START,
    CONF_GATEWAY_DOWN_TOPIC,
    CONF_GATEWAY_MAC,
    CONF_GATEWAY_UP_TOPIC,
    CONF_MAC,
    CONF_MODEL,
    CONF_NAME,
    CONF_PM_SAMPLING_INTERVAL,
    CONF_REPORT_INTERVAL,
    CONF_TEMPERATURE_UNIT,
    CONF_TEMPORARY_REPORT,
    CONF_TEMPORARY_REPORT_DURATION,
    CONF_TEMPORARY_REPORT_INTERVAL,
    CONF_TEMPORARY_REPORT_REFRESH,
    DEFAULT_BLE_TIMEOUT,
    DEFAULT_TEMPORARY_REPORT_DURATION,
    DEFAULT_TEMPORARY_REPORT_INTERVAL,
    DEFAULT_TEMPORARY_REPORT_REFRESH,
    DOMAIN,
    MQTT_TOPIC_PREFIX,
    QP_MODELS,
)
from .qingping_private import down_topic_variants, maybe_normalize_mac, mqtt_topic_is_valid, parse_json_payload, up_topic_variants
from .reports import decode_ble_report
from .topology import BLE_CANDIDATES, entry_settings, gateway_entries, parse_ble_devices, remember_candidate, resolve_gateway

_DISCOVERED = "discovered_mac"
_MANUAL = ""
_NO_GATEWAY = ""
_GATEWAY_DEFAULTS = {
    CONF_GATEWAY_UP_TOPIC: "",
    CONF_GATEWAY_DOWN_TOPIC: "",
    CONF_AUTO_ADD_BLE: False,
    CONF_TEMPORARY_REPORT: True,
    CONF_TEMPORARY_REPORT_INTERVAL: DEFAULT_TEMPORARY_REPORT_INTERVAL,
    CONF_TEMPORARY_REPORT_DURATION: DEFAULT_TEMPORARY_REPORT_DURATION,
    CONF_TEMPORARY_REPORT_REFRESH: DEFAULT_TEMPORARY_REPORT_REFRESH,
    CONF_CONFIGURE_GATEWAY_ON_START: False,
    CONF_CONFIGURE_GATEWAY_ON_RECONNECT: False,
    CONF_REPORT_INTERVAL: 60,
    CONF_COLLECT_INTERVAL: 10,
    CONF_CO2_SAMPLING_INTERVAL: 10,
    CONF_PM_SAMPLING_INTERVAL: 10,
    CONF_TEMPERATURE_UNIT: "C",
}


def _validate_topics(values: dict[str, Any]) -> bool:
    up = values.get(CONF_GATEWAY_UP_TOPIC, "")
    down = values.get(CONF_GATEWAY_DOWN_TOPIC, "")
    return (
        (not up or mqtt_topic_is_valid(up))
        and (not down or mqtt_topic_is_valid(down))
        and (not up or up != down)
        and (not up or up.endswith("/up") or bool(down))
    )


def _settings_schema(settings: dict[str, Any]) -> dict[Any, Any]:
    validators = {
        CONF_GATEWAY_UP_TOPIC: str,
        CONF_GATEWAY_DOWN_TOPIC: str,
        CONF_AUTO_ADD_BLE: bool,
        CONF_TEMPORARY_REPORT: bool,
        CONF_TEMPORARY_REPORT_INTERVAL: vol.All(vol.Coerce(int), vol.Range(min=1, max=86400)),
        CONF_TEMPORARY_REPORT_DURATION: vol.All(vol.Coerce(int), vol.Range(min=30, max=604800)),
        CONF_TEMPORARY_REPORT_REFRESH: vol.All(vol.Coerce(int), vol.Range(min=1, max=604800)),
        CONF_CONFIGURE_GATEWAY_ON_START: bool,
        CONF_CONFIGURE_GATEWAY_ON_RECONNECT: bool,
        CONF_REPORT_INTERVAL: vol.All(vol.Coerce(int), vol.Range(min=1, max=86400)),
        CONF_COLLECT_INTERVAL: vol.All(vol.Coerce(int), vol.Range(min=1, max=86400)),
        CONF_CO2_SAMPLING_INTERVAL: vol.All(vol.Coerce(int), vol.Range(min=1, max=86400)),
        CONF_PM_SAMPLING_INTERVAL: vol.All(vol.Coerce(int), vol.Range(min=1, max=86400)),
        CONF_TEMPERATURE_UNIT: vol.In(["C", "F"]),
    }
    return {
        vol.Optional(key, default=settings.get(key, default)): validators[key]
        for key, default in _GATEWAY_DEFAULTS.items()
    }


def _gateway_choices(hass) -> dict[str, str]:
    return {mac: f"{entry.title} ({mac})" for mac, entry in gateway_entries(hass).items()}


def _topics_in_use(hass, mac, settings):
    def topics(address, values):
        up = values.get(CONF_GATEWAY_UP_TOPIC)
        down = values.get(CONF_GATEWAY_DOWN_TOPIC)
        if not down and up and up.endswith("/up"):
            down = f"{up[:-3]}/down"
        return set([up] if up else up_topic_variants(address)) | set([down] if down else down_topic_variants(address))

    requested = topics(mac, settings)
    return any(
        other != mac and bool(requested & topics(other, entry_settings(entry)))
        for other, entry in gateway_entries(hass).items()
    )


class ConfigFlow(config_entries.ConfigFlow, domain=DOMAIN):
    """Add gateways and sensors manually or from verified MQTT traffic."""

    VERSION = 2

    def __init__(self) -> None:
        self._gateway_discovery: dict[str, str] | None = None
        self._other_discovery: dict[str, str] = {}
        self._discovery: dict[str, Any] = {}

    async def async_step_user(self, user_input=None):
        return self.async_show_menu(step_id="user", menu_options=["gateway", "ble", "other"])

    async def _async_scan_gateways(self) -> dict[str, str]:
        discovered = {}

        @callback
        def message_received(message):
            if message.retain:
                return
            mac = maybe_normalize_mac(message.topic.split("/")[1])
            if not mac:
                return
            if message.payload.startswith(b"CG"):
                self._other_discovery[mac] = f"Qingping TLV · {mac}"
                return
            payload = parse_json_payload(message.payload)
            if payload is None:
                return
            kind = str(payload.get("type", ""))
            if kind in {"10", "25"}:
                discovered[mac] = message.topic
                self._other_discovery.pop(mac, None)
            elif kind in {"12", "13", "15", "17", "21", "27"} and mac not in discovered:
                self._other_discovery[mac] = f"Qingping JSON · {mac}"
            if kind == "9" and (reading := decode_ble_report(payload)):
                discovered[mac] = message.topic
                remember_candidate(self.hass, mac, reading[0], reading[1].get("signal_strength"))

        try:
            unsubscribe = await mqtt.async_subscribe(
                self.hass, f"{MQTT_TOPIC_PREFIX}/+/up", message_received, 1, encoding=None
            )
        except HomeAssistantError:
            return discovered
        try:
            await asyncio.sleep(5)
        finally:
            unsubscribe()
        return discovered

    async def _async_create_device(self, data):
        mac = data[CONF_MAC]
        # Preserve entity IDs from older entries that stored a differently formatted address.
        if any(
            maybe_normalize_mac(entry.data.get(CONF_MAC) or entry.unique_id) == mac
            for entry in self._async_current_entries()
        ):
            return self.async_abort(reason="already_configured")
        await self.async_set_unique_id(mac)
        self._abort_if_unique_id_configured()
        return self.async_create_entry(title=data[CONF_NAME], data=data)

    async def async_step_gateway(self, user_input=None):
        if self._gateway_discovery is None:
            self._gateway_discovery = await self._async_scan_gateways()
        errors = {}
        if user_input is not None:
            mac = maybe_normalize_mac(user_input.get(CONF_MAC) or user_input.get(_DISCOVERED))
            if not mac:
                errors[CONF_MAC] = "invalid_mac"
            else:
                data = {
                    CONF_MAC: mac,
                    CONF_NAME: user_input.get(CONF_NAME) or f"Qingping CGDN1 {mac[-4:]}",
                    CONF_MODEL: "CGDN1",
                    CONF_GATEWAY_UP_TOPIC: user_input.get(CONF_GATEWAY_UP_TOPIC) or self._gateway_discovery.get(mac, ""),
                    CONF_GATEWAY_DOWN_TOPIC: user_input.get(CONF_GATEWAY_DOWN_TOPIC, ""),
                }
                if not _validate_topics(data):
                    errors["base"] = "invalid_topics"
                elif _topics_in_use(self.hass, mac, data):
                    errors["base"] = "topics_in_use"
                else:
                    return await self._async_create_device(data)
        choices = {_MANUAL: "—", **{mac: f"{mac} · {topic}" for mac, topic in self._gateway_discovery.items()}}
        return self.async_show_form(step_id="gateway", data_schema=vol.Schema({
            vol.Optional(_DISCOVERED, default=_MANUAL): vol.In(choices),
            vol.Optional(CONF_MAC): str,
            vol.Optional(CONF_NAME): str,
            vol.Optional(CONF_GATEWAY_UP_TOPIC): str,
            vol.Optional(CONF_GATEWAY_DOWN_TOPIC): str,
        }), errors=errors)

    async def async_step_ble(self, user_input=None):
        gateways = _gateway_choices(self.hass)
        if not gateways:
            return self.async_abort(reason="no_gateway")
        errors = {}
        if user_input is not None:
            selected = user_input.get(_DISCOVERED, _MANUAL)
            observed_gateway, _, observed_mac = selected.partition("/")
            mac = maybe_normalize_mac(user_input.get(CONF_MAC) or observed_mac)
            gateway = user_input.get(CONF_GATEWAY_MAC) or observed_gateway
            if not mac:
                errors[CONF_MAC] = "invalid_mac"
            if gateway not in gateways:
                errors[CONF_GATEWAY_MAC] = "no_gateway"
            if not errors:
                return await self._async_create_device({
                    CONF_MAC: mac,
                    CONF_NAME: user_input.get(CONF_NAME) or f"CGDK2 {mac[-4:]}",
                    CONF_MODEL: "CGDK2",
                    CONF_GATEWAY_MAC: gateway,
                    CONF_BLE_TIMEOUT: user_input[CONF_BLE_TIMEOUT],
                })
        configured = {
            maybe_normalize_mac(entry.data.get(CONF_MAC) or entry.unique_id) for entry in self._async_current_entries()
        }
        choices = {_MANUAL: "—"}
        candidates = self.hass.data.get(DOMAIN, {}).get(BLE_CANDIDATES, {})
        for (gateway, mac), observation in sorted(candidates.items(), key=lambda item: -item[1]["last_seen"]):
            if mac in configured or gateway not in gateways or time.time() - observation["last_seen"] > BLE_CANDIDATE_TTL:
                continue
            local = dt_util.as_local(datetime.fromtimestamp(observation["last_seen"], dt_util.UTC))
            rssi = observation["rssi"]
            signal = f" · {rssi} dBm" if rssi is not None else ""
            choices[f"{gateway}/{mac}"] = f"{mac} · {gateways[gateway]}{signal} · {local:%m-%d %H:%M}"
        for gateway, entry in gateway_entries(self.hass).items():
            for mac, name in parse_ble_devices(entry_settings(entry).get(CONF_BLE_DEVICES)).items():
                if mac not in configured:
                    choices.setdefault(f"{gateway}/{mac}", f"{name} · {mac} · {gateways[gateway]}")
        selected_gateway = next(iter(gateways)) if len(gateways) == 1 else _NO_GATEWAY
        return self.async_show_form(step_id="ble", data_schema=vol.Schema({
            vol.Optional(_DISCOVERED, default=_MANUAL): vol.In(choices),
            vol.Optional(CONF_MAC): str,
            vol.Optional(CONF_GATEWAY_MAC, default=selected_gateway): vol.In({_NO_GATEWAY: "—", **gateways}),
            vol.Optional(CONF_NAME): str,
            vol.Required(CONF_BLE_TIMEOUT, default=DEFAULT_BLE_TIMEOUT): vol.All(vol.Coerce(int), vol.Range(min=60, max=86400)),
        }), errors=errors)

    async def async_step_other(self, user_input=None):
        if self._gateway_discovery is None:
            self._gateway_discovery = await self._async_scan_gateways()
        errors = {}
        if user_input is not None:
            if mac := maybe_normalize_mac(user_input.get(CONF_MAC) or user_input.get(_DISCOVERED)):
                return await self._async_create_device({
                    CONF_MAC: mac, CONF_NAME: user_input[CONF_NAME], CONF_MODEL: user_input[CONF_MODEL]
                })
            errors[CONF_MAC] = "invalid_mac"
        return self.async_show_form(step_id="other", data_schema=vol.Schema({
            vol.Optional(_DISCOVERED, default=_MANUAL): vol.In({_MANUAL: "—", **self._other_discovery}),
            vol.Optional(CONF_MAC): str,
            vol.Required(CONF_NAME): str,
            vol.Required(CONF_MODEL, default="CGS1"): vol.In(
                [model for model in QP_MODELS if model != "CGDN1" and model not in ADV_MODELS]
            ),
        }), errors=errors)

    async def async_step_integration_discovery(self, discovery_info):
        mac = maybe_normalize_mac(discovery_info.get(CONF_MAC))
        gateway_mac = maybe_normalize_mac(discovery_info.get(CONF_GATEWAY_MAC))
        gateways = gateway_entries(self.hass)
        if not mac or gateway_mac not in gateways:
            return self.async_abort(reason="no_gateway")
        if any(
            maybe_normalize_mac(entry.data.get(CONF_MAC) or entry.unique_id) == mac
            for entry in self._async_current_entries()
        ):
            return self.async_abort(reason="already_configured")
        await self.async_set_unique_id(mac)
        self._abort_if_unique_id_configured()
        self._discovery = {
            CONF_MAC: mac, CONF_GATEWAY_MAC: gateway_mac,
            CONF_MODEL: "CGDK2", CONF_NAME: f"CGDK2 {mac[-4:]}",
        }
        self.context["title_placeholders"] = {"name": self._discovery[CONF_NAME]}
        if entry_settings(gateways[gateway_mac]).get(CONF_AUTO_ADD_BLE, False):
            return self.async_create_entry(title=self._discovery[CONF_NAME], data=self._discovery)
        return await self.async_step_confirm_ble()

    async def async_step_confirm_ble(self, user_input=None):
        gateways = _gateway_choices(self.hass)
        selected = self._discovery[CONF_GATEWAY_MAC]
        if selected not in gateways:
            return self.async_abort(reason="no_gateway")
        if user_input is not None:
            self._abort_if_unique_id_configured()
            return self.async_create_entry(title=user_input[CONF_NAME], data={**self._discovery, **user_input})
        return self.async_show_form(
            step_id="confirm_ble",
            description_placeholders={"mac": self._discovery[CONF_MAC]},
            data_schema=vol.Schema({
                vol.Required(CONF_NAME, default=self._discovery[CONF_NAME]): str,
                vol.Required(CONF_GATEWAY_MAC, default=selected): vol.In(gateways),
            }),
        )

    @staticmethod
    @callback
    def async_get_options_flow(config_entry):
        return OptionsFlowHandler(config_entry)


class OptionsFlowHandler(config_entries.OptionsFlow):
    """Persist gateway settings or change a sensor's owning gateway."""

    def __init__(self, config_entry):
        self._entry = config_entry

    async def async_step_init(self, user_input=None):
        model = entry_settings(self._entry)[CONF_MODEL]
        if model in ADV_MODELS:
            return await self.async_step_sensor_options(user_input)
        if model == "CGDN1":
            return await self.async_step_gateway_options(user_input)
        return await self.async_step_device_options(user_input)

    async def _async_save(self, changes):
        data = {**entry_settings(self._entry), **changes}
        if changes.get(CONF_MODEL) == "CGDN1" and "timezone" in self._entry.options:
            data["timezone_wire"] = self._entry.options["timezone"]
        self.hass.config_entries.async_update_entry(self._entry, data=data, options={})
        await self.hass.config_entries.async_reload(self._entry.entry_id)
        return self.async_create_entry(title="", data={})

    async def async_step_gateway_options(self, user_input=None):
        settings = entry_settings(self._entry)
        errors = {}
        if user_input is not None:
            if not _validate_topics(user_input):
                errors["base"] = "invalid_topics"
            elif _topics_in_use(self.hass, maybe_normalize_mac(settings[CONF_MAC]), user_input):
                errors["base"] = "topics_in_use"
            elif user_input[CONF_TEMPORARY_REPORT] and user_input[CONF_TEMPORARY_REPORT_DURATION] <= user_input[CONF_TEMPORARY_REPORT_INTERVAL]:
                errors[CONF_TEMPORARY_REPORT_DURATION] = "duration_too_short"
            else:
                return await self._async_save(user_input)
        return self.async_show_form(step_id="gateway_options", data_schema=vol.Schema({
            vol.Required(CONF_MODEL, default=settings[CONF_MODEL]): vol.In(
                [model for model in QP_MODELS if model not in ADV_MODELS]
            ),
            **_settings_schema(settings),
        }), errors=errors)

    async def async_step_device_options(self, user_input=None):
        if user_input is not None:
            return await self._async_save(user_input)
        return self.async_show_form(
            step_id="device_options",
            data_schema=vol.Schema({
                vol.Required(CONF_MODEL, default=entry_settings(self._entry)[CONF_MODEL]): vol.In(
                    [model for model in QP_MODELS if model not in ADV_MODELS]
                )
            }),
        )

    async def async_step_sensor_options(self, user_input=None):
        gateways = gateway_entries(self.hass)
        settings = entry_settings(self._entry)
        if user_input is not None:
            await self._async_save(user_input)
            from . import async_sync_gateway_membership

            await async_sync_gateway_membership(self.hass)
            return self.async_create_entry(title="", data={})
        selected = resolve_gateway(settings, gateways) or _NO_GATEWAY
        choices = {_NO_GATEWAY: "—", **_gateway_choices(self.hass)}
        if selected not in choices:
            choices[selected] = selected
        return self.async_show_form(step_id="sensor_options", data_schema=vol.Schema({
            vol.Required(CONF_GATEWAY_MAC, default=selected): vol.In(choices),
            vol.Required(CONF_BLE_TIMEOUT, default=settings.get(CONF_BLE_TIMEOUT, DEFAULT_BLE_TIMEOUT)):
                vol.All(vol.Coerce(int), vol.Range(min=60, max=86400)),
        }))
