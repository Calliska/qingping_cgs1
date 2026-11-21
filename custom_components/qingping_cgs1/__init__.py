"""Set up Qingping devices and manage gateway membership."""

from __future__ import annotations

import logging

import voluptuous as vol
from homeassistant.config_entries import ConfigEntry
from homeassistant.const import Platform
from homeassistant.core import HomeAssistant, ServiceCall
from homeassistant.exceptions import ConfigEntryError, HomeAssistantError
from homeassistant.helpers import device_registry as dr
from homeassistant.helpers.event import async_call_later
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator

from .const import (
    ADV_MODELS,
    CONF_BLE_DEVICES,
    CONF_GATEWAY_MAC,
    CONF_HUMIDITY_OFFSET,
    CONF_MAC,
    CONF_MODEL,
    CONF_TEMPERATURE_OFFSET,
    CONF_TEMPORARY_REPORT_INTERVAL,
    CONF_TIMEZONE,
    CONF_UPDATE_INTERVAL,
    DEFAULT_OFFSET,
    DEFAULT_TEMPORARY_REPORT_INTERVAL,
    DEFAULT_UPDATE_INTERVAL,
    DOMAIN,
)
from .qingping_private import maybe_normalize_mac
from .topology import entry_settings, gateway_entries, parse_ble_devices, resolve_gateway

PLATFORMS = [Platform.SENSOR, Platform.NUMBER, Platform.SELECT, Platform.SWITCH, Platform.BUTTON, Platform.TIME]
_LOGGER = logging.getLogger(__name__)


def _private_handlers(hass: HomeAssistant, gateway_mac: str | None = None):
    """Find loaded gateways without consulting unrelated device entries."""
    normalized = maybe_normalize_mac(gateway_mac) if gateway_mac else None
    if gateway_mac and not normalized:
        raise HomeAssistantError("Invalid Qingping gateway MAC address")
    data = hass.data.get(DOMAIN, {})
    return [
        handler for mac, entry in gateway_entries(hass).items()
        if (normalized is None or mac == normalized)
        and (handler := data.get(entry.entry_id, {}).get("private_handler")) is not None
    ]


async def _async_register_private_services(hass: HomeAssistant) -> None:
    if hass.services.has_service(DOMAIN, "refresh_ble_devices"):
        return

    async def _call_handlers(call: ServiceCall, method: str, *args) -> None:
        handlers = _private_handlers(hass, call.data.get(CONF_GATEWAY_MAC))
        if not handlers:
            raise HomeAssistantError("No matching Qingping gateway is loaded")
        failed = []
        for handler in handlers:
            if not await getattr(handler, method)(*args):
                failed.append(handler.gateway_mac)
        if failed:
            raise HomeAssistantError(f"Qingping could not publish to: {', '.join(failed)}")

    async def refresh_ble_devices(call: ServiceCall) -> None:
        await _call_handlers(call, "async_sync_devices", True)

    async def enable_temporary_report(call: ServiceCall) -> None:
        await _call_handlers(call, "async_enable_temporary_report", call.data.get("interval"), call.data.get("duration"))

    async def enable_diagnostic_report(call: ServiceCall) -> None:
        await _call_handlers(call, "async_enable_diagnostic_report")

    async def configure_gateway_settings(call: ServiceCall) -> None:
        await _call_handlers(call, "async_configure_gateway_settings")

    async def read_gateway_settings(call: ServiceCall) -> None:
        await _call_handlers(call, "async_read_settings")

    gateway_schema = vol.Schema({vol.Optional(CONF_GATEWAY_MAC): str})
    for name, handler in (
        ("refresh_ble_devices", refresh_ble_devices),
        ("enable_diagnostic_report", enable_diagnostic_report),
        ("configure_gateway_settings", configure_gateway_settings),
        ("read_gateway_settings", read_gateway_settings),
    ):
        hass.services.async_register(DOMAIN, name, handler, schema=gateway_schema)
    hass.services.async_register(
        DOMAIN, "enable_temporary_report", enable_temporary_report,
        schema=gateway_schema.extend({
            vol.Optional("interval"): vol.All(vol.Coerce(int), vol.Range(min=1, max=86400)),
            vol.Optional("duration"): vol.All(vol.Coerce(int), vol.Range(min=1, max=604800)),
        }),
    )


def _device_id(hass: HomeAssistant, entry: ConfigEntry) -> str | None:
    identifier = DOMAIN, entry.data[CONF_MAC]
    if lookup := getattr(dr, "async_get_device_id_by_identifier", None):
        try:
            return lookup(hass, identifier, config_entry_id=entry.entry_id)
        except ValueError:
            return None
    device = dr.async_get(hass).async_get_device(identifiers={identifier})
    return device.id if device else None


async def async_sync_gateway_membership(hass: HomeAssistant) -> None:
    """Refresh device links and independently reconcile each gateway list."""
    gateways = gateway_entries(hass)
    registry = dr.async_get(hass)
    for entry in hass.config_entries.async_entries(DOMAIN, include_disabled=False):
        settings = entry_settings(entry)
        if settings.get(CONF_MODEL) not in ADV_MODELS or not (device_id := _device_id(hass, entry)):
            continue
        gateway = gateways.get(resolve_gateway(settings, gateways))
        registry.async_update_device(device_id, via_device_id=_device_id(hass, gateway) if gateway else None)
    for handler in _private_handlers(hass):
        await handler.async_sync_devices()


async def async_migrate_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Preserve identifiers and infer ownership for unambiguous legacy setups."""
    if entry.version > 2:
        return False
    if entry.version == 2:
        return True
    data = entry_settings(entry)
    model = data.get(CONF_MODEL)
    if model in ADV_MODELS and CONF_GATEWAY_MAC not in data:
        if gateway := resolve_gateway(data, gateway_entries(hass)):
            data[CONF_GATEWAY_MAC] = gateway
    elif model == "CGDN1":
        if (
            CONF_UPDATE_INTERVAL in data
            and data.get(CONF_TEMPORARY_REPORT_INTERVAL, DEFAULT_TEMPORARY_REPORT_INTERVAL)
            == DEFAULT_TEMPORARY_REPORT_INTERVAL
        ):
            data[CONF_TEMPORARY_REPORT_INTERVAL] = data[CONF_UPDATE_INTERVAL]
        if CONF_TIMEZONE in entry.options:
            data["timezone_wire"] = entry.options[CONF_TIMEZONE]
            data[CONF_TIMEZONE] = float(entry.options[CONF_TIMEZONE]) / 10
    hass.config_entries.async_update_entry(entry, data=data, options={}, version=2)
    return True


async def async_remove_entry(hass: HomeAssistant, entry: ConfigEntry) -> None:
    """Remove a deleted sensor from any legacy lists before synchronizing."""
    if entry.data.get(CONF_MODEL) not in ADV_MODELS:
        return
    mac = maybe_normalize_mac(entry.data.get(CONF_MAC))
    for gateway in gateway_entries(hass).values():
        data = dict(gateway.data)
        options = dict(gateway.options)
        changed = False
        for mapping in (data, options):
            legacy = parse_ble_devices(mapping.get(CONF_BLE_DEVICES))
            if mac in legacy:
                legacy.pop(mac)
                mapping[CONF_BLE_DEVICES] = [{"mac": address, "name": name} for address, name in legacy.items()]
                changed = True
        if changed:
            hass.config_entries.async_update_entry(gateway, data=data, options=options)

    async def _after_remove(_):
        await async_sync_gateway_membership(hass)

    # HA versions differ on whether the entry is still indexed during this hook.
    async_call_later(hass, 0, _after_remove)


async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Set up one push-only device."""
    hass.data.setdefault(DOMAIN, {})
    if not maybe_normalize_mac(entry.data.get(CONF_MAC)):
        raise ConfigEntryError("Qingping device has an invalid MAC address")
    await _async_register_private_services(hass)

    async def async_update_data():
        """Keep push state intact when an existing control requests a refresh."""
        return hass.data[DOMAIN].get(entry.entry_id, {})

    coordinator = DataUpdateCoordinator(hass, _LOGGER, name=entry.title, update_method=async_update_data)
    hass.data[DOMAIN][entry.entry_id] = {
        CONF_TEMPERATURE_OFFSET: entry.data.get(CONF_TEMPERATURE_OFFSET, DEFAULT_OFFSET),
        CONF_HUMIDITY_OFFSET: entry.data.get(CONF_HUMIDITY_OFFSET, DEFAULT_OFFSET),
        CONF_UPDATE_INTERVAL: entry.data.get(CONF_UPDATE_INTERVAL, DEFAULT_UPDATE_INTERVAL),
        "coordinator": coordinator,
    }
    coordinator.async_set_updated_data(hass.data[DOMAIN][entry.entry_id])
    try:
        await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)
    except Exception:
        hass.data[DOMAIN].pop(entry.entry_id, None)
        raise
    return True


async def async_unload_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Unload entities before discarding their shared state."""
    unload_ok = await hass.config_entries.async_unload_platforms(entry, PLATFORMS)
    if unload_ok:
        hass.data[DOMAIN].pop(entry.entry_id, None)
    return unload_ok
