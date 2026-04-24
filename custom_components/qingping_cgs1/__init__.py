"""The Qingping Device integration."""
from __future__ import annotations

from homeassistant.config_entries import ConfigEntry
from homeassistant.const import Platform
from homeassistant.core import HomeAssistant, ServiceCall
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator
import voluptuous as vol
import logging

from .const import (
    CONF_HUMIDITY_OFFSET,
    CONF_TEMPERATURE_OFFSET,
    CONF_UPDATE_INTERVAL,
    DEFAULT_OFFSET,
    DEFAULT_TEMPORARY_REPORT_DURATION,
    DEFAULT_TEMPORARY_REPORT_INTERVAL,
    DEFAULT_UPDATE_INTERVAL,
    DOMAIN,
)
from .qingping_private import maybe_normalize_mac

PLATFORMS: list[Platform] = [Platform.SENSOR, Platform.NUMBER, Platform.SELECT, Platform.SWITCH, Platform.BUTTON, Platform.TIME]
PRIVATE_SERVICE_DOMAIN = "qingping_private"

_LOGGER = logging.getLogger(__name__)


def _private_handlers(hass: HomeAssistant, gateway_mac: str | None = None):
    """Return active CGDN1 private MQTT handlers."""
    normalized = maybe_normalize_mac(gateway_mac) if gateway_mac else None
    if gateway_mac and not normalized:
        _LOGGER.warning("Invalid CGDN1 gateway_mac in service call: %s", gateway_mac)
        return []
    handlers = []
    for entry_data in hass.data.get(DOMAIN, {}).values():
        if not isinstance(entry_data, dict):
            continue
        handler = entry_data.get("private_handler")
        if handler is None:
            continue
        if normalized and handler.gateway_mac != normalized:
            continue
        handlers.append(handler)
    return handlers


async def _async_register_private_services(hass: HomeAssistant) -> None:
    """Register Qingping private MQTT services once."""
    if hass.services.has_service(PRIVATE_SERVICE_DOMAIN, "refresh_ble_devices"):
        return

    gateway_schema = vol.Schema({vol.Optional("gateway_mac"): str})

    async def _call_handlers(call: ServiceCall, method: str, *args) -> None:
        handlers = _private_handlers(hass, call.data.get("gateway_mac"))
        if not handlers:
            _LOGGER.warning("No active CGDN1 private MQTT gateway handlers found")
            return
        for handler in handlers:
            await getattr(handler, method)(*args)

    async def refresh_ble_devices(call: ServiceCall) -> None:
        await _call_handlers(call, "async_publish_ble_devices")
        await _call_handlers(call, "async_publish_ble_devices_with_names")

    async def enable_temporary_report(call: ServiceCall) -> None:
        await _call_handlers(
            call,
            "async_enable_temporary_report",
            call.data.get("interval", DEFAULT_TEMPORARY_REPORT_INTERVAL),
            call.data.get("duration", DEFAULT_TEMPORARY_REPORT_DURATION),
        )

    async def enable_diagnostic_report(call: ServiceCall) -> None:
        await _call_handlers(call, "async_enable_diagnostic_report")

    async def configure_gateway_settings(call: ServiceCall) -> None:
        await _call_handlers(call, "async_configure_gateway_settings")

    hass.services.async_register(
        PRIVATE_SERVICE_DOMAIN,
        "refresh_ble_devices",
        refresh_ble_devices,
        schema=gateway_schema,
    )
    hass.services.async_register(
        PRIVATE_SERVICE_DOMAIN,
        "enable_temporary_report",
        enable_temporary_report,
        schema=vol.Schema({
            vol.Optional("gateway_mac"): str,
            vol.Optional("interval", default=DEFAULT_TEMPORARY_REPORT_INTERVAL): vol.Coerce(int),
            vol.Optional("duration", default=DEFAULT_TEMPORARY_REPORT_DURATION): vol.Coerce(int),
        }),
    )
    hass.services.async_register(
        PRIVATE_SERVICE_DOMAIN,
        "enable_diagnostic_report",
        enable_diagnostic_report,
        schema=gateway_schema,
    )
    hass.services.async_register(
        PRIVATE_SERVICE_DOMAIN,
        "configure_gateway_settings",
        configure_gateway_settings,
        schema=gateway_schema,
    )

async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Set up Qingping Device from a config entry."""
    hass.data.setdefault(DOMAIN, {})
    await _async_register_private_services(hass)

    async def async_update_data():
        """Fetch data from API endpoint.

        This is the place to pre-process the data to lookup tables
        so entities can quickly look up their data.
        """
        # Note: This is a placeholder. In a real scenario, you might
        # fetch data from an API or process local data here.
        return {}

    coordinator = DataUpdateCoordinator(
        hass,
        _LOGGER,
        name="sensor",
        update_method=async_update_data,
        # Polling interval. Will only be polled if there are subscribers.
        update_interval=None  # We're not polling regularly, only when offsets change
    )

    # Fetch initial data so we have data when entities subscribe
    await coordinator.async_config_entry_first_refresh()

    hass.data[DOMAIN][entry.entry_id] = {
        "config": entry.data,
        CONF_TEMPERATURE_OFFSET: entry.data.get(CONF_TEMPERATURE_OFFSET, DEFAULT_OFFSET),
        CONF_HUMIDITY_OFFSET: entry.data.get(CONF_HUMIDITY_OFFSET, DEFAULT_OFFSET),
        CONF_UPDATE_INTERVAL: entry.data.get(CONF_UPDATE_INTERVAL, DEFAULT_UPDATE_INTERVAL),
        "coordinator": coordinator,
    }

    coordinator.data = hass.data[DOMAIN][entry.entry_id]

    await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)
    return True

async def async_unload_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Unload a config entry."""
    entry_data = hass.data.get(DOMAIN, {}).get(entry.entry_id, {})
    handler = entry_data.get("private_handler") if isinstance(entry_data, dict) else None
    if handler is not None:
        handler.async_stop()
    unload_ok = await hass.config_entries.async_unload_platforms(entry, PLATFORMS)
    if unload_ok:
        hass.data[DOMAIN].pop(entry.entry_id)
    return unload_ok
