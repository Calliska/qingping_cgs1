"""Redacted diagnostics for Qingping gateway and sensor entries."""

from homeassistant.components.diagnostics import async_redact_data
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant

from .const import CONF_GATEWAY_MAC, CONF_MAC, CONF_MODEL, DOMAIN
from .topology import entry_settings, gateway_entries, resolve_gateway


async def async_get_config_entry_diagnostics(hass: HomeAssistant, entry: ConfigEntry) -> dict:
    settings = entry_settings(entry)
    data = hass.data.get(DOMAIN, {}).get(entry.entry_id, {})
    result = {
        "model": settings.get(CONF_MODEL),
        "gateway_mac": resolve_gateway(settings, gateway_entries(hass)),
        "last_packet_at": data.get("last_packet_at"),
        "last_measurement_at": data.get("last_measurement_at"),
    }
    if status := data.get("status_sensor"):
        result["status"] = status.native_value
        result["status_attributes"] = status.extra_state_attributes
    if handler := data.get("private_handler"):
        result["gateway"] = {
            "last_message_at": handler.last_seen_gateway,
            "last_measurement_report_at": handler.last_sensor_data_at,
            "requested_interval": handler.requested_interval,
            "observed_interval": handler.observed_interval,
            "interval_status": handler.report_status,
            "configured_ble_count": len(handler.ble_devices),
            "settings_confirmed": handler.settings_confirmed,
            "reported_interval": handler.last_gateway_settings.get("report_interval"),
            "reported_collect_interval": handler.last_gateway_settings.get("collect_interval"),
            "last_command_result": handler.last_result,
        }
    return async_redact_data(result, {CONF_MAC, CONF_GATEWAY_MAC})
