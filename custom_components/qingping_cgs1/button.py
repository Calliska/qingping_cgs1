"""Support for Qingping Device button entities."""
from __future__ import annotations

from homeassistant.components.button import ButtonEntity
from homeassistant.components import mqtt
from homeassistant.config_entries import ConfigEntry
from homeassistant.const import CONF_NAME, CONF_MAC, CONF_MODEL
from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from homeassistant.helpers.update_coordinator import CoordinatorEntity
from homeassistant.helpers.entity import EntityCategory
from homeassistant.exceptions import HomeAssistantError

from .const import DOMAIN, TLV_MODELS, ADV_MODELS
from .tlv_encoder import tlv_encode

async def async_setup_entry(
    hass: HomeAssistant,
    config_entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Set up Qingping Device button entities from a config entry."""
    mac = config_entry.data[CONF_MAC]
    name = config_entry.data[CONF_NAME]
    model = config_entry.data[CONF_MODEL]
    if model in ADV_MODELS:
        return
    coordinator = hass.data[DOMAIN][config_entry.entry_id]["coordinator"]

    device_info = {
        "identifiers": {(DOMAIN, mac)},
        "name": name,
        "manufacturer": "Qingping",
        "model": model,
    }

    entities = []

    if model == "CGDN1":
        entities.extend([
            QingpingDeviceManualCalibrationButton(config_entry, mac, name, device_info),
            QingpingGatewayButton(config_entry, mac, device_info, "sync_ble_devices", "async_sync_devices", True),
            QingpingGatewayButton(config_entry, mac, device_info, "read_gateway_settings", "async_read_settings"),
        ])

    # Add CO2 calibration button for TLV devices with CO2 sensor
    if model in TLV_MODELS and model in ["CGP22C", "CGR1W", "CGR1PW"]:
        entities.append(
            QingpingTLVCO2CalibrationButton(coordinator, config_entry, mac, name, device_info)
        )

    if entities:
        async_add_entities(entities)


class QingpingTLVCO2CalibrationButton(CoordinatorEntity, ButtonEntity):
    """Representation of a Qingping TLV device CO2 calibration button."""

    def __init__(self, coordinator, config_entry, mac, name, device_info):
        """Initialize the button."""
        super().__init__(coordinator)
        self._config_entry = config_entry
        self._mac = mac
        self._attr_name = f"{name} CO2 Calibration"
        self._attr_unique_id = f"{mac}_co2_calibration"
        self._attr_device_info = device_info
        self._attr_entity_category = EntityCategory.CONFIG
        self._attr_icon = "mdi:tune-vertical-variant"

    async def async_press(self) -> None:
        """Handle the button press."""
        # Send TLV command to device (KEY 0x41)
        # 1 = Perform calibration
        packets = {
            0x41: bytes([1])
        }
        payload = tlv_encode(0x32, packets)

        topic = f"qingping/{self._mac}/down"
        await mqtt.async_publish(self.hass, topic, payload)

class QingpingDeviceManualCalibrationButton(ButtonEntity):
    """Button to trigger manual CO2 calibration."""

    def __init__(self, config_entry, mac, name, device_info):
        """Initialize the button."""
        self._config_entry = config_entry
        self._mac = mac
        self._attr_name = f"{name} Manual Calibration"
        self._attr_unique_id = f"{mac}_manual_calibration"
        self._attr_device_info = device_info
        self._attr_entity_category = EntityCategory.CONFIG
        self._attr_icon = "mdi:tune-vertical"

    async def async_press(self) -> None:
        """Handle the button press."""
        handler = self.hass.data[DOMAIN].get(self._config_entry.entry_id, {}).get("private_handler")
        if handler is None or not await handler.async_publish({"type": "29"}):
            raise HomeAssistantError("Could not send the Qingping calibration request")


class QingpingGatewayButton(ButtonEntity):
    """Provide direct access to gateway commands on its device page."""

    _attr_has_entity_name = True
    _attr_entity_category = EntityCategory.CONFIG

    def __init__(self, entry, mac, device_info, translation_key, method, *args):
        self._entry = entry
        self._method = method
        self._args = args
        self._attr_unique_id = f"{mac}_{translation_key}"
        self._attr_translation_key = translation_key
        self._attr_device_info = device_info

    async def async_press(self) -> None:
        handler = self.hass.data[DOMAIN].get(self._entry.entry_id, {}).get("private_handler")
        if handler is None or not await getattr(handler, self._method)(*self._args):
            raise HomeAssistantError("Could not publish to the Qingping gateway")
