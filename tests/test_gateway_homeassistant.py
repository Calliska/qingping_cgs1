"""Exercise the real HA configuration flow, MQTT callbacks and entities."""

import json
import time
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest
from homeassistant import config_entries, data_entry_flow
from homeassistant.helpers import device_registry as dr, entity_registry as er
from pytest_homeassistant_custom_component.common import MockConfigEntry, async_fire_mqtt_message

from custom_components.qingping_cgs1.const import DOMAIN

from .test_gateway_reports import ADVERTISEMENT, GATEWAY, GATEWAY_2, MAC, OTHER

pytestmark = pytest.mark.usefixtures("enable_custom_integrations")

UP = "qingping/ccb5d13196dc/up"
DOWN = "qingping/ccb5d13196dc/down"
SECOND_UP = "house/second/uplink"
SECOND_DOWN = "house/second/downlink"


@pytest.fixture(autouse=True)
async def unload_integration(hass, mock_hass_config, mqtt_mock_entry):
    # The fixture simulates transport state without a real paho network loop.
    with patch("homeassistant.components.mqtt.client.MQTT._async_start_misc_periodic"):
        await mqtt_mock_entry()
    yield
    for domain in (DOMAIN, "mqtt"):
        for entry in hass.config_entries.async_entries(domain):
            if entry.state == config_entries.ConfigEntryState.LOADED:
                await hass.config_entries.async_unload(entry.entry_id)
    await hass.async_block_till_done()


async def setup_device(hass, mac, model, **settings):
    entry = MockConfigEntry(
        domain=DOMAIN, title=settings.get("name", mac), unique_id=mac,
        data={"mac": mac, "model": model, "name": mac, **settings}, version=2,
    )
    entry.add_to_hass(hass)
    assert await hass.config_entries.async_setup(entry.entry_id)
    await hass.async_block_till_done()
    return entry


async def send(hass, topic, payload, *, retain=False):
    async_fire_mqtt_message(hass, topic, json.dumps(payload), retain=retain)
    await hass.async_block_till_done()


def packets(publisher):
    return [(call.args[1], json.loads(call.args[2])) for call in publisher.await_args_list]


@pytest.mark.asyncio
async def test_two_gateways_reports_and_sensor_transfer(hass):
    with patch("homeassistant.components.mqtt.async_publish", new_callable=AsyncMock) as publisher:
        first = await setup_device(hass, GATEWAY, "CGDN1")
        second = await setup_device(
            hass, GATEWAY_2, "CGDN1", gateway_up_topic=SECOND_UP, gateway_down_topic=SECOND_DOWN,
        )
        sensor = await setup_device(hass, MAC, "CGDK2", name="Kitchen", gateway_mac=GATEWAY)
        registry = er.async_get(hass)
        entity_id = registry.async_get_entity_id("sensor", DOMAIN, f"{MAC}_temperature")
        assert entity_id is not None
        payload = {"type": "9", "mac": MAC, "adv_data": ADVERTISEMENT, "rssi": "-55"}
        await send(hass, SECOND_UP, payload)
        assert hass.states.get(entity_id).state == "unavailable"
        await send(hass, UP, payload, retain=True)
        assert hass.states.get(entity_id).state == "unavailable"
        await send(hass, UP, payload)
        assert float(hass.states.get(entity_id).state) == -5

        publisher.reset_mock()
        await send(hass, UP, {"type": 10, "id": 91})
        await send(hass, SECOND_UP, {"type": "10", "id": 92})
        replies = [(topic, packet) for topic, packet in packets(publisher) if "id" in packet]
        assert replies == [
            (DOWN, {"type": "11", "dev_list": [MAC], "id": 91}),
            (SECOND_DOWN, {"type": "11", "dev_list": [], "id": 92}),
        ]

        device_registry = dr.async_get(hass)
        child = device_registry.async_get_device(identifiers={(DOMAIN, MAC)})
        first_device = device_registry.async_get_device(identifiers={(DOMAIN, GATEWAY)})
        second_device = device_registry.async_get_device(identifiers={(DOMAIN, GATEWAY_2)})
        assert child.via_device_id == first_device.id
        flow = await hass.config_entries.options.async_init(sensor.entry_id)
        result = await hass.config_entries.options.async_configure(
            flow["flow_id"], {"gateway_mac": GATEWAY_2, "ble_timeout": 600}
        )
        assert result["type"] == data_entry_flow.FlowResultType.CREATE_ENTRY
        await hass.async_block_till_done()
        assert registry.async_get_entity_id("sensor", DOMAIN, f"{MAC}_temperature") == entity_id
        assert device_registry.async_get(child.id).via_device_id == second_device.id
        assert hass.data[DOMAIN][first.entry_id]["private_handler"].ble_devices == []
        assert hass.data[DOMAIN][second.entry_id]["private_handler"].ble_devices == [{"mac": MAC, "name": "Kitchen"}]
        await send(hass, SECOND_UP, payload)
        assert float(hass.states.get(entity_id).state) == -5


@pytest.mark.asyncio
async def test_gateway_number_and_no_duplicate_callbacks_after_reload(hass):
    with patch("homeassistant.components.mqtt.async_publish", new_callable=AsyncMock) as publisher:
        gateway = await setup_device(hass, GATEWAY, "CGDN1")
        await send(hass, UP, {"type": "13", "sw_version": "test"})
        number_id = er.async_get(hass).async_get_entity_id("number", DOMAIN, f"{GATEWAY}_update_interval")
        publisher.reset_mock()
        await hass.services.async_call("number", "set_value", {"entity_id": number_id, "value": 125}, blocking=True)
        assert packets(publisher) == [(DOWN, {"type": "12", "up_itvl": "125", "duration": "86400"})]
        assert gateway.data["temporary_report_interval"] == 125
        for _ in range(2):
            assert await hass.config_entries.async_reload(gateway.entry_id)
            await hass.async_block_till_done()
        publisher.reset_mock()
        await send(hass, UP, {"type": "25", "id": 81})
        assert packets(publisher) == [(DOWN, {"type": "26", "homekit_dev_list": [], "id": 81})]
        assert gateway.data["temporary_report_interval"] == 125


@pytest.mark.asyncio
async def test_discovery_requires_confirmation_unless_enabled(hass):
    with patch("homeassistant.components.mqtt.async_publish", new_callable=AsyncMock):
        one = await setup_device(hass, GATEWAY, "CGDN1")
        await setup_device(hass, GATEWAY_2, "CGDN1", auto_add_ble=True)
        await send(hass, UP, {"type": 9, "mac": MAC, "adv_data": ADVERTISEMENT})
        assert [entry for entry in hass.config_entries.async_entries(DOMAIN) if entry.unique_id == MAC] == []
        flow = next(flow for flow in hass.config_entries.flow.async_progress() if flow["handler"] == DOMAIN)
        result = await hass.config_entries.flow.async_configure(
            flow["flow_id"], {"name": "Kitchen", "gateway_mac": one.data["mac"]}
        )
        assert result["type"] == data_entry_flow.FlowResultType.CREATE_ENTRY
        other_up = f"qingping/{GATEWAY_2.lower()}/up"
        await send(hass, other_up, {"type": 9, "mac": OTHER, "adv_data": ADVERTISEMENT.replace("15 5c", "16 5c")})
        entry = next(entry for entry in hass.config_entries.async_entries(DOMAIN) if entry.unique_id == OTHER)
        assert entry.source == config_entries.SOURCE_INTEGRATION_DISCOVERY
        assert entry.data["gateway_mac"] == GATEWAY_2


@pytest.mark.asyncio
async def test_temporary_mode_renews_across_48_simulated_hours(hass):
    seconds = [100.0]
    transmitted = []

    async def note_publish(_, topic, raw, **kwargs):
        packet = json.loads(raw)
        if packet["type"] == "12":
            transmitted.append((seconds[0], topic, packet))

    clock = SimpleNamespace(time=time.time, monotonic=lambda: seconds[0])
    with (
        patch("homeassistant.components.mqtt.async_publish", side_effect=note_publish),
        patch("custom_components.qingping_cgs1.private_mqtt.time", clock),
    ):
        gateway = await setup_device(
            hass, GATEWAY, "CGDN1", temporary_report_interval=125,
            gateway_up_topic=UP, gateway_down_topic=DOWN,
        )
        handler = hass.data[DOMAIN][gateway.entry_id]["private_handler"]
        await handler.async_enable_temporary_report()
        transmitted[:] = transmitted[-1:]
        for elapsed in range(15, 172800 + 15, 15):
            seconds[0] = 100 + elapsed
            await handler._async_tick()
        assert len(transmitted) == 5
        assert all(topic == DOWN and packet["up_itvl"] == "125" for _, topic, packet in transmitted)
        assert all(new[0] - old[0] < int(old[2]["duration"]) for old, new in zip(transmitted, transmitted[1:]))


@pytest.mark.asyncio
async def test_legacy_assignment_preserves_identity(hass):
    with patch("homeassistant.components.mqtt.async_publish", new_callable=AsyncMock):
        await setup_device(hass, GATEWAY, "CGDN1")
        lowercase = MAC.lower()
        old = MockConfigEntry(
            domain=DOMAIN, title="Legacy", unique_id=lowercase, version=1,
            data={"mac": lowercase, "model": "CGDK2", "name": "Legacy"},
        )
        old.add_to_hass(hass)
        assert await hass.config_entries.async_setup(old.entry_id)
        await hass.async_block_till_done()
        assert old.version == 2
        assert old.unique_id == lowercase
        assert old.data["gateway_mac"] == GATEWAY
        assert er.async_get(hass).async_get_entity_id("sensor", DOMAIN, f"{lowercase}_temperature")
