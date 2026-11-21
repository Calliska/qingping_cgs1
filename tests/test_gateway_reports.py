"""Gateway coverage for routing and report validation."""

import time
import unittest
from types import SimpleNamespace

from custom_components.qingping_cgs1.adv_decoder import decode_qingping_adv_data
from custom_components.qingping_cgs1.qingping_private import build_type_17_gateway_settings_payload
from custom_components.qingping_cgs1.reports import Readings, decode_ble_report
from custom_components.qingping_cgs1.topology import configured_ble_devices, resolve_gateway

MAC = "582D34885C15"
OTHER = "582D34885C16"
GATEWAY = "CCB5D13196DC"
GATEWAY_2 = "CCDDEEFF1122"
ADVERTISEMENT = "14 16 cd fd 08 10 15 5c 88 34 2d 58 01 04 ce ff af 02 02 01 64"


def entry(mac, model, **data):
    return SimpleNamespace(data={"mac": mac, "model": model, **data}, options={}, disabled_by=None)


class ReportTests(unittest.TestCase):
    def test_signed_ble_and_prefix_variants(self):
        for advertisement in (
            ADVERTISEMENT, ADVERTISEMENT[6:], "fd cd" + ADVERTISEMENT[12:],
        ):
            result = decode_qingping_adv_data(advertisement)
            self.assertTrue(result["ok"])
            self.assertEqual(result["temperature"], -5.0)
            self.assertEqual(result["humidity"], 68.7)
            self.assertEqual(result["battery"], 100)

    def test_invalid_or_foreign_ble(self):
        for content in ("fdcd123456", ADVERTISEMENT[:-3], ADVERTISEMENT.replace("08 10", "08 0e"), None):
            self.assertFalse(decode_qingping_adv_data(content)["ok"])
        self.assertIsNone(decode_ble_report({"mac": OTHER, "adv_data": ADVERTISEMENT}))

    def test_history_cannot_replace_current_and_updates_are_per_field(self):
        history = Readings()
        now = time.time()
        valid, values = history.accept([
            {"timestamp": {"value": now - 70}, "temperature": {"value": 21}},
            {"timestamp": {"value": now - 90}, "temperature": {"value": 19}, "humidity": {"value": 40}},
        ], now, historic=True)
        self.assertTrue(valid)
        self.assertEqual(values, {"temperature": {"value": 21}, "humidity": {"value": 40}})
        self.assertEqual(history.accept([{"temperature": {"value": 22}}], now)[1]["temperature"]["value"], 22)
        self.assertEqual(history.accept([{"temperature": 17}], now + 1, historic=True), (True, {}))
        self.assertEqual(history.accept([{"temperature": 90, "humidity": 255, "battery": 255}], now + 2)[1], {"temperature": 90})
        self.assertEqual(history.accept([{"temperature": -5}], now + 3)[1], {"temperature": -5})

    def test_settings_have_documented_envelope(self):
        payload = build_type_17_gateway_settings_payload(
            23, report_interval=125, collect_interval=10, co2_sampling_interval=2,
            pm_sampling_interval=1, temperature_unit="C", timestamp=111,
        )
        self.assertEqual((payload["id"], payload["timestamp"], payload["need_ack"]), (23, 111, 1))
        self.assertEqual(payload["setting"]["report_interval"], 125)
        self.assertNotIn("timezone", payload["setting"])


class TopologyTests(unittest.TestCase):
    def test_two_gateways_reassignment_and_detach(self):
        one = entry(GATEWAY, "CGDN1", ble_devices=MAC)
        two = entry(GATEWAY_2, "CGDN1")
        sensor = entry(MAC, "CGDK2", name="Kitchen")
        other = entry(OTHER, "CGDK2", gateway_mac=GATEWAY_2)
        gateways = {GATEWAY: one, GATEWAY_2: two}
        self.assertEqual(resolve_gateway(sensor.data, gateways), GATEWAY)
        self.assertEqual(configured_ble_devices(GATEWAY, [sensor, other], gateways), [{"mac": MAC, "name": "Kitchen"}])
        self.assertEqual([value["mac"] for value in configured_ble_devices(GATEWAY_2, [sensor, other], gateways)], [OTHER])
        sensor.data["gateway_mac"] = GATEWAY_2
        self.assertEqual(configured_ble_devices(GATEWAY, [sensor, other], gateways), [])
        self.assertEqual(len(configured_ble_devices(GATEWAY_2, [sensor, other], gateways)), 2)
        sensor.data["gateway_mac"] = ""
        self.assertIsNone(resolve_gateway(sensor.data, {GATEWAY: one}))
        self.assertEqual(configured_ble_devices(GATEWAY, [sensor, other], gateways), [])

    def test_ambiguous_legacy_lists_are_not_published(self):
        one = entry(GATEWAY, "CGDN1", ble_devices=MAC)
        two = entry(GATEWAY_2, "CGDN1", ble_devices=MAC)
        sensor = entry(MAC, "CGDK2")
        gateways = {GATEWAY: one, GATEWAY_2: two}
        self.assertIsNone(resolve_gateway(sensor.data, gateways))
        self.assertEqual(configured_ble_devices(GATEWAY, [], gateways), [])
