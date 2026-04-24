"""Tests for Qingping private MQTT pure helpers."""
from __future__ import annotations

import unittest

from custom_components.qingping_cgs1.qingping_private import (
    build_type_11_payload,
    build_type_12_payload,
    build_type_18_ack_payload,
    build_type_26_payload,
    down_topic,
    normalize_mac,
    parse_json_payload,
    result_description,
    up_topic,
)


class QingpingPrivateHelperTests(unittest.TestCase):
    """Pure helper tests."""

    def test_normalize_mac(self) -> None:
        self.assertEqual(normalize_mac("aa:bb-cc:dd-ee:ff"), "AABBCCDDEEFF")
        with self.assertRaises(ValueError):
            normalize_mac("not-a-mac")

    def test_topics(self) -> None:
        self.assertEqual(up_topic("aa:bb:cc:dd:ee:ff"), "qingping/AABBCCDDEEFF/up")
        self.assertEqual(down_topic("aa:bb:cc:dd:ee:ff"), "qingping/AABBCCDDEEFF/down")

    def test_type_11_payload(self) -> None:
        self.assertEqual(
            build_type_11_payload([{"mac": "11:22:33:44:55:66"}]),
            {"type": "11", "dev_list": ["112233445566"]},
        )

    def test_type_26_payload(self) -> None:
        self.assertEqual(
            build_type_26_payload([
                {"mac": "112233445566", "name": "Bedroom"},
                {"mac": "AABBCCDDEEFF"},
            ]),
            {
                "type": "26",
                "homekit_dev_list": [
                    {"mac": "112233445566", "name": "Bedroom"},
                    {"mac": "AABBCCDDEEFF", "name": "CGDK2 EEFF"},
                ],
            },
        )

    def test_type_12_payload(self) -> None:
        self.assertEqual(
            build_type_12_payload(60, 86400),
            {"type": "12", "up_itvl": "60", "duration": "86400"},
        )

    def test_type_18_ack_payload(self) -> None:
        self.assertEqual(
            build_type_18_ack_payload(1223, timestamp=1594815555),
            {"type": "18", "ack_id": 1223, "code": 0, "timestamp": 1594815555, "desc": ""},
        )

    def test_parse_json_payload(self) -> None:
        self.assertEqual(parse_json_payload(b'{"type":"10"}'), {"type": "10"})
        self.assertIsNone(parse_json_payload(b"{broken"))

    def test_result_description(self) -> None:
        self.assertEqual(result_description("-2"), "Bluetooth busy")


if __name__ == "__main__":
    unittest.main()
