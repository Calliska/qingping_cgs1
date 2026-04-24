"""Pure helpers for Qingping private MQTT gateway protocol."""
from __future__ import annotations

import json
import re
import time
from collections.abc import Iterable
from typing import Any

from .const import (
    DEFAULT_TEMPORARY_REPORT_DURATION,
    DEFAULT_TEMPORARY_REPORT_INTERVAL,
    DIAGNOSTIC_REPORT_DURATION,
    DIAGNOSTIC_REPORT_INTERVAL,
    MQTT_TOPIC_PREFIX,
)

MAC_RE = re.compile(r"^[0-9A-F]{12}$")

RESULT_CODES = {
    "1": "Success",
    "-1": "Connect timeout",
    "-2": "Bluetooth busy",
    "-3": "write data failed",
    "-4": "read data failed",
    "-5": "Open BLE notification failed",
    "-6": "Close BLE notification failed",
    "-7": "Unbinding device",
    "-8": "Connect exception",
    "-9": "Device offline",
    "-10": "Sparrow unbinding",
}


def normalize_mac(mac: str) -> str:
    """Normalize a MAC address to uppercase 12-char Qingping format."""
    normalized = str(mac or "").replace(":", "").replace("-", "").upper()
    if not MAC_RE.match(normalized):
        raise ValueError(f"Invalid MAC address: {mac!r}")
    return normalized


def maybe_normalize_mac(mac: Any) -> str | None:
    """Normalize a MAC address, returning None instead of raising."""
    try:
        return normalize_mac(str(mac))
    except (TypeError, ValueError):
        return None


def up_topic(gateway_mac: str) -> str:
    """Return the CGDN1 up topic."""
    return f"{MQTT_TOPIC_PREFIX}/{normalize_mac(gateway_mac)}/up"


def down_topic(gateway_mac: str) -> str:
    """Return the CGDN1 down topic."""
    return f"{MQTT_TOPIC_PREFIX}/{normalize_mac(gateway_mac)}/down"


def topic_mac_variants(gateway_mac: str) -> list[str]:
    """Return topic MAC variants for case-sensitive MQTT brokers."""
    raw = str(gateway_mac or "").replace(":", "").replace("-", "")
    normalized = normalize_mac(gateway_mac)
    variants = [raw, normalized, normalized.lower()]
    return list(dict.fromkeys(variant for variant in variants if variant))


def up_topic_variants(gateway_mac: str) -> list[str]:
    """Return possible CGDN1 up topics for existing mixed-case entries."""
    return [f"{MQTT_TOPIC_PREFIX}/{variant}/up" for variant in topic_mac_variants(gateway_mac)]


def down_topic_variants(gateway_mac: str) -> list[str]:
    """Return possible CGDN1 down topics for existing mixed-case entries."""
    return [f"{MQTT_TOPIC_PREFIX}/{variant}/down" for variant in topic_mac_variants(gateway_mac)]


def build_type_11_payload(ble_devices: Iterable[dict[str, Any]], request_id: Any = None) -> dict[str, Any]:
    """Build BLE device list response."""
    payload = {
        "type": "11",
        "dev_list": [normalize_mac(device["mac"]) for device in ble_devices],
    }
    if request_id is not None:
        payload["id"] = request_id
    return payload


def build_type_26_payload(ble_devices: Iterable[dict[str, Any]], request_id: Any = None) -> dict[str, Any]:
    """Build BLE device list response with names."""
    homekit_dev_list = []
    for device in ble_devices:
        mac = normalize_mac(device["mac"])
        name = str(device.get("name") or f"CGDK2 {mac[-4:]}")
        homekit_dev_list.append({"mac": mac, "name": name})
    payload = {"type": "26", "homekit_dev_list": homekit_dev_list}
    if request_id is not None:
        payload["id"] = request_id
    return payload


def build_type_12_payload(
    interval: int = DEFAULT_TEMPORARY_REPORT_INTERVAL,
    duration: int = DEFAULT_TEMPORARY_REPORT_DURATION,
) -> dict[str, Any]:
    """Build temporary report mode command."""
    return {"type": "12", "up_itvl": str(int(interval)), "duration": str(int(duration))}


def build_diagnostic_report_payload() -> dict[str, Any]:
    """Build short diagnostic report mode command."""
    return build_type_12_payload(DIAGNOSTIC_REPORT_INTERVAL, DIAGNOSTIC_REPORT_DURATION)


def build_type_18_ack_payload(ack_id: int | str, timestamp: int | None = None) -> dict[str, Any]:
    """Build history report ACK payload."""
    return {
        "type": "18",
        "ack_id": ack_id,
        "code": 0,
        "timestamp": int(timestamp if timestamp is not None else time.time()),
        "desc": "",
    }


def build_type_17_gateway_settings_payload(
    message_id: int,
    *,
    report_interval: int,
    collect_interval: int,
    co2_sampling_interval: int,
    pm_sampling_interval: int,
    temperature_unit: str,
    timezone: int,
) -> dict[str, Any]:
    """Build persistent CGDN1 gateway settings command."""
    return {
        "id": int(message_id),
        "need_ack": 1,
        "type": "17",
        "setting": {
            "report_interval": int(report_interval),
            "collect_interval": int(collect_interval),
            "co2_sampling_interval": int(co2_sampling_interval),
            "pm_sampling_interval": int(pm_sampling_interval),
            "temperature_unit": str(temperature_unit),
            "timezone": int(timezone),
        },
    }


def result_description(result: Any) -> str:
    """Return a human-readable Qingping result description."""
    result_code = str(result)
    return RESULT_CODES.get(result_code, f"Unknown result code {result_code}")


def parse_json_payload(raw_payload: bytes | str) -> dict[str, Any] | None:
    """Parse a JSON MQTT payload defensively."""
    try:
        if isinstance(raw_payload, bytes):
            raw_payload = raw_payload.decode("utf-8", errors="ignore")
        payload = json.loads(raw_payload)
    except (TypeError, json.JSONDecodeError, UnicodeDecodeError):
        return None
    return payload if isinstance(payload, dict) else None
