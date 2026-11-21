"""Decode Qingping CGDK2 service data received through a gateway."""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any

_QINGPING_UUIDS = (b"\xcd\xfd", b"\xfd\xcd")
_CGDK2_DEVICE_TYPE = 0x10


def _service_data(raw: bytes) -> Iterator[bytes]:
    """Accept the complete advertisement or the UUID-prefixed service data."""
    if raw[:2] in _QINGPING_UUIDS:
        yield raw[2:]
        return
    pos = 0
    while pos < len(raw) and raw[pos]:
        end = pos + raw[pos] + 1
        if end > len(raw):
            return
        if raw[pos + 1] == 0x16 and raw[pos + 2 : pos + 4] in _QINGPING_UUIDS:
            yield raw[pos + 4 : end]
        pos = end


def decode_qingping_adv_data(adv_hex: Any) -> dict[str, Any]:
    """Return only supported measurements from a complete CGDK2 frame."""
    empty = {"ok": False, "temperature": None, "humidity": None, "battery": None, "payload_mac": None}
    if not isinstance(adv_hex, str):
        return empty
    try:
        raw = bytes.fromhex(adv_hex)
    except ValueError:
        return empty

    for data in _service_data(raw):
        if len(data) < 8 or data[1] != _CGDK2_DEVICE_TYPE:
            continue
        result = {**empty, "payload_mac": data[2:8][::-1].hex().upper()}
        pos = 8
        while pos + 2 <= len(data):
            kind, size = data[pos : pos + 2]
            end = pos + 2 + size
            if end > len(data):
                break
            value = data[pos + 2 : end]
            if kind == 0x01 and size == 4:
                temperature = int.from_bytes(value[:2], "little", signed=True) / 10
                humidity = int.from_bytes(value[2:], "little") / 10
                if -100 <= temperature <= 100:
                    result["temperature"] = temperature
                if 0 <= humidity <= 100:
                    result["humidity"] = humidity
            elif kind == 0x02 and size == 1 and value[0] <= 100:
                result["battery"] = value[0]
            pos = end
        if pos == len(data) and any(result[key] is not None for key in ("temperature", "humidity", "battery")):
            result["ok"] = True
            return result
    return empty
