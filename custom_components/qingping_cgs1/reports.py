"""Validate and order JSON and BLE readings before they reach HA entities."""

from __future__ import annotations

import math
from typing import Any

from .adv_decoder import decode_qingping_adv_data
from .qingping_private import maybe_normalize_mac

_RANGES = {"temperature": (-100, 100), "humidity": (0, 100), "battery": (0, 100)}
_MEASUREMENTS = {
    *_RANGES, "co2", "pm25", "pm10", "tvoc", "tvoc_index", "noise",
    "pressure", "light", "signal_strength",
}


def numeric(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return number if math.isfinite(number) else None


def _value(item: Any) -> Any:
    return item.get("value") if isinstance(item, dict) else item


def decode_ble_report(payload: dict[str, Any]) -> tuple[str, dict[str, Any]] | None:
    mac = maybe_normalize_mac(payload.get("mac"))
    reading = decode_qingping_adv_data(payload.get("adv_data"))
    if not mac or not reading["ok"] or reading["payload_mac"] != mac:
        return None
    if (rssi := numeric(payload.get("rssi"))) is not None and -127 <= rssi <= 20:
        reading["signal_strength"] = int(rssi)
    return mac, reading


class Readings:
    """Keep a per-measurement watermark regardless of MQTT arrival order."""

    def __init__(self) -> None:
        self._watermarks: dict[str, float] = {}
        self.last_measurement_at: float | None = None

    def accept(
        self,
        rows: Any,
        received_at: float,
        *,
        historic: bool = False,
        fallback_timestamp: Any = None,
        allowed: set[str] | None = None,
    ) -> tuple[bool, dict[str, Any]]:
        """Return whether the report was valid and the latest new value per field."""
        measurements = _MEASUREMENTS if allowed is None else allowed
        valid = False
        latest = {}
        if not isinstance(rows, list):
            return valid, latest
        for row in rows:
            if not isinstance(row, dict):
                continue
            stamp = numeric(_value(row.get("timestamp"))) or numeric(_value(fallback_timestamp))
            if stamp is not None and not 0 < stamp <= received_at + 300:
                stamp = None
            if stamp is None and not historic:
                stamp = received_at
            for field in row.keys() & measurements:
                item = row[field]
                value = numeric(_value(item))
                if value is None:
                    continue
                if field in _RANGES:
                    low, high = _RANGES[field]
                    if not low <= value <= high:
                        continue
                if field not in {"battery", "pm25", "pm10"} and isinstance(item, dict):
                    status = item.get("status")
                    if status is not None and numeric(status) != 0:
                        continue
                valid = True
                if stamp is None or stamp <= self._watermarks.get(field, 0):
                    continue
                self._watermarks[field] = stamp
                self.last_measurement_at = max(self.last_measurement_at or 0, stamp)
                latest[field] = item
        return valid, latest
