"""Resolve the relationship between configured Qingping gateways and sensors."""

from __future__ import annotations

import json
import time
from collections.abc import Iterable
from typing import Any

from .const import (
    ADV_MODELS,
    BLE_CANDIDATE_TTL,
    CONF_BLE_DEVICES,
    CONF_GATEWAY_MAC,
    CONF_MAC,
    CONF_MODEL,
    CONF_NAME,
    DOMAIN,
)
from .qingping_private import maybe_normalize_mac

BLE_CANDIDATES = "ble_candidates"


def entry_settings(entry: Any) -> dict[str, Any]:
    """Read older option storage alongside current config entry data."""
    return {**entry.data, **entry.options}


def parse_ble_devices(value: Any) -> dict[str, str]:
    """Normalize legacy JSON lists or comma-separated addresses."""
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except json.JSONDecodeError:
            value = value.split(",")
    if not isinstance(value, list):
        return {}
    devices = {}
    for item in value:
        if isinstance(item, str):
            item = {"mac": item}
        if not isinstance(item, dict) or not isinstance(raw_mac := item.get("mac"), str):
            continue
        if not (mac := maybe_normalize_mac(raw_mac.strip())):
            continue
        devices[mac] = str(item.get("name") or f"CGDK2 {mac[-4:]}")
    return devices


def gateway_entries(hass: Any) -> dict[str, Any]:
    """Return enabled configured gateways indexed by their normalized MAC."""
    gateways = {}
    for entry in hass.config_entries.async_entries(DOMAIN):
        if getattr(entry, "disabled_by", None):
            continue
        settings = entry_settings(entry)
        runtime = hass.data.get(DOMAIN, {}).get(entry.entry_id, {})
        if settings.get(CONF_MODEL) != "CGDN1" and not runtime.get("private_handler"):
            continue
        if mac := maybe_normalize_mac(settings.get(CONF_MAC)):
            gateways[mac] = entry
    return gateways


def resolve_gateway(
    settings: dict[str, Any], gateways: dict[str, Any], *, infer_single: bool = True
) -> str | None:
    """Resolve stored ownership or a unique legacy list/single-gateway match."""
    if CONF_GATEWAY_MAC in settings:
        return maybe_normalize_mac(settings[CONF_GATEWAY_MAC])
    mac = maybe_normalize_mac(settings.get(CONF_MAC))
    if not mac:
        return None
    claims = [
        gateway_mac for gateway_mac, gateway in gateways.items()
        if mac in parse_ble_devices(entry_settings(gateway).get(CONF_BLE_DEVICES))
    ]
    if claims:
        return claims[0] if len(claims) == 1 else None
    if infer_single and len(gateways) == 1:
        gateway_mac, gateway = next(iter(gateways.items()))
        if not parse_ble_devices(entry_settings(gateway).get(CONF_BLE_DEVICES)):
            return gateway_mac
    return None


def configured_ble_devices(
    gateway_mac: str, entries: Iterable[Any], gateways: dict[str, Any]
) -> list[dict[str, str]]:
    """Return only active sensors assigned to this gateway."""
    devices = {}
    registered = set()
    for entry in entries:
        settings = entry_settings(entry)
        if settings.get(CONF_MODEL) not in ADV_MODELS or not (mac := maybe_normalize_mac(settings.get(CONF_MAC))):
            continue
        registered.add(mac)
        if getattr(entry, "disabled_by", None) or resolve_gateway(settings, gateways) != gateway_mac:
            continue
        devices[mac] = str(settings.get(CONF_NAME) or f"CGDK2 {mac[-4:]}")
    # Old configurations can contain devices that have no HA entity yet.
    gateway = gateways.get(gateway_mac)
    if gateway:
        for mac, name in parse_ble_devices(entry_settings(gateway).get(CONF_BLE_DEVICES)).items():
            if mac not in registered and resolve_gateway({CONF_MAC: mac}, gateways) == gateway_mac:
                devices[mac] = name
    return [{"mac": mac, "name": name} for mac, name in sorted(devices.items())]


def remember_candidate(hass: Any, gateway_mac: str, mac: str, rssi: int | None) -> None:
    """Remember a verified BLE advertisement with its gateway and last RSSI."""
    candidates = hass.data.setdefault(DOMAIN, {}).setdefault(BLE_CANDIDATES, {})
    now = time.time()
    for key, value in tuple(candidates.items()):
        if now - value["last_seen"] > BLE_CANDIDATE_TTL:
            candidates.pop(key, None)
    candidates[gateway_mac, mac] = {"rssi": rssi, "last_seen": now}
