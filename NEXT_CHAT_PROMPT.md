# Qingping CGDN1 MQTT Handoff

## Short Context

Repo: `c:\Users\klime\Nextcloud\HomeAssistant\HACS\qingping_cgs1`

Home Assistant integration: `custom_components/qingping_cgs1`

Goal: finish/debug Qingping CGDN1 private MQTT gateway support. Gateway MAC is `CCB5D13196DC`. It was discovered as `CGS1`, but MQTT logs show it behaves as `CGDN1` and sends private protocol messages such as `type=10` and `type=13`.

Expected behavior: when the gateway sends `type=10` or `type=25`, the integration should reply with the configured BLE device list: 4 `CGDK2` devices.

Known BLE MACs:

```json
[
  {"mac": "582D34885C15", "name": "CGDK2 5C15"},
  {"mac": "582D34885C16", "name": "CGDK2 5C16"},
  {"mac": "582D34885772", "name": "CGDK2 5772"},
  {"mac": "582D34885BF0", "name": "CGDK2 5BF0"}
]
```

## What The Logs Showed

- Gateway `CCB5D13196DC` was configured/discovered as `CGS1`.
- It sent `type=10` on `qingping/CCB5D13196DC/up`.
- After adding fallback logic, a `type=11` response appeared, but later `dev_list` became empty.
- Root cause: `CGDK2` entries subscribe to `qingping/#`, so they also received gateway service messages from `qingping/CCB5D13196DC/up`.
- The previous fallback treated those messages as if each `CGDK2` entry was a `CGDN1` gateway, creating duplicate private handlers and repeated responses.

Bad log examples:

```text
Device 582D... is configured as CGDK2 but sent CGDN1 gateway request type 10; enabling private MQTT handler
Processing CGDN1 service message type 10 from topic qingping/CCB5D13196DC/up even though entry MAC is 582D...
Publishing CGDN1 private payload ... type=11
Publishing CGDN1 private payload ... type=26
Publishing CGDN1 private payload ... type=12
```

Unrelated log issues:

- `HACS ... TimeoutError` is unrelated to Qingping; HACS timed out while fetching `data.json`.
- `Something is blocking Home Assistant...` points to pending HACS startup task.
- `Received unknown command: brands/access_token` looks unrelated/non-critical.

## Changes Already Made

### `custom_components/qingping_cgs1/sensor.py`

- Added private handler fallback for non-ADV entries receiving CGDN1 gateway request messages `type=10` or `type=25`.
- Important guard: fallback now only triggers if the MAC in the MQTT topic matches the current config entry MAC.
- This should prevent `CGDK2` entries from creating private handlers when they see gateway messages through `qingping/#`.
- For `CGDN1` and `CGDK2`, subscriptions use `qingping/#`; other devices subscribe to up-topic variants for their own MAC.

### `custom_components/qingping_cgs1/private_mqtt.py`

- Added `QingpingPrivateGatewayHandler`.
- Handles:
  - `type=10` -> reply `type=11` with `dev_list`
  - `type=25` -> reply `type=26` with named device list
  - `type=13` heartbeat
  - `type=14` reconnect
  - sensor data ACK -> `type=18`
  - temporary report command -> `type=12`
- Replies prefer the observed down-topic. Example: incoming topic `qingping/CCB5D13196DC/up` replies to `qingping/CCB5D13196DC/down`.
- Added warning if BLE list is empty. It logs model counts and suggests checking `ble_devices` or ensuring BLE entries are model `CGDK2`.

### `custom_components/qingping_cgs1/qingping_private.py`

- Added helper functions:
  - `normalize_mac`
  - `maybe_normalize_mac`
  - `topic_mac_variants`
  - `up_topic_variants`
  - `down_topic_variants`
  - payload builders for `type=11`, `type=26`, `type=12`, `type=18`, diagnostics/settings.
- `type=11` and `type=26` preserve request `id` if present.

### `custom_components/qingping_cgs1/__init__.py`

- Private MQTT services are registered under domain `qingping_cgs1`, not `qingping_private`, because HA reported: `Integration 'qingping_private' not found`.

## Checks Already Run

- `git diff --check` passed with no whitespace errors.
- Git only printed CRLF/LF warnings.
- Local Python is unavailable, so tests were not run.

## What To Check After Restarting HA

Expected good log:

```text
Device CCB5D13196DC is configured as CGS1 but sent CGDN1 gateway request type 10; enabling private MQTT handler
CGDN1 requested BLE device list on qingping/CCB5D13196DC/up; replying with 4 devices to ['qingping/CCB5D13196DC/down']
Publishing CGDN1 private payload to qingping/CCB5D13196DC/down: type=11
```

Should NOT appear anymore:

```text
Device 582D... is configured as CGDK2 but sent CGDN1 gateway request type 10; enabling private MQTT handler
Processing CGDN1 service message type 10 from topic qingping/CCB5D13196DC/up even though entry MAC is 582D...
```

If `replying with 0 devices` still appears:

- Check HA UI: all 4 BLE entries must have model `CGDK2`.
- Gateway `CCB5D13196DC` should be model `CGDN1`, or at least send `type=10` so fallback updates it.
- If a previous buggy run saved BLE entries as `CGDN1`, change them back to `CGDK2`.
- As a workaround, configure explicit `ble_devices` in options using the JSON list above.

## Prompt For A New Chat

```text
We are working in this Home Assistant custom integration repo:
c:\Users\klime\Nextcloud\HomeAssistant\HACS\qingping_cgs1

Problem:
Qingping CGDN1 gateway with MAC CCB5D13196DC was discovered as CGS1, but MQTT logs show it sends CGDN1 private protocol messages type=10/type=13 on qingping/CCB5D13196DC/up. The integration must reply to type=10/type=25 with a list of 4 CGDK2 BLE devices and must not create duplicate handlers on CGDK2 entries.

Important logs:
- Gateway sends type=10 on qingping/CCB5D13196DC/up.
- A type=11 response appeared, but dev_list became empty.
- Root cause: CGDK2 entries subscribe to qingping/# and also received gateway service messages. Old fallback enabled private MQTT handler on those CGDK2 entries.
- Bad log pattern:
  Device 582D... is configured as CGDK2 but sent CGDN1 gateway request type 10; enabling private MQTT handler
  Processing CGDN1 service message type 10 from topic qingping/CCB5D13196DC/up even though entry MAC is 582D...

Already changed:
- custom_components/qingping_cgs1/sensor.py now only enables CGDN1 private handler if the MAC in the MQTT topic matches the current config entry MAC.
- custom_components/qingping_cgs1/private_mqtt.py has QingpingPrivateGatewayHandler:
  type=10 -> type=11 dev_list,
  type=25 -> type=26 named list,
  type=13 heartbeat,
  type=14 reconnect,
  ACK -> type=18,
  temporary report -> type=12.
- custom_components/qingping_cgs1/qingping_private.py has MAC normalization, topic variants, and payload builders.
- Services are registered under qingping_cgs1, not qingping_private.

Known BLE devices:
[
  {"mac": "582D34885C15", "name": "CGDK2 5C15"},
  {"mac": "582D34885C16", "name": "CGDK2 5C16"},
  {"mac": "582D34885772", "name": "CGDK2 5772"},
  {"mac": "582D34885BF0", "name": "CGDK2 5BF0"}
]

Next steps:
1. Review current diff and topic matching.
2. If HA restart still logs replying with 0 devices, debug get_configured_ble_devices().
3. Make sure it sees 4 entries with model CGDK2 or uses explicit ble_devices.
4. Expected good log after restart:
   CGDN1 requested BLE device list on qingping/CCB5D13196DC/up; replying with 4 devices to ['qingping/CCB5D13196DC/down']
5. Ensure no warnings remain for 582D... configured as CGDK2 but sent CGDN1 gateway request type 10.

Environment notes:
- Use apply_patch for edits.
- Local Python is unavailable, so tests may not run.
- git diff --check previously passed except CRLF/LF warnings.
```
