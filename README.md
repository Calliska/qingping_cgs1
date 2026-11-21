# Qingping integration for Home Assistant · Calliska fork

<img src="https://brands.home-assistant.io/qingping_cgs1/dark_icon.png" alt="Qingping CGSx Icon" width="150" align="left" style="margin-right: 20px;">

[![HACS custom repository](https://img.shields.io/badge/HACS-Custom-orange.svg)](https://hacs.dev/) [Releases](https://github.com/Calliska/qingping_cgs1/releases)

This fork of [mash2k3/qingping_cgs1](https://github.com/mash2k3/qingping_cgs1) keeps the Qingping Wi-Fi monitoring integration and adds CGDK2 temperature and humidity sensors relayed by CGDN1 Air Monitor Lite gateways over private MQTT.
<br /><br /><br />

## What this fork adds

- CGDN1 private MQTT gateway support with separate topics, connection status and BLE membership per gateway.
- CGDK2 decoding for temperature, humidity, battery and RSSI. Historical data cannot replace newer measurements.
- Native Home Assistant setup for confirmed BLE discovery, manual entry and reassignment. Hands-free enrollment can be enabled for an individual gateway.
- Temporary MQTT report settings, persistent gateway settings, settings readback and diagnostics with requested and observed report intervals.
- In-place migration of existing configuration entries. Existing entity IDs are preserved, and older BLE assignments are inferred when unambiguous.

This fork and upstream use the same integration domain, `qingping_cgs1`. Install one copy per Home Assistant instance. See the [`1.3.1_beta1` release](https://github.com/Calliska/qingping_cgs1/releases/tag/1.3.1_beta1).

## Requirements

- MQTT integration installed and configured.
- Configure Qingping Wi-Fi devices for private MQTT. CGDK2 sensors have no MQTT settings of their own.
- For HACS installation, add this fork as a custom **Integration** repository.

## Supported Devices

|   | Model | MQTT Format |
|---|-------|-------------|
| <img width="64" height="64" alt="image" src="https://github.com/user-attachments/assets/860613a7-2b3d-4cd6-a77c-a007195277b2" /> | Air Monitor Lite `(CGDN1)` | JSON |
| | Temp & RH Monitor Lite `(CGDK2)` | BLE advertisements and sensor reports via CGDN1 |
| <img width="64" height="74" alt="image" src="https://github.com/user-attachments/assets/8be6d4fc-bf5f-4c2d-b8f4-2c406e70aa88" /> | Air Monitor Pro `(CGS1)` | JSON |
| <img width="64" height="74" alt="image" src="https://github.com/user-attachments/assets/394e61fa-47d8-46e3-a034-041122daf1e4" /> | Air Monitor Pro 2 `(CGS2)` | JSON |
| <img width="64" height="64" alt="image" src="https://github.com/user-attachments/assets/dee1356d-1c6b-4fcc-9640-77772e87b652" /> | Temp & RH Monitor Pro S `(CGP22W)` | TLV Binary |
| <img width="64" height="64" alt="image" src="https://github.com/user-attachments/assets/b7c993cb-8a1f-418b-bc38-387daba02756" /> | Temp & RH Barometer Pro S `(CGP23W)` | TLV Binary |
| <img width="64" height="64" alt="image" src="https://github.com/user-attachments/assets/c23b6cbf-ba75-486b-b221-73cd2b18105d" /> | CO₂ & Temp & RH Monitor `(CGP22C)` | TLV Binary |
| <img width="64" height="64" alt="image" src="https://github.com/user-attachments/assets/8b758081-20b9-49f4-a97e-500cab179291" /> | Indoor Environment Monitor `(CGR1PW, CGR1W)` | TLV Binary |
 
## Features

- Automatic discovery of Qingping devices *
- Real-time updates of air quality data
- Configurable offsets
- Configuration entites
- Adjustable update interval
- Automatic unit conversion for temperature
- Device status monitoring
- Battery level monitoring

<div style="clear: both;"></div>

## Installation

1. Configure MQTT on the Wi-Fi device using [the private MQTT setup guide](enableMQTT.md). The default topics are `qingping/<MAC>/up` and `qingping/<MAC>/down`. Use a unique MQTT client ID for each gateway.
2. In HACS, add `https://github.com/Calliska/qingping_cgs1` as a custom **Integration** repository. If HACS manages the upstream copy, switch the managed repository and keep the existing Qingping entries under **Devices & services**. Back up your HA configuration before upgrading to the beta.
3. Choose release `1.3.1_beta1` in the fork's download version selector. Enable beta versions if your HACS UI filters them out. Restart Home Assistant to apply the update.
4. To install manually, extract the release asset `qingping_cgs1.zip` into `custom_components/qingping_cgs1` in the HA configuration directory. Its `manifest.json` should be directly inside that directory. Restart Home Assistant.

## Configure a CGDN1 gateway

1. Go to **Settings → Devices & services → Add integration → Qingping → Add a CGDN1 gateway**.
2. Select the gateway when detected, or enter its MAC address. If you configured custom MQTT topics, enter their exact case-sensitive names. Set both topics when the reporting topic does not end in `/up`.
3. Open the CGDN1 device. Use **Read gateway settings** to request settings from the gateway and **Synchronize BLE devices** to publish its assigned list. The diagnostics files and device status show MQTT activity and observed timing.
4. For a second gateway, repeat the setup with its own MAC, client ID and topics. Keep separate topic pairs for the two gateways.

An existing Air Monitor Lite saved as `CGS1` can answer gateway list requests if it sends `10` or `25`. Set its model to `CGDN1` in **Configure** to expose all gateway controls and options.

## Add a CGDK2 BLE sensor

CGDK2 broadcasts measurements over BLE. CGDN1 receives the compatible Qingping advertisements and relays them over MQTT. This workflow is passive, so the integration does not open a persistent BLE connection or ask for a pairing code. The gateway firmware controls what it scans and forwards.

- **Discover and confirm.** Keep the sensor with a working battery within range of the gateway. When the gateway relays a valid CGDK2 advertisement, HA shows a discovered Qingping device in **Devices & services**. Select **Configure**, check the name and gateway, and submit.
- **Add automatically.** In that gateway's **Configure** dialog, enable **Automatically add discovered CGDK2 sensors**. While enabled, a valid new advertisement creates a sensor entry assigned to that gateway. Use this when you want that gateway to enroll any compatible CGDK2 it can hear. To add an already pending discovery, confirm it in **Devices & services**.
- **Enter the MAC manually.** If the firmware does not forward a sensor before it is listed, use **Qingping → Add a CGDK2 BLE sensor**. Enter the 12-digit BLE MAC, choose its gateway, and name it. Manual input is available even when other sensors have been discovered. Once the entry is created, the integration sends the gateway an updated list. **Synchronize BLE devices** can retry publication.
- **Move or remove a sensor.** Open its **Configure** dialog and select a different gateway or leave it unassigned. The integration reconciles the lists on both gateways. Removing the entry also removes any matching address from imported legacy lists. If hands-free enrollment is enabled and the same gateway still hears a deleted sensor, it can discover it again. Use HA's discovery ignore action or disable hands-free enrollment to prevent that.

Verify that the CGDK2 page has fresh temperature and humidity and shows its assigned gateway. In MQTT, watch for the CGDK2 MAC in an uplink `type: "9"` report. Some firmware also relays measurements in `12` or `17`. Seeing only the lists `11` and `26` confirms publication, while a fresh reading confirms receipt of usable data. If you see no supported frames even with the MAC listed and the sensor near the gateway, check that the sensor model is CGDK2, check its battery and confirm that your gateway firmware supports relaying this sensor. Some hardware variants advertise encrypted MiBeacon and need a receiver/parser that supports that format.

## MQTT intervals and availability

- **Temporary MQTT Report Interval** on the CGDN1 device sends command `12` and stores the selected value. For example, enter **125 s** and check the gateway status attributes for **requested MQTT interval**, **observed MQTT interval**, and **interval status**. The integration uses successive real-time gateway reports to verify the cadence. Identical HA states may be omitted from history, so use raw MQTT to measure it independently.
- **Configure** on the gateway sets the temporary duration and renewal interval. It also offers the persistent report and measurement settings. Apply persistent settings with the gateway action or explicitly enable application on startup or reconnect. The integration asks for settings readback with `28` on supported firmware.
- The gateway's MQTT interval controls reporting to HA. CGDK2 radio and measurement intervals depend on the sensor firmware. Qingping's public gateway protocol does not specify a CGDK2 command to change those intervals.
- CGDN1 heartbeat, measurement freshness, and BLE sensor availability are tracked independently. A BLE sensor defaults to a **600-second timeout**, configurable from its options. The **Status** sensor lists the reason for an outage and the last accepted packet and measurement. For example, `mqtt_disconnected` points to the broker, `gateway_unavailable` to the configured route and `packet_timeout` to missing valid messages for the device.
- **Download diagnostics** from the integration entry to get redacted reporting details. In the MQTT integration's **Listen to a topic** panel, watch both gateway topics to distinguish a broker or gateway interruption from missing packets for just one MAC. Filter history reports by their measurement timestamps.

### Protocol and migration notes

| MQTT direction | Messages |
| --- | --- |
| Gateway → HA | `9` BLE advertisement, `10` list request, `13` heartbeat, `14` reconnect, `25` named list request |
| HA → gateway | `11` MAC list, `12` temporary report mode, `17` persistent settings, `18` history ACK, `26` named list |
| Gateway → HA reports | `12` real-time data, `17` history, `28` settings response |
| HA → gateway query | `28` settings request |

See the [Qingping JSON MQTT protocol](https://developer.qingping.co/private/communication-protocols/public-mqtt-json). Gateway capabilities and rate limits vary by firmware. This beta is intended for verifying the physical BLE and MQTT link on your installation.

The migration keeps device and entity identifiers. An unambiguous old MAC list or a single gateway can establish the initial sensor assignment. If lists conflict, open the sensor's **Configure** dialog and choose the owner. MACs kept in older explicit lists remain available as candidates even before creating an HA sensor. Removing a gateway leaves its explicitly assigned sensors available for reassignment.

If your CGDN1 still had the default 60-second temporary interval, an interval entered in its legacy **Update Interval** control is carried over to the new control. A separately configured non-default temporary interval takes priority.

### Gateway actions

| Action | Purpose |
| --- | --- |
| `qingping_cgs1.refresh_ble_devices` | Publish the selected gateway's `11` and `26` lists again. |
| `qingping_cgs1.enable_temporary_report` | Send `12` using the configured interval and duration, or values supplied to the action. |
| `qingping_cgs1.enable_diagnostic_report` | Request 10-second MQTT reports for 10 minutes. |
| `qingping_cgs1.configure_gateway_settings` | Publish the persistent settings and request readback. |
| `qingping_cgs1.read_gateway_settings` | Ask the gateway to report its current settings. |

Pass `gateway_mac` to target one gateway. Without it, the actions target all loaded CGDN1 gateways.

## How it Works
<img src="https://github.com/user-attachments/assets/55a42477-59a7-48b6-b70b-f743c5e2a69a" alt="Device Discovery" width="275" align="right">

1. **Device Discovery**: The integration uses MQTT to discover Qingping devices on your network. It listens for messages on the MQTT topic `qingping/#` to identify available devices.

2. **Configuration**: Once a device is discovered, you can add it to your Home Assistant instance through the UI. The integration will prompt you to enter a name and model for the device and confirm its MAC address.

3. **Sensors**: The integration creates several sensors for each Qingping CGS1/CGS2/CGDN1 device:
   - Temperature
   - Humidity
   - CO2 level
   - PM2.5
   - PM10
   - TVOC (ppb, ppm and mg/m³) `Only on CGS1`
   - eTVOC (ppb, VOC index and mg/m³) `Only on CGS2`
   - Noise level `Only on CGS2, CGR1PW, CGR1W`
   - Temp & Humidity Offsets
   - PM2.5 Offsets
   - PM10 Offsets
   - TVOC Offsets `Only on CGS1`
   - eTVOC Offsets `Only on CGS2`
   - Pressure Offset `Only on CGP23W`
   - CO2 Offsets
   - CO₂ Interval `Only on CGP22C`
   - Auto Sliding `Only on CGDN1`
   - Auto CO2 Calibration `Only on CGDN1, CGP22C, CGR1PW, CGR1W`
   - Manual Calibration `Only on CGDN1, CGP22C, CGR1PW, CGR1W`
   - Night Mode `Only on CGDN1`
   - Power Off Time `Only on CGDN1, CGP22C`
   - Screensaver `Only on CGDN1`
   - Timezone `Only on CGDN1`
   - Pressure `Only on CGP23W`
   - Light/Illuminance `Only on CGR1PW, CGR1W`
   - Battery Charging State
   - Battery level
   - Device status (online/offline)
   - Firmware version
   - Report type (realtime /historic) `⚠️ TLV Devices real-time mode only work when device is USB powered`
   - MAC address

4. **TVOC Sensor**: The sensor can be set to 3 different measurement units, by default it is ppb. The component converts from ppb to get ppm and mg/m³.
   - ppm = ppb/1000
   - mg/m³ = ppb/218.77<br />
   
   **eTVOC Sensor**: The sensor can be set to 3 different measurement units, by default it is VOC index. The component converts from voc index to get ppb and mg/m³.
   - ppb = ( math.log ( 501 - voc_index ) - 6.24) * -2215.4
   - mg/m³ = ( ppb * 4.5 * 10 + 5 ) / 10 / 1000
      
5. **Data Updates**: The component subscribes to MQTT messages from the device. When new data is received, it updates the relevant sensors in Home Assistant.
   - ⚠️ Configuration changes **only work when device is USB powered** `Only on TLV Devices`
   - ⚠️ Real-time mode **only work when device is USB powered** `Only on TLV Devices`

7. **Offset Adjustments**: The integration allows you to set offset values for sesor readings. These offsets are applied to the device before it's displayed in Home Assistant.

8. **Reporting**: You can configure how often the device should report new data.
   - **Update Interval**: Use the number entity for JSON devices. On CGDN1 it is named **Temporary MQTT Report Interval**. See [MQTT intervals and availability](#mqtt-intervals-and-availability).
   - **Report Interval**: How often device sends data (10-60 minutes, historic mode only) `Only on TLV Devices`
   - **Sample Interval**: How often device reads sensors (10-300 seconds, historic mode only) `Only on TLV Devices`
   - **Report Mode**: Manual override for real-time/historic mode `Only on TLV Devices`

9. **Configuration Publishing**: The integration periodically publishes configuration messages to the device via MQTT. This ensures that the device maintains the correct reporting interval, realtime reporting and other settings. `⚠️ Note: For battery operating TLV devices, configuration changes only work while device is plugged into USB power`

10. **Status Monitoring for standalone MQTT devices**:
    - Real-time mode: Offline after 5 minutes of no activity. `JSON and TLV Devices`
    - Historic mode: Offline after 15 minutes of no activity. `Only on TLV Devices`

11. **Unit Conversion**:
    - The integration automatically converts temperature readings to the unit system configured in your Home Assistant instance (Celsius or Fahrenheit). `Only on JSON Devices`
    - Temperature Unit: Switch between Celsius (°C) and Fahrenheit (°F) on device page  `Only on TLV Devices`
    -  ⚠️ Time format (12h/24h) cannot be changed via MQTT Yet.  `Only on TLV Devices`
    
13. **Automatic Report Mode Switching**: `Only on TLV Devices`
    - Real-time mode: Automatically enabled when USB powered (fast updates every 3-5 seconds)
    - Historic mode: Automatically enabled when on battery (slower updates, battery friendly)

14. **TLV Protocol Support**: Newer devices use TLV (Type-Length-Value) binary format instead of JSON:
    - Automatic detection of message format
    - Support for historical data buffering (CMD 0x42)
    - Always displays most recent sensor readings
    - Enhanced configuration options via TLV commands

## Troubleshooting

If you encounter any issues:
1. Check that your Qingping device can send data via MQTT
2. Ensure MQTT is set up on each device as instructed
3. Ensure that MQTT is properly set up in your Home Assistant instance
4. Check the Home Assistant logs for any error messages related to this integration

## Contributing

Contributions to this project are welcome! Please feel free to submit a Pull Request.

## Support

If you have any questions or need help, please open an [issue in the Calliska fork](https://github.com/Calliska/qingping_cgs1/issues).

## Acknowledgments

Special thanks to the **Qingping team** for their technical support and for providing sample devices to help improve this integration for the community. Their collaboration has been invaluable in bringing enhanced support for TLV protocol devices and advanced configuration features to Home Assistant users.
