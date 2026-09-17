"""Unit tests for the Flume bridge's parsers, HA Discovery payloads, the
alert-window counter, and the timezone-fallback warning — synthetic
fixtures only, no network access.

Mirrors govee-mqtt-bridge's `test_discovery_regression.py` layout: stub
out `requests`/`paho` (and point at the in-repo toolkit source) so
`main.py` imports cleanly without real credentials or the vendored
toolkit being pip-installed. Run with::

    cd app
    FLUME_CLIENT_ID=x FLUME_CLIENT_SECRET=x FLUME_USERNAME=x \\
        FLUME_PASSWORD=x MQTT_PASSWORD=x python -m pytest test_bridge.py -q
"""

from __future__ import annotations

import logging
import os
import sys
import types
import unittest

_REQUIRED = {
    "FLUME_CLIENT_ID": "test-client",
    "FLUME_CLIENT_SECRET": "test-secret",
    "FLUME_USERNAME": "x@example.com",
    "FLUME_PASSWORD": "test",
    "MQTT_PASSWORD": "test",
}
for k, v in _REQUIRED.items():
    os.environ.setdefault(k, v)


def _stub_module(name: str, **attrs: object) -> None:
    if name in sys.modules:
        return
    mod = types.ModuleType(name)
    for attr_name, attr_val in attrs.items():
        setattr(mod, attr_name, attr_val)
    sys.modules[name] = mod


_stub_module("requests")
sys.modules["requests"].RequestException = Exception  # type: ignore[attr-defined]
sys.modules["requests"].get = lambda *a, **k: None  # type: ignore[attr-defined]
sys.modules["requests"].post = lambda *a, **k: None  # type: ignore[attr-defined]
_stub_module("paho")
_stub_module("paho.mqtt")
mqtt_stub = types.ModuleType("paho.mqtt.client")


class _FakeCallbackAPIVersion:
    VERSION2 = 2


class _FakeMqttClient:
    def __init__(self, *_, **__):
        pass


mqtt_stub.CallbackAPIVersion = _FakeCallbackAPIVersion
mqtt_stub.Client = _FakeMqttClient
sys.modules["paho.mqtt.client"] = mqtt_stub

if "ha_mqtt_bridge" not in sys.modules:
    here = os.path.dirname(os.path.abspath(__file__))
    toolkit = os.path.normpath(
        os.path.join(here, "..", "..", "..", "_shared", "ha-mqtt-bridge-toolkit")
    )
    sys.path.insert(0, toolkit)

import main  # noqa: E402


# -------------------------------------------------------------- parse_devices


class TestParseDevices(unittest.TestCase):
    def test_sensor_and_bridge_split_by_type(self):
        payload = [
            {
                "id": "sensor-1",
                "type": 2,
                "bridge_id": "bridge-1",
                "location": {"name": "House", "tz": "America/Chicago"},
                "last_seen": "2026-09-01T00:00:00.000Z",
                "connected": True,
                "battery_level": "high",
                "oriented": True,
                "product": "flume2",
            },
            {
                "id": "bridge-1",
                "type": 1,
                "last_seen": "2026-09-01T00:00:00.000Z",
                "connected": True,
                "product": "flume2",
            },
        ]
        sensors, bridges = main.parse_devices(payload)
        self.assertIn("sensor-1", sensors)
        self.assertIn("bridge-1", bridges)
        self.assertEqual(sensors["sensor-1"].tz, "America/Chicago")
        self.assertEqual(sensors["sensor-1"].location_name, "House")

    def test_unknown_type_ignored(self):
        payload = [{"id": "x", "type": 99}]
        sensors, bridges = main.parse_devices(payload)
        self.assertEqual(sensors, {})
        self.assertEqual(bridges, {})


# -------------------------------------------------------------- discovery


class TestDiscoverySpecs(unittest.TestCase):
    def setUp(self):
        self.sensor = main.Sensor(
            device_id="sensor-1", bridge_id="bridge-1", name="Flume Bridge: House",
            tz="America/Chicago", last_seen="2026-09-01T00:00:00.000Z",
            connected=True, battery_level="high", oriented=True,
            product="flume2", location_name="House",
        )
        self.bridge = main.Bridge(
            device_id="bridge-1", name="Flume Gateway: idge-1",
            last_seen="2026-09-01T00:00:00.000Z", connected=True, product="flume2",
        )

    def test_leak_event_count_is_measurement_not_total_increasing(self):
        # Regression for the audit finding: `/usage-alerts` is fetched
        # with limit=100, so the leak count within that window can
        # legitimately decrease as older alerts roll off — a real
        # `total_increasing` statistic must never decrease. Same
        # unique_id as before this fix, just an honest state_class/name.
        items = main.discovery_specs_sensor(self.sensor)
        payload = next(p for _, _, p in items if p["unique_id"].endswith("_leak_event_count"))
        self.assertEqual(payload["state_class"], "measurement")
        self.assertNotIn("Lifetime", payload["name"])

    def test_sensor_and_bridge_wrapper_matches_split_functions(self):
        snap = main.Snapshot(sensors={"sensor-1": self.sensor}, bridges={"bridge-1": self.bridge})
        combined = main.discovery_specs(snap)
        split = main.discovery_specs_sensor(self.sensor) + main.discovery_specs_bridge(self.bridge)
        self.assertEqual(
            {p["unique_id"] for _, _, p in combined},
            {p["unique_id"] for _, _, p in split},
        )

    def test_bridge_entities_present(self):
        items = main.discovery_specs_bridge(self.bridge)
        unique_ids = {p["unique_id"] for _, _, p in items}
        self.assertIn("flume_mqtt_bridge_gw_bridge-1_connected", unique_ids)
        self.assertIn("flume_mqtt_bridge_gw_bridge-1_last_seen", unique_ids)


# -------------------------------------------------------------- alert window counter


class TestAbsorbAlerts(unittest.TestCase):
    def test_leak_event_count_reflects_current_window(self):
        snap = main.Snapshot()
        log = logging.getLogger("test")
        # 3 leak alerts in the first (simulated) fetch.
        alerts = [
            {"device_id": "s1", "triggered_datetime": "2026-09-01T00:00:00.000Z", "flume_leak": True},
            {"device_id": "s1", "triggered_datetime": "2026-08-01T00:00:00.000Z", "flume_leak": True},
            {"device_id": "s1", "triggered_datetime": "2026-07-01T00:00:00.000Z", "flume_leak": True},
        ]
        main._absorb_alerts(snap, alerts, log)
        self.assertEqual(snap.alerts["s1"].leak_event_count, 3)

        # A later fetch's window only contains 1 of those (the other 2
        # aged out of the API's limit=100 — this is what makes the
        # counter a `measurement`, not a `total_increasing`, in discovery).
        fewer_alerts = [
            {"device_id": "s1", "triggered_datetime": "2026-09-01T00:00:00.000Z", "flume_leak": True},
        ]
        main._absorb_alerts(snap, fewer_alerts, log)
        self.assertEqual(snap.alerts["s1"].leak_event_count, 1)


# -------------------------------------------------------------- tz fallback warning


class _RecordingLogger:
    def __init__(self):
        self.warnings = []

    def warning(self, msg, *args):
        self.warnings.append(msg % args if args else msg)


class TestParseTzWarning(unittest.TestCase):
    def setUp(self):
        main._WARNED_TZ_NAMES.clear()

    def test_valid_tz_no_warning(self):
        log = _RecordingLogger()
        tz = main._parse_tz("America/Chicago", log)
        self.assertEqual(str(tz), "America/Chicago")
        self.assertEqual(log.warnings, [])

    def test_bad_tz_warns_once(self):
        log = _RecordingLogger()
        main._parse_tz("Not/ARealZone", log)
        main._parse_tz("Not/ARealZone", log)
        main._parse_tz("Not/ARealZone", log)
        self.assertEqual(len(log.warnings), 1, "must warn once per distinct bad name, not every call")

    def test_bad_tz_falls_back_to_utc(self):
        from datetime import timezone
        log = _RecordingLogger()
        tz = main._parse_tz("Not/ARealZone", log)
        self.assertEqual(tz, timezone.utc)

    def test_no_log_no_crash(self):
        # Callers that don't pass a logger (or pass None) must not raise.
        from datetime import timezone
        self.assertEqual(main._parse_tz("Not/ARealZone", None), timezone.utc)


if __name__ == "__main__":
    unittest.main()
