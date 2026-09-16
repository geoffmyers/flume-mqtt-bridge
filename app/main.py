"""
Flume Water Cloud → MQTT bridge.

Polls the Flume v2 REST API (https://api.flumewater.com) on three cadences:

  - FAST (default 60s):  per-sensor current GPM from /devices/{id}/query/active
  - SLOW (default 300s): day / month / year totals via /devices/{id}/query
                         + per-device telemetry (battery, connected,
                         last_seen, oriented) via /devices?user=true
  - ALERT (default 1800s): /usage-alerts (leak history) + /notifications

Publishes Home Assistant MQTT discovery configs and per-poll state
updates under `flume/<sensor_id>/...`.

The bridge intentionally creates a SEPARATE HA device ("Flume Bridge:
<location>") from the official `flume` HA integration so the two can
coexist while we evaluate. Entity unique_ids are namespaced
`flume_bridge_<sensor_id>_*` and will not collide.

Per-minute raw flow data (1-min resolution = Flume API ceiling) is
emitted on `flume/<sensor_id>/flow/min` as JSON {datetime, gallons} —
intended for Telegraf/InfluxDB consumers, not HA entities.

API surface live-probed 2026-05-22.
"""

from __future__ import annotations

import json
import logging
import os
import re
import signal
import sys
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone

import requests
from ha_mqtt_bridge import (
    ThreadedPublisher,
    availability_block,
    build_device_block,
    build_discovery_payload,
    configure_logging,
    epoch_to_iso,
    iso_now,
    now_ms,
    register_github_error_reporter,
)


# --- production-error reporter ---------------------------------------
# Installs sys.excepthook + threading.excepthook so every uncaught
# exception flows through GitHub repository_dispatch → the Production
# Error Intake workflow → Claude auto-fix PR. Silently disabled when
# GITHUB_ERROR_TOKEN is unset (e.g. local dev).
register_github_error_reporter("flume-mqtt-bridge")
# ---------------------------------------------------------------------
FLUME_BASE = "https://api.flumewater.com"

CLIENT_ID = os.environ["FLUME_CLIENT_ID"]
CLIENT_SECRET = os.environ["FLUME_CLIENT_SECRET"]
USERNAME = os.environ["FLUME_USERNAME"]
PASSWORD = os.environ["FLUME_PASSWORD"]

MQTT_HOST = os.environ.get("MQTT_HOST", "mosquitto")
MQTT_PORT = int(os.environ.get("MQTT_PORT", "1883"))
MQTT_USER = os.environ.get("MQTT_USER", "")
MQTT_PASS = os.environ["MQTT_PASSWORD"]

FAST_POLL = int(os.environ.get("FAST_POLL_INTERVAL", "60"))
SLOW_POLL = int(os.environ.get("SLOW_POLL_INTERVAL", "300"))
ALERT_POLL = int(os.environ.get("ALERT_POLL_INTERVAL", "1800"))
LEAK_HOLD_SECONDS = int(os.environ.get("LEAK_HOLD_SECONDS", "1800"))

DISCOVERY_PREFIX = os.environ.get("HA_DISCOVERY_PREFIX", "homeassistant")
TOPIC_PREFIX = os.environ.get("MQTT_TOPIC_PREFIX", "flume")
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()

BRIDGE_LWT_TOPIC = f"{TOPIC_PREFIX}/bridge/online"

# Battery enum → numeric percent (Flume only returns low/medium/high).
BATTERY_PCT = {"low": 25, "medium": 50, "high": 100}


def _humanize_enum(value):
    """Lowercase / UPPER_SNAKE → Title Case. None / empty / period-bearing
    strings (versions, timestamps) pass through unchanged."""
    if value is None or value == "" or not isinstance(value, str):
        return value
    if "." in value:
        return value
    return value.replace("_", " ").lower().title()


@dataclass
class Sensor:
    device_id: str
    bridge_id: str | None
    name: str
    tz: str
    last_seen: str | None          # ISO-8601 from Flume (UTC)
    connected: bool
    battery_level: str | None      # "low" | "medium" | "high"
    oriented: bool
    product: str
    location_name: str


@dataclass
class Bridge:
    device_id: str
    name: str
    last_seen: str | None
    connected: bool
    product: str


@dataclass
class FlowSample:
    current_gpm: float
    active: bool
    sample_at_iso: str             # local-tz datetime string from API


@dataclass
class PeriodTotals:
    today: float = 0.0
    this_month: float = 0.0
    this_year: float = 0.0


@dataclass
class AlertState:
    last_leak_event_iso: str | None = None
    leak_event_count: int = 0
    leak_until_ms: int = 0          # virtual leak-active hold window
    last_notification_iso: str | None = None
    notifications_24h: int = 0


@dataclass
class Snapshot:
    sensors: dict[str, Sensor] = field(default_factory=dict)
    bridges: dict[str, Bridge] = field(default_factory=dict)
    flow: dict[str, FlowSample] = field(default_factory=dict)
    period: dict[str, PeriodTotals] = field(default_factory=dict)
    alerts: dict[str, AlertState] = field(default_factory=dict)
    user_id: int | None = None


# -------------------------------------------------------------- API client


def _post(url: str, *, headers: dict | None = None, json_body: dict | None = None) -> dict:
    r = requests.post(url, headers=headers or {}, json=json_body, timeout=20)
    if r.status_code == 401:
        raise PermissionError(f"401 from {url}")
    r.raise_for_status()
    return r.json()


def _get(url: str, *, headers: dict | None = None) -> dict:
    r = requests.get(url, headers=headers or {}, timeout=20)
    if r.status_code == 401:
        raise PermissionError(f"401 from {url}")
    r.raise_for_status()
    return r.json()


def oauth_token() -> tuple[str, str, int]:
    """Returns (access_token, refresh_token, expires_at_ms)."""
    data = _post(
        f"{FLUME_BASE}/oauth/token",
        json_body={
            "grant_type": "password",
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET,
            "username": USERNAME,
            "password": PASSWORD,
        },
    )
    block = (data.get("data") or [None])[0]
    if not block or not block.get("access_token"):
        raise RuntimeError(f"oauth response missing access_token: {data}")
    access = block["access_token"]
    refresh = block.get("refresh_token") or ""
    expires_at = now_ms() + int(block.get("expires_in", 604800)) * 1000
    return access, refresh, expires_at


def refresh_token(refresh: str) -> tuple[str, str, int]:
    """Refresh path — Flume's refresh tokens are single-use."""
    data = _post(
        f"{FLUME_BASE}/oauth/token",
        json_body={
            "grant_type": "refresh_token",
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET,
            "refresh_token": refresh,
        },
    )
    block = (data.get("data") or [None])[0]
    if not block or not block.get("access_token"):
        raise RuntimeError(f"refresh response missing access_token: {data}")
    return (
        block["access_token"],
        block.get("refresh_token") or refresh,
        now_ms() + int(block.get("expires_in", 604800)) * 1000,
    )


def extract_user_id(token: str) -> int:
    import base64

    payload_b64 = token.split(".")[1]
    pad = "=" * (-len(payload_b64) % 4)
    payload = json.loads(base64.urlsafe_b64decode(payload_b64 + pad).decode())
    return int(payload["user_id"])


def fetch_devices(user_id: int, token: str) -> list[dict]:
    data = _get(
        f"{FLUME_BASE}/users/{user_id}/devices?user=true&location=true",
        headers={"Authorization": f"Bearer {token}"},
    )
    return data.get("data") or []


def fetch_query_active(user_id: int, device_id: str, token: str) -> dict | None:
    data = _get(
        f"{FLUME_BASE}/users/{user_id}/devices/{device_id}/query/active",
        headers={"Authorization": f"Bearer {token}"},
    )
    items = data.get("data") or []
    return items[0] if items else None


def fetch_period_totals(user_id: int, device_id: str, sensor_tz: str, token: str) -> PeriodTotals:
    """Day / Month / Year totals — one query call returns all three."""
    # Compute local-tz datetimes for the request. We use the sensor's tz so
    # "today" lines up with the customer's calendar day.
    try:
        tz = _parse_tz(sensor_tz)
    except Exception:
        tz = timezone.utc
    local_now = datetime.now(timezone.utc).astimezone(tz)
    start_of_today = local_now.replace(hour=0, minute=0, second=0, microsecond=0)
    start_of_month = start_of_today.replace(day=1)
    start_of_year = start_of_today.replace(month=1, day=1)

    fmt = "%Y-%m-%d %H:%M:%S"
    body = {
        "queries": [
            {
                "request_id": "today",
                "bucket": "DAY",
                "since_datetime": start_of_today.strftime(fmt),
                "until_datetime": local_now.strftime(fmt),
                "units": "GALLONS",
            },
            {
                "request_id": "month",
                "bucket": "MON",
                "since_datetime": start_of_month.strftime(fmt),
                "until_datetime": local_now.strftime(fmt),
                "units": "GALLONS",
            },
            {
                "request_id": "year",
                "bucket": "YR",
                "since_datetime": start_of_year.strftime(fmt),
                "until_datetime": local_now.strftime(fmt),
                "units": "GALLONS",
            },
        ]
    }
    data = _post(
        f"{FLUME_BASE}/users/{user_id}/devices/{device_id}/query",
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        json_body=body,
    )
    rows = (data.get("data") or [{}])[0]
    return PeriodTotals(
        today=_sum_rows(rows.get("today")),
        this_month=_sum_rows(rows.get("month")),
        this_year=_sum_rows(rows.get("year")),
    )


def fetch_last_minute_flow(user_id: int, device_id: str, sensor_tz: str, token: str) -> dict | None:
    """Return the most recent fully-elapsed 1-min flow row, or None."""
    try:
        tz = _parse_tz(sensor_tz)
    except Exception:
        tz = timezone.utc
    local_now = datetime.now(timezone.utc).astimezone(tz)
    since = (local_now - timedelta(minutes=3)).strftime("%Y-%m-%d %H:%M:%S")
    until = local_now.strftime("%Y-%m-%d %H:%M:%S")
    data = _post(
        f"{FLUME_BASE}/users/{user_id}/devices/{device_id}/query",
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        json_body={
            "queries": [
                {
                    "request_id": "min",
                    "bucket": "MIN",
                    "since_datetime": since,
                    "until_datetime": until,
                    "units": "GALLONS",
                }
            ]
        },
    )
    rows = (data.get("data") or [{}])[0].get("min") or []
    return rows[-1] if rows else None


def fetch_usage_alerts(user_id: int, token: str, limit: int = 50) -> list[dict]:
    data = _get(
        f"{FLUME_BASE}/users/{user_id}/usage-alerts?limit={limit}",
        headers={"Authorization": f"Bearer {token}"},
    )
    return data.get("data") or []


def fetch_notifications(user_id: int, token: str, limit: int = 50) -> list[dict]:
    data = _get(
        f"{FLUME_BASE}/users/{user_id}/notifications?limit={limit}",
        headers={"Authorization": f"Bearer {token}"},
    )
    return data.get("data") or []


# -------------------------------------------------------------- helpers


def _parse_tz(tz_name: str) -> timezone:
    """Parse Flume's tz field. Returns UTC if anything goes wrong.

    Flume stores `tz` as an IANA name like "America/New_York". Python's stdlib
    zoneinfo handles those directly; we fall back to UTC if zoneinfo isn't
    available or the name doesn't resolve.
    """
    try:
        from zoneinfo import ZoneInfo

        return ZoneInfo(tz_name)  # type: ignore[return-value]
    except Exception:
        return timezone.utc


def _sum_rows(rows: list[dict] | None) -> float:
    if not rows:
        return 0.0
    return float(sum(r.get("value", 0) or 0 for r in rows))


def _alert_ts_ms(alert: dict) -> int:
    """Pull the ISO timestamp from a Flume usage-alert and return epoch ms.

    Falls back to 0 if missing or malformed.
    """
    raw = alert.get("triggered_datetime") or ""
    if not raw:
        return 0
    try:
        # Flume uses "2024-03-27T00:13:00.000Z" — strip the Z and parse.
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        return int(dt.timestamp() * 1000)
    except Exception:
        return 0


def _notif_ts_ms(notif: dict) -> int:
    raw = notif.get("created_datetime") or ""
    if not raw:
        return 0
    try:
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        return int(dt.timestamp() * 1000)
    except Exception:
        return 0


def parse_devices(payload: list[dict]) -> tuple[dict[str, Sensor], dict[str, Bridge]]:
    sensors: dict[str, Sensor] = {}
    bridges: dict[str, Bridge] = {}
    for d in payload:
        # type 2 = sensor (flume2); type 1 = bridge
        if d.get("type") == 2:
            loc = d.get("location") or {}
            sensors[d["id"]] = Sensor(
                device_id=d["id"],
                bridge_id=d.get("bridge_id"),
                name=f"Flume Bridge: {loc.get('name') or 'House'}",
                tz=loc.get("tz") or "UTC",
                last_seen=d.get("last_seen"),
                connected=bool(d.get("connected")),
                battery_level=d.get("battery_level"),
                oriented=bool(d.get("oriented")),
                product=d.get("product") or "flume2",
                location_name=loc.get("name") or "House",
            )
        elif d.get("type") == 1:
            bridges[d["id"]] = Bridge(
                device_id=d["id"],
                name=f"Flume Gateway: {d['id'][-6:]}",
                last_seen=d.get("last_seen"),
                connected=bool(d.get("connected")),
                product=d.get("product") or "flume2",
            )
    return sensors, bridges


# -------------------------------------------------------------- HA Discovery


def _sensor_device_block(s: Sensor) -> dict:
    return build_device_block(
        identifiers=[f"flume_mqtt_bridge_{s.device_id}"],
        name=s.name,
        manufacturer="Flume",
        model="Flume 2",
    )


def _bridge_device_block(b: Bridge) -> dict:
    return build_device_block(
        identifiers=[f"flume_mqtt_bridge_gw_{b.device_id}"],
        name=b.name,
        manufacturer="Flume",
        model="Flume 2 Bridge",
    )


def discovery_specs(snap: Snapshot) -> list[tuple[str, str, dict]]:
    """Return (component, unique_id, payload) triples for HA MQTT discovery."""
    items: list[tuple[str, str, dict]] = []
    avail = availability_block(BRIDGE_LWT_TOPIC)

    for s in snap.sensors.values():
        dev_uid = f"flume_mqtt_bridge_{s.device_id}"
        device_block = _sensor_device_block(s)
        sensor_topic_base = f"{TOPIC_PREFIX}/{s.device_id}"

        sensor_entities = [
            (
                "sensor", "current_gpm", "Current Flow",
                {
                    "device_class": "volume_flow_rate",
                    "unit_of_measurement": "gal/min",
                    "state_class": "measurement",
                    "icon": "mdi:water-pump",
                },
            ),
            (
                "binary_sensor", "active", "Flow Active",
                {
                    "device_class": "moving",
                    "payload_on": "ON",
                    "payload_off": "OFF",
                    "icon": "mdi:water",
                },
            ),
            (
                "sensor", "gallons_today", "Gallons Today",
                {
                    "device_class": "water",
                    "unit_of_measurement": "gal",
                    "state_class": "total_increasing",
                    "icon": "mdi:counter",
                },
            ),
            (
                "sensor", "gallons_month", "Gallons This Month",
                {
                    "device_class": "water",
                    "unit_of_measurement": "gal",
                    "state_class": "total_increasing",
                    "icon": "mdi:calendar-month",
                },
            ),
            (
                "sensor", "gallons_year", "Gallons This Year",
                {
                    "device_class": "water",
                    "unit_of_measurement": "gal",
                    "state_class": "total_increasing",
                    "icon": "mdi:calendar",
                },
            ),
            (
                "sensor", "battery", "Battery",
                {
                    "device_class": "battery",
                    "unit_of_measurement": "%",
                    "state_class": "measurement",
                    "entity_category": "diagnostic",
                },
            ),
            (
                "sensor", "battery_level", "Battery Level",
                {
                    "icon": "mdi:battery",
                    "entity_category": "diagnostic",
                },
            ),
            (
                "binary_sensor", "connected", "Connected",
                {
                    "device_class": "connectivity",
                    "payload_on": "ON",
                    "payload_off": "OFF",
                    "entity_category": "diagnostic",
                },
            ),
            (
                "binary_sensor", "oriented", "Mounted Correctly",
                {
                    "payload_on": "ON",
                    "payload_off": "OFF",
                    "icon": "mdi:gauge",
                    "entity_category": "diagnostic",
                },
            ),
            (
                "sensor", "last_seen", "Last Seen",
                {
                    "device_class": "timestamp",
                    "icon": "mdi:clock-outline",
                    "entity_category": "diagnostic",
                },
            ),
            (
                "binary_sensor", "smart_leak_active", "Smart Leak Active",
                {
                    "device_class": "moisture",
                    "payload_on": "ON",
                    "payload_off": "OFF",
                    "icon": "mdi:water-alert",
                },
            ),
            (
                "sensor", "last_leak_event", "Last Smart-Leak Event",
                {
                    "device_class": "timestamp",
                    "icon": "mdi:water-alert-outline",
                },
            ),
            (
                "sensor", "leak_event_count", "Smart-Leak Events Lifetime",
                {
                    "state_class": "total_increasing",
                    "icon": "mdi:counter",
                    "entity_category": "diagnostic",
                },
            ),
            (
                "sensor", "last_notification", "Last Notification",
                {
                    "device_class": "timestamp",
                    "icon": "mdi:bell-outline",
                    "entity_category": "diagnostic",
                },
            ),
            (
                "sensor", "notifications_24h", "Notifications 24h",
                {
                    "state_class": "measurement",
                    "icon": "mdi:bell-ring",
                    "entity_category": "diagnostic",
                },
            ),
        ]
        for component, slug, name, extras in sensor_entities:
            uid = f"{dev_uid}_{slug}"
            items.append(
                (
                    component,
                    f"{dev_uid}/{slug}",
                    build_discovery_payload(
                        name=name,
                        unique_id=uid,
                        object_id=uid,
                        state_topic=f"{sensor_topic_base}/{slug}",
                        device=device_block,
                        **avail,
                        **extras,
                    ),
                )
            )

    for b in snap.bridges.values():
        bridge_uid = f"flume_mqtt_bridge_gw_{b.device_id}"
        device_block = _bridge_device_block(b)
        bridge_topic_base = f"{TOPIC_PREFIX}/{b.device_id}"
        bridge_entities = [
            (
                "binary_sensor", "connected", "Connected",
                {
                    "device_class": "connectivity",
                    "payload_on": "ON",
                    "payload_off": "OFF",
                    "entity_category": "diagnostic",
                },
            ),
            (
                "sensor", "last_seen", "Last Seen",
                {
                    "device_class": "timestamp",
                    "icon": "mdi:clock-outline",
                    "entity_category": "diagnostic",
                },
            ),
        ]
        for component, slug, name, extras in bridge_entities:
            uid = f"{bridge_uid}_{slug}"
            items.append(
                (
                    component,
                    f"{bridge_uid}/{slug}",
                    build_discovery_payload(
                        name=name,
                        unique_id=uid,
                        object_id=uid,
                        state_topic=f"{bridge_topic_base}/{slug}",
                        device=device_block,
                        **avail,
                        **extras,
                    ),
                )
            )

    return items


# -------------------------------------------------------------- publish helpers


def publish_device_telemetry(pub: ThreadedPublisher, snap: Snapshot) -> None:
    """Battery / connected / last_seen / oriented per sensor + bridge."""
    for s in snap.sensors.values():
        base = f"{TOPIC_PREFIX}/{s.device_id}"
        pub.publish_state(f"{base}/connected", "ON" if s.connected else "OFF")
        pub.publish_state(f"{base}/oriented", "ON" if s.oriented else "OFF")
        if s.last_seen:
            pub.publish_state(f"{base}/last_seen", s.last_seen)
        if s.battery_level:
            pub.publish_state(f"{base}/battery_level", _humanize_enum(s.battery_level))
            pct = BATTERY_PCT.get(s.battery_level)
            if pct is not None:
                pub.publish_state(f"{base}/battery", str(pct))
    for b in snap.bridges.values():
        base = f"{TOPIC_PREFIX}/{b.device_id}"
        pub.publish_state(f"{base}/connected", "ON" if b.connected else "OFF")
        if b.last_seen:
            pub.publish_state(f"{base}/last_seen", b.last_seen)


def publish_flow(pub: ThreadedPublisher, snap: Snapshot) -> None:
    """Per-sensor current_gpm + active flow state."""
    for sensor_id, flow in snap.flow.items():
        base = f"{TOPIC_PREFIX}/{sensor_id}"
        pub.publish_state(f"{base}/current_gpm", f"{flow.current_gpm:.4f}")
        pub.publish_state(f"{base}/active", "ON" if flow.active else "OFF")


def publish_period_totals(pub: ThreadedPublisher, snap: Snapshot) -> None:
    for sensor_id, period in snap.period.items():
        base = f"{TOPIC_PREFIX}/{sensor_id}"
        pub.publish_state(f"{base}/gallons_today", f"{period.today:.2f}")
        pub.publish_state(f"{base}/gallons_month", f"{period.this_month:.1f}")
        pub.publish_state(f"{base}/gallons_year", f"{period.this_year:.1f}")


def publish_alerts(pub: ThreadedPublisher, snap: Snapshot) -> None:
    now = now_ms()
    for sensor_id, alerts in snap.alerts.items():
        base = f"{TOPIC_PREFIX}/{sensor_id}"
        pub.publish_state(
            f"{base}/smart_leak_active",
            "ON" if alerts.leak_until_ms > now else "OFF",
        )
        if alerts.last_leak_event_iso:
            pub.publish_state(f"{base}/last_leak_event", alerts.last_leak_event_iso)
        pub.publish_state(f"{base}/leak_event_count", str(alerts.leak_event_count))
        if alerts.last_notification_iso:
            pub.publish_state(f"{base}/last_notification", alerts.last_notification_iso)
        pub.publish_state(f"{base}/notifications_24h", str(alerts.notifications_24h))


def publish_minute_flow(pub: ThreadedPublisher, sensor_id: str, row: dict | None) -> None:
    """Raw 1-min flow sample on a side-channel topic for Telegraf/InfluxDB.

    `row` is the Flume MIN-bucket row: {datetime, value}. Topic is NOT a HA
    entity — it carries JSON so a Telegraf MQTT consumer can ingest it.
    """
    if not row:
        return
    payload = {
        "datetime": row.get("datetime"),
        "gallons": float(row.get("value") or 0),
        "published_at": iso_now(),
    }
    pub.publish_state(f"{TOPIC_PREFIX}/{sensor_id}/flow/min", json.dumps(payload))


# -------------------------------------------------------------- main loop


def _absorb_alerts(snap: Snapshot, alerts_resp: list[dict], log: logging.Logger) -> None:
    """Update snap.alerts from a /usage-alerts response.

    Each alert has `device_id`, `triggered_datetime`, `flume_leak`, `query`.
    We track per-sensor: most recent event ISO, lifetime count, and a
    leak-active hold window if the most recent event is fresh.
    """
    now = now_ms()
    per_sensor: dict[str, list[dict]] = {}
    for a in alerts_resp:
        sid = a.get("device_id")
        if not sid:
            continue
        per_sensor.setdefault(sid, []).append(a)
    for sid, items in per_sensor.items():
        # Sort newest first
        items.sort(key=_alert_ts_ms, reverse=True)
        leak_items = [x for x in items if x.get("flume_leak")]
        state = snap.alerts.setdefault(sid, AlertState())
        state.leak_event_count = len(leak_items)
        if leak_items:
            newest = leak_items[0]
            iso = newest.get("triggered_datetime") or ""
            ts_ms = _alert_ts_ms(newest)
            if ts_ms:
                state.last_leak_event_iso = iso
                # Hold the leak ON if the event is within LEAK_HOLD_SECONDS.
                if (now - ts_ms) // 1000 < LEAK_HOLD_SECONDS:
                    state.leak_until_ms = ts_ms + LEAK_HOLD_SECONDS * 1000
                    log.warning("active smart-leak alert %s @ %s", sid, iso)


def _absorb_notifications(snap: Snapshot, notif_resp: list[dict]) -> None:
    now = now_ms()
    per_sensor: dict[str, list[dict]] = {}
    for n in notif_resp:
        sid = n.get("device_id")
        if not sid:
            continue
        per_sensor.setdefault(sid, []).append(n)
    cutoff_ms = now - 24 * 3600 * 1000
    for sid, items in per_sensor.items():
        items.sort(key=_notif_ts_ms, reverse=True)
        state = snap.alerts.setdefault(sid, AlertState())
        if items:
            newest_ts = _notif_ts_ms(items[0])
            if newest_ts:
                state.last_notification_iso = items[0].get("created_datetime")
        state.notifications_24h = sum(1 for x in items if _notif_ts_ms(x) >= cutoff_ms)


def _refresh_or_login(refresh: str | None, log: logging.Logger) -> tuple[str, str, int]:
    if refresh:
        try:
            return refresh_token(refresh)
        except Exception as e:
            log.warning("refresh failed (%s); falling back to password grant", e)
    return oauth_token()


def main() -> int:
    log = configure_logging("flume-mqtt-bridge", LOG_LEVEL)
    log.info(
        "starting; fast=%ss slow=%ss alert=%ss leak_hold=%ss",
        FAST_POLL, SLOW_POLL, ALERT_POLL, LEAK_HOLD_SECONDS,
    )

    access, refresh, token_expires_at = oauth_token()
    user_id = extract_user_id(access)
    log.info("login ok; user_id=%s token expires in %ds",
             user_id, (token_expires_at - now_ms()) // 1000)

    pub = ThreadedPublisher(
        host=MQTT_HOST,
        port=MQTT_PORT,
        username=MQTT_USER,
        password=MQTT_PASS,
        client_id=f"flume-mqtt-bridge-{uuid.uuid4().hex[:8]}",
        lwt_topic=BRIDGE_LWT_TOPIC,
        discovery_prefix=DISCOVERY_PREFIX,
        health_path="/tmp/healthy",
    )
    pub.start()

    snap = Snapshot(user_id=user_id)
    discovery_published = False
    last_slow_at = 0.0
    last_alert_at = 0.0

    stopping = False

    def on_signal(signum, _frame):
        nonlocal stopping
        log.info("signal %s, shutting down", signum)
        stopping = True

    signal.signal(signal.SIGTERM, on_signal)
    signal.signal(signal.SIGINT, on_signal)

    while not stopping:
        try:
            # Refresh token a minute before expiry
            if now_ms() > token_expires_at - 60_000:
                access, refresh, token_expires_at = _refresh_or_login(refresh, log)
                log.info("token refreshed; expires in %ds",
                         (token_expires_at - now_ms()) // 1000)

            # ---- Fast poll: device telemetry + current GPM + minute flow ----
            devices_payload = fetch_devices(user_id, access)
            sensors, bridges = parse_devices(devices_payload)
            snap.sensors = sensors
            snap.bridges = bridges

            if not discovery_published:
                if not sensors:
                    log.warning("no Flume sensors found in account")
                for component, unique_id, payload in discovery_specs(snap):
                    pub.publish_discovery(
                        component=component, unique_id=unique_id, payload=payload,
                    )
                discovery_published = True
                log.info("discovery published: %d sensor(s), %d bridge(s)",
                         len(sensors), len(bridges))

            for sid, s in sensors.items():
                try:
                    active = fetch_query_active(user_id, sid, access)
                    if active is not None:
                        snap.flow[sid] = FlowSample(
                            current_gpm=float(active.get("gpm") or 0),
                            active=bool(active.get("active")),
                            sample_at_iso=active.get("datetime") or "",
                        )
                except Exception as e:
                    log.warning("active flow fetch failed for %s: %s", sid, e)
                try:
                    row = fetch_last_minute_flow(user_id, sid, s.tz, access)
                    publish_minute_flow(pub, sid, row)
                except Exception as e:
                    log.warning("min-flow fetch failed for %s: %s", sid, e)

            publish_device_telemetry(pub, snap)
            publish_flow(pub, snap)

            # ---- Slow poll: period totals every SLOW_POLL seconds ----
            now_s = time.time()
            if now_s - last_slow_at >= SLOW_POLL:
                for sid, s in sensors.items():
                    try:
                        snap.period[sid] = fetch_period_totals(user_id, sid, s.tz, access)
                    except Exception as e:
                        log.warning("period totals fetch failed for %s: %s", sid, e)
                publish_period_totals(pub, snap)
                last_slow_at = now_s

            # ---- Alert poll: usage-alerts + notifications every ALERT_POLL ----
            if now_s - last_alert_at >= ALERT_POLL:
                try:
                    alerts_resp = fetch_usage_alerts(user_id, access, limit=100)
                    _absorb_alerts(snap, alerts_resp, log)
                except Exception as e:
                    log.warning("usage-alerts fetch failed: %s", e)
                try:
                    notif_resp = fetch_notifications(user_id, access, limit=100)
                    _absorb_notifications(snap, notif_resp)
                except Exception as e:
                    log.warning("notifications fetch failed: %s", e)
                last_alert_at = now_s

            publish_alerts(pub, snap)

        except PermissionError:
            log.warning("auth expired mid-poll; refreshing")
            try:
                access, refresh, token_expires_at = _refresh_or_login(refresh, log)
            except Exception as e:
                log.error("re-auth failed: %s", e)
                time.sleep(30)
        except requests.RequestException as e:
            log.error("network/HTTP error: %s", e)
        except Exception:
            log.exception("poll failed")

        for _ in range(FAST_POLL):
            if stopping:
                break
            time.sleep(1)

    pub.stop()
    log.info("shutdown clean")
    return 0


if __name__ == "__main__":
    sys.exit(main())
