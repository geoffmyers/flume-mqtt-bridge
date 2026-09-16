# Architecture

A single Python process. It logs in once via OAuth, then runs three
independent poll loops against the
[Flume Water v2 REST API](https://flumetech.readme.io/reference).

## Auth

OAuth password grant (`grant_type: password`) against `POST /oauth/token`
using your Personal Developer `client_id`/`client_secret` plus your account
username/password, returning an access token (roughly a 7-day TTL) and a
refresh token. The account's numeric user ID is extracted from the JWT
access token itself (its `user_id` claim) rather than a separate API call.
On expiry the bridge tries the refresh-token grant first, falling back to a
fresh password grant if that fails — Flume's refresh tokens are single-use,
so a lost refresh token (for example after a bridge restart before the next
save) is not fatal.

## Poll tiers

| Tier | Default interval | What it fetches |
|---|---|---|
| Fast | 60 s | Per-sensor current flow (`/query/active`) and the most recent one-minute flow bucket |
| Slow | 300 s | Day/month/year usage totals via one batched `/query` call per sensor, plus device list (battery, `connected`, `last_seen`, `oriented`) |
| Alert | 1800 s | `/usage-alerts` (Smart Leak event history) and `/notifications` |

The device list itself (which sensors and bridges exist) is fetched on
every fast-tier cycle, since it's needed to know what to poll; it also
double as the health-telemetry source.

## Leak alerts

Each `/usage-alerts` record includes a `flume_leak` boolean and a trigger
timestamp. When the most recent leak-flagged alert for a sensor is more
recent than `LEAK_HOLD_SECONDS`, the `Smart Leak Active` binary_sensor is
held `ON`; it clears once that window elapses without a newer alert.

## Module layout

| Path | Role |
|---|---|
| `app/main.py` | OAuth (login, refresh), the three poll loops, Home Assistant Discovery payload assembly, MQTT publish |
| `app/requirements.txt` | `paho-mqtt`, `requests` |
| `_shared/ha-mqtt-bridge-toolkit/` | Vendored at publish time — MQTT client, Discovery payload builders, topic/timestamp helpers |
| `_shared/python-github-error-reporter/` | Vendored at publish time — optional production-error reporting |

## MQTT topics

All topics live under `<MQTT_TOPIC_PREFIX>/` (`flume/` by default).

| Topic | Payload |
|---|---|
| `<prefix>/bridge/online` | `ON`/`OFF` — bridge LWT |
| `<prefix>/<sensor_id>/current_gpm` | float, gallons per minute |
| `<prefix>/<sensor_id>/active` | `ON`/`OFF` — whether water is currently flowing |
| `<prefix>/<sensor_id>/gallons_{today,month,year}` | float, resets on the sensor's local calendar boundary |
| `<prefix>/<sensor_id>/battery`, `battery_level` | numeric % (25/50/100 mapped from low/medium/high) and the human-readable level |
| `<prefix>/<sensor_id>/connected`, `oriented`, `last_seen` | device health |
| `<prefix>/<sensor_id>/smart_leak_active`, `last_leak_event`, `leak_event_count` | leak alert state and history |
| `<prefix>/<sensor_id>/last_notification`, `notifications_24h` | notification stream |
| `<prefix>/<sensor_id>/flow/min` | JSON `{datetime, gallons, published_at}` — one-minute raw flow, not a Home Assistant entity |
| `<prefix>/<bridge_id>/connected`, `last_seen` | gateway (bridge device) health |
