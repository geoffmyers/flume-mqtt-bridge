<p align="center">
  <img src="docs/icon.svg" width="96" height="96" alt="Flume MQTT Bridge icon">
</p>

# Flume MQTT Bridge

<!-- BADGES:START -->
![Python 3.12](https://img.shields.io/badge/Python-3.12-3776ab?style=flat-square&logo=python)
[![Licence GPL-3.0-or-later](https://img.shields.io/badge/licence-GPL--3.0--or--later-blue?style=flat-square)](LICENSE.md)
[![PRs welcome](https://img.shields.io/badge/PRs-welcome-brightgreen?style=flat-square)](CONTRIBUTING.md)
<!-- BADGES:END -->

## Table of Contents

- [Description](#description)
- [Features](#features)
- [Requirements](#requirements)
- [Installation](#installation)
- [Usage](#usage)
- [Configuration](#configuration)
- [Architecture](#architecture)
- [Credits](#credits)
- [Contributing](#contributing)
- [License](#license)

## Description

A Python daemon that polls the official
[Flume Water v2 REST API](https://flumetech.readme.io/reference) on three
independent cadences and republishes flow rate, usage totals, device health
and Smart Leak alerts over MQTT with Home Assistant
[MQTT Discovery](https://www.home-assistant.io/integrations/mqtt/#mqtt-discovery).

Home Assistant already has an official `flume` integration, but it doesn't
surface everything Flume's API returns: per-sensor battery level, device
`connected`/`last_seen`/`oriented` health, full Smart Leak event history,
the notification stream, or a raw one-minute flow feed suitable for
time-series storage outside Home Assistant's recorder. This bridge runs
alongside the official integration rather than replacing it — its entities
live under a separate Home Assistant device, so there's no collision, and
you can run both while deciding which one to keep.

## Features

- Polls current flow rate, daily/monthly/yearly usage totals, and device
  health (battery, connectivity, orientation) on independent schedules.
- Publishes Home Assistant MQTT Discovery configs automatically — no manual
  YAML.
- Smart Leak alert and notification history, with a configurable hold time
  so a leak alert stays visible for a while after it fires.
- A raw one-minute flow reading published as a JSON side-channel topic (not
  a Home Assistant entity) for any consumer that wants finer-grained history
  than Home Assistant's recorder keeps.
- Handles Flume's OAuth token lifecycle automatically, including refresh and
  fallback to a fresh password grant if the refresh token is lost.
- Runs alongside Home Assistant's official `flume` integration with no
  entity collisions.

## Requirements

- **Docker** with the Compose plugin (Compose v2.17+, for `additional_contexts`)
- An MQTT broker reachable from the container, with Home Assistant's MQTT
  integration pointed at the same broker
- A [Flume Water](https://flumewater.com/) smart water monitor already set
  up on a Flume account
- A **Personal Developer** OAuth client (Flume Portal → Settings → API) —
  this issues the `client_id`/`client_secret` pair the bridge authenticates
  with, separate from your Flume account login

## Installation

```bash
git clone https://github.com/geoffmyers/flume-mqtt-bridge.git
cd flume-mqtt-bridge

cp .env.example .env
cp docker-compose.example.yml docker-compose.yml
```

Edit `.env` with your Flume Developer client credentials, account
credentials and MQTT broker details (see [Configuration](#configuration)),
then build and start the bridge:

```bash
docker compose build
docker compose up -d
docker compose logs -f
```

The application code is bind-mounted from `./app`, so after the first build
a code change only needs a container restart, not a rebuild.

The image is also published on the GitHub Container Registry as
`ghcr.io/geoffmyers/flume-mqtt-bridge`, for `linux/amd64` and `linux/arm64`, with the
application code in it: `docker compose pull` fetches it instead of
building. The compose file still mounts `./app` over that copy, so the
code in your checkout is what runs.

## Usage

On startup the bridge logs in, discovers every sensor and bridge (gateway)
device on your Flume account, publishes Home Assistant Discovery configs,
and starts polling. Each Flume sensor becomes its own Home Assistant device
(`Flume Bridge: <location>`) with entities including current flow rate,
daily/monthly/yearly gallons, battery, connectivity, orientation, Smart Leak
status and history, and notification counts. Each Flume gateway (bridge)
device gets its own connectivity and last-seen entities.

A raw one-minute flow reading is also published on
`<prefix>/<sensor_id>/flow/min` as JSON (`{datetime, gallons, published_at}`)
— this is not a Home Assistant entity, but a side channel for anything that
wants to ingest per-minute flow into a time-series database.

## Configuration

Environment variables, set in `.env` (`.env.example` lists them all):

| Variable | Default | Description |
|---|---|---|
| `FLUME_CLIENT_ID` | *(required)* | Personal Developer client id (Flume Portal → Settings → API) |
| `FLUME_CLIENT_SECRET` | *(required)* | Personal Developer client secret |
| `FLUME_USERNAME` | *(required)* | Flume account email |
| `FLUME_PASSWORD` | *(required)* | Flume account password |
| `MQTT_HOST` | `mosquitto` | MQTT broker hostname |
| `MQTT_PORT` | `1883` | MQTT broker port |
| `MQTT_USER` | *(empty)* | MQTT username |
| `MQTT_PASSWORD` | *(required)* | MQTT password |
| `FAST_POLL_INTERVAL` | `60` | Seconds between current-flow polls |
| `SLOW_POLL_INTERVAL` | `300` | Seconds between usage-total polls |
| `ALERT_POLL_INTERVAL` | `1800` | Seconds between Smart Leak / notification polls |
| `LEAK_HOLD_SECONDS` | `1800` | How long the Smart Leak entity stays `ON` after a fresh event |
| `HA_DISCOVERY_PREFIX` | `homeassistant` | Home Assistant MQTT Discovery topic prefix |
| `MQTT_TOPIC_PREFIX` | `flume` | Prefix for this bridge's own MQTT topics |
| `LOG_LEVEL` | `INFO` | Python log level |
| `GITHUB_ERROR_TOKEN` | *(unset)* | Optional. A GitHub token with `repo` scope; when set together with `GITHUB_REPO`, uncaught exceptions are filed as GitHub issues via `repository_dispatch` (see [Credits](#credits)) |
| `GITHUB_REPO` | *(unset)* | Optional. `owner/name` of the repo to file error reports against |
| `GITHUB_ERROR_ENVIRONMENT` | `production` | Optional. Environment label attached to filed error reports |

### Rate limits

Flume enforces 120 requests per minute per account, and a single flow-history
query can't span more than 24 hours. At the default poll intervals this
bridge uses roughly 200 requests/hour per sensor — comfortably under the
7,200/hour ceiling.

## Architecture

```
Flume Water v2 API  ◄──poll──  flume-mqtt-bridge  ──publish──►  MQTT broker  ──►  Home Assistant
(api.flumewater.com)            (Python, paho-mqtt)              (mosquitto)      (MQTT Discovery)
```

See [ARCHITECTURE.md](ARCHITECTURE.md) for the three poll tiers, the OAuth
flow and the full MQTT topic structure.

## Credits

- Talks to the official, documented
  [Flume Water v2 REST API](https://flumetech.readme.io/reference).
- MQTT client, Home Assistant Discovery payloads and topic/timestamp helpers
  come from this repository's own `ha-mqtt-bridge-toolkit` package, vendored
  in at `_shared/ha-mqtt-bridge-toolkit/` when this repository is published.
- Optional production-error reporting uses this repository's own
  `python-github-error-reporter` package, vendored in the same way at
  `_shared/python-github-error-reporter/`.
- The README icon is the [Font Awesome](https://fontawesome.com/)
  `faucet-drip` glyph, used under
  [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/).
- This project is not affiliated with, endorsed by, or supported by Flume,
  Inc.

Written by Geoff Myers.

## Contributing

Bug reports and pull requests are welcome. See [CONTRIBUTING.md](CONTRIBUTING.md)
for setup, checks and how this repository is published.

## License

This program is free software: you can redistribute it and/or modify it under
the terms of the GNU General Public License as published by the Free Software
Foundation, either version 3 of the License, or (at your option) any later
version.

This program is distributed in the hope that it will be useful, but WITHOUT ANY
WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
PARTICULAR PURPOSE. See [LICENSE.md](LICENSE.md) for the full text of the GNU
General Public License.

SPDX-License-Identifier: `GPL-3.0-or-later`
