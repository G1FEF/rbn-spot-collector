# RBN Spot Collector

This app connects to the Reverse Beacon Network feeds, parses incoming spot lines, stores them in MariaDB, and periodically deletes rows older than 24 hours.

## What it stores

Each parsed spot is saved with:

- spotter callsign
- spotted callsign
- frequency in kHz
- mode
- SNR in dB
- speed and speed unit
- grid square for FT8/FT4 spots
- report text
- the UTC timestamp carried by the spot
- the raw line received from the feed
- the UTC time the app inserted the row

## Debian Trixie setup

1. Install system packages:

```bash
sudo apt update
sudo apt install -y python3 python3-venv mariadb-server
```

2. Create a database and user in MariaDB:

```sql
CREATE DATABASE rbn CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
CREATE USER 'rbn'@'localhost' IDENTIFIED BY 'change-me';
GRANT ALL PRIVILEGES ON rbn.* TO 'rbn'@'localhost';
FLUSH PRIVILEGES;
```

3. Create a virtual environment and install Python dependencies:

```bash
python3 -m venv .venv
. .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
```

4. Copy the example environment file and adjust values:

```bash
cp .env.example .env
```

5. Export the environment variables before running:

```bash
set -a
. ./.env
set +a
python app.py
```

Set `callsign` in `.env` to the value you want sent when the telnet server prompts with `Please enter your call:`.
Set `api_port` in `.env` to the TCP port the JSON lookup listener should bind to.
Set `QRZ_USERNAME` and `QRZ_PASSWORD` if you want automatic QRZ.com callsign enrichment.
Set `CALLSIGN_EXPIRY` to the number of seconds a cached QRZ callsign record should be kept before it is refreshed.
Set `CALLSIGN_NACK` to the number of seconds to wait before retrying a QRZ lookup for a callsign that is cached with `geoloc` still `null`.

## Run as a systemd daemon

The repository includes a sample unit file at [rbn-spot-collector.service](/Users/chris/Documents/Codex/2026-04-26/rbn-spot-collector/rbn-spot-collector.service).

It expects:

- the app to live at `/opt/rbn-spot-collector`
- a virtualenv at `/opt/rbn-spot-collector/.venv`
- the environment file at `/opt/rbn-spot-collector/.env`
- a system user and group named `rbn`

Install it on a Debian or other systemd host with:

```bash
sudo useradd --system --home /opt/rbn-spot-collector --shell /usr/sbin/nologin rbn
sudo mkdir -p /opt/rbn-spot-collector
sudo cp -r . /opt/rbn-spot-collector
cd /opt/rbn-spot-collector
sudo python3 -m venv .venv
sudo .venv/bin/pip install --upgrade pip
sudo .venv/bin/pip install -r requirements.txt
sudo cp rbn-spot-collector.service /etc/systemd/system/
sudo chown -R rbn:rbn /opt/rbn-spot-collector
sudo systemctl daemon-reload
sudo systemctl enable --now rbn-spot-collector
```

Useful service commands:

```bash
sudo systemctl status rbn-spot-collector
sudo journalctl -u rbn-spot-collector -f
sudo systemctl restart rbn-spot-collector
```

## Notes about the RBN endpoints

The official RBN site documents `telnet.reversebeacon.net:7000` for CW/RTTY and `telnet.reversebeacon.net:7001` for FT8. This app connects to both feeds, and parses FT8 and FT4 lines from the `7001` stream using the grid token that appears where CW/RTTY spots carry speed data. If direct connections are refused on your system, point `RBN_HOST` and `RBN_PORT` at a relay DX cluster for the CW/RTTY stream and adjust the code similarly for the FT stream if needed.

## QRZ callsign cache

The app creates a `callsigns` table and, after each spot insert, checks both `dx_call` and `spotter_call`. If a callsign is missing from the cache, or its `updated_at` is older than `CALLSIGN_EXPIRY`, and QRZ credentials are configured, the app logs into the official QRZ XML API and stores these fields. If a QRZ lookup fails or returns no callsign record, the app still inserts or updates the row with only `callsign` and `updated_at`, leaving the other fields `NULL`. Those negative-cache rows are retried only after `CALLSIGN_NACK` seconds if `geoloc` is still `NULL`.

- `callsign`
- `aliases`
- `dxcc`
- `fname`
- `name`
- `addr1`
- `addr2`
- `state`
- `zip`
- `country`
- `lat`
- `lon`
- `grid`
- `county`
- `ccode`
- `fips`
- `land`
- `email`
- `class`
- `areacode`
- `timezone`
- `gmtoffset`
- `dst`
- `cqzone`
- `ituzone`
- `iota`
- `geoloc`
- `updated_at`

## Cleanup behavior

The app runs:

```sql
DELETE FROM rbn_spots
WHERE received_at < (UTC_TIMESTAMP() - INTERVAL 24 HOUR);
```

by default every 300 seconds. You can change that with `CLEANUP_INTERVAL_SECONDS`.

## TCP JSON lookup API

The app also opens a TCP listener on `api_port`. Each connection is handled in a child process.

Send a JSON object with `field`, `data`, `maxage`, and optionally `mode` and `detail`, for example:

```json
{"field":"bycall","data":"VE6AMR","maxage":3600,"mode":"FT8","detail":true}
```

`field` supports:

- `bycall` to search `rbn_spots.dx_call`
- `ofcall` to search `rbn_spots.spotter_call`
- `dx_grid` to search `rbn_spots.dx_grid`
- `spotter_grid` to search `rbn_spots.spotter_grid`

`maxage` limits matches to rows whose `received_at` is within that many seconds of the current UTC time.
`mode` filters matches on the database `mode` field. If `mode` is `""`, `null`, or omitted, the app returns all modes.
`detail` controls the response shape. If it is missing, `null`, or `false`, each result only includes `spotter_call`, `spotter_grid`, `dx_call`, `dx_grid`, `frequency_khz`, `mode`, `snr_db`, and `spotted_at`. If `detail` is `true`, the app returns all available fields.

If the request is valid, the app returns:

```json
{"status":"ok","results":[...]}
```

Each result also includes:

- `spotter_grid` from `callsigns.grid` for the `spotter_call`
- `dx_grid` from `callsigns.grid` for the `dx_call`

## Run tests

```bash
python -m unittest discover -s tests
```
