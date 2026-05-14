from __future__ import annotations

from datetime import datetime, timezone
import logging
import socket
import threading
import time

from .api_server import run_api_server
from . import __version__
from .config import load_settings
from .parser import parse_spot_line
from .qrz import CallsignLookupService

LOGGER = logging.getLogger("rbn_spot_collector")
CALLSIGN_PROMPT = "Please enter your call:"
FT_HOST = "telnet.reversebeacon.net"
FT_PORT = 7001


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )


def wait_for_prompt(sock: socket.socket, prompt: str) -> None:
    prompt_bytes = prompt.encode("utf-8")
    received = b""

    while prompt_bytes not in received:
        chunk = sock.recv(4096)
        if not chunk:
            raise ConnectionError(f"Connection closed before prompt {prompt!r} was received")
        received += chunk


def send_callsign(sock: socket.socket, callsign: str) -> None:
    sock.sendall(f"{callsign}\n".encode("utf-8"))


def line_stream(host: str, port: int, timeout_seconds: float, callsign: str):
    with socket.create_connection((host, port), timeout=timeout_seconds) as sock:
        sock.settimeout(timeout_seconds)
        wait_for_prompt(sock, CALLSIGN_PROMPT)
        send_callsign(sock, callsign)
        buffer = sock.makefile("r", encoding="utf-8", errors="replace", newline="\n")
        for line in buffer:
            yield line


def process_feed(
    host: str,
    port: int,
    timeout_seconds: float,
    reconnect_delay_seconds: float,
    callsign: str,
    database,
    callsign_lookup_service: CallsignLookupService,
) -> None:
    while True:
        try:
            LOGGER.info("Connecting to RBN feed at %s:%s", host, port)
            for line in line_stream(host, port, timeout_seconds, callsign):
                now_utc = datetime.now(timezone.utc)
                spot = parse_spot_line(line, now_utc=now_utc)
                if spot is not None:
                    database.insert_spot(spot)
                    callsign_lookup_service.ensure_callsign(spot.dx_call)
                    callsign_lookup_service.ensure_callsign(spot.spotter_call)
        except (OSError, socket.timeout) as exc:
            LOGGER.warning("Feed connection error on %s:%s: %s", host, port, exc)
        except Exception:
            LOGGER.exception("Unexpected failure while processing the RBN feed on %s:%s", host, port)

        LOGGER.info("Reconnecting %s:%s in %s seconds", host, port, reconnect_delay_seconds)
        time.sleep(reconnect_delay_seconds)


def run() -> None:
    from .database import Database

    configure_logging()
    LOGGER.info("Starting RBN Spot Collector version %s", __version__)
    settings = load_settings()
    if not settings.callsign:
        raise ValueError("The callsign environment variable must be set before starting the collector")

    database = Database(settings)
    database.initialize()
    LOGGER.info("Database initialized")
    callsign_lookup_service = CallsignLookupService(settings, database)
    threading.Thread(
        target=run_api_server,
        args=(settings,),
        daemon=True,
        name="rbn-api-listener",
    ).start()
    LOGGER.info("API listener thread started on port %s", settings.api_port)

    feed_threads = [
        threading.Thread(
            target=process_feed,
            args=(
                settings.rbn_host,
                settings.rbn_port,
                settings.socket_timeout_seconds,
                settings.reconnect_delay_seconds,
                settings.callsign,
                database,
                callsign_lookup_service,
            ),
            daemon=True,
            name="rbn-feed-cw-rtty",
        ),
        threading.Thread(
            target=process_feed,
            args=(
                FT_HOST,
                FT_PORT,
                settings.socket_timeout_seconds,
                settings.reconnect_delay_seconds,
                settings.callsign,
                database,
                callsign_lookup_service,
            ),
            daemon=True,
            name="rbn-feed-ft8-ft4",
        ),
    ]
    for thread in feed_threads:
        thread.start()
    LOGGER.info("Feed listener threads started for %s:%s and %s:%s", settings.rbn_host, settings.rbn_port, FT_HOST, FT_PORT)

    next_cleanup_at = time.monotonic()

    while True:
        if time.monotonic() >= next_cleanup_at:
            deleted = database.purge_old_spots()
            LOGGER.info("Cleanup complete, deleted %s old rows", deleted)
            next_cleanup_at = time.monotonic() + settings.cleanup_interval_seconds

        time.sleep(1)


if __name__ == "__main__":
    run()
