from __future__ import annotations

import json
import logging
from multiprocessing import get_context
import socket
from typing import Any

from . import __version__
from .config import Settings

LOGGER = logging.getLogger("rbn_spot_collector.api")
MAX_REQUEST_BYTES = 64 * 1024
SUPPORTED_FIELDS = {
    "bycall": "dx_call",
    "ofcall": "spotter_call",
    "dx_grid": "dx_grid",
    "spotter_grid": "spotter_grid",
}
SUMMARY_RESULT_FIELDS = (
    "spotter_call",
    "spotter_grid",
    "dx_call",
    "dx_grid",
    "frequency_khz",
    "mode",
    "snr_db",
    "spotted_at",
)


def read_request(sock: socket.socket) -> bytes:
    chunks = bytearray()
    while len(chunks) < MAX_REQUEST_BYTES:
        chunk = sock.recv(4096)
        if not chunk:
            break
        chunks.extend(chunk)
        if b"\n" in chunk:
            break
    return bytes(chunks).strip()


def parse_request_payload(payload: bytes) -> tuple[str, str, int, str | None, bool]:
    try:
        request = json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("Request body must be valid JSON") from exc

    if not isinstance(request, dict):
        raise ValueError("Request body must be a JSON object")

    field = request.get("field")
    data = request.get("data")
    maxage = request.get("maxage")
    mode = request.get("mode")
    detail = request.get("detail")
    if not isinstance(field, str) or not isinstance(data, str):
        raise ValueError("Request JSON must include string fields 'field' and 'data'")
    if not isinstance(maxage, int) or isinstance(maxage, bool) or maxage < 0:
        raise ValueError("Request JSON must include integer field 'maxage' with a value of 0 or greater")
    if mode is None:
        normalized_mode = None
    elif isinstance(mode, str):
        normalized_mode = mode.strip().upper() or None
    else:
        raise ValueError("Request JSON field 'mode' must be a string, null, or omitted")
    if detail is None:
        normalized_detail = False
    elif isinstance(detail, bool):
        normalized_detail = detail
    else:
        raise ValueError("Request JSON field 'detail' must be a boolean, null, or omitted")

    return field, data, maxage, normalized_mode, normalized_detail


def format_results(rows: list[dict[str, Any]], detail: bool) -> list[dict[str, Any]]:
    if detail:
        return rows

    return [{field: row.get(field) for field in SUMMARY_RESULT_FIELDS} for row in rows]


def encode_response(status: str, body: dict[str, Any]) -> bytes:
    payload = {"status": status, "version": __version__, **body}
    return (json.dumps(payload, separators=(",", ":")) + "\n").encode("utf-8")


def handle_api_connection(conn: socket.socket, settings: Settings) -> None:
    from .database import Database

    with conn:
        try:
            payload = read_request(conn)
            if not payload:
                conn.sendall(encode_response("error", {"error": "Empty request body"}))
                return

            field, data, maxage, mode, detail = parse_request_payload(payload)
            db_field = SUPPORTED_FIELDS.get(field)
            if db_field is None:
                conn.sendall(
                    encode_response(
                        "error",
                        {"error": "Unsupported field value", "supported_fields": sorted(SUPPORTED_FIELDS)},
                    )
                )
                return

            database = Database(settings)
            results = format_results(
                database.find_spots(db_field, data, maxage, mode),
                detail,
            )
            conn.sendall(encode_response("ok", {"results": results}))
        except Exception as exc:
            LOGGER.warning("API request failed: %s", exc)
            conn.sendall(encode_response("error", {"error": str(exc)}))


def _serve_connection(conn: socket.socket, settings: Settings) -> None:
    handle_api_connection(conn, settings)


def run_api_server(settings: Settings) -> None:
    ctx = get_context("fork")
    with socket.create_server(("0.0.0.0", settings.api_port), reuse_port=False) as server:
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        LOGGER.info("API listener ready on 0.0.0.0:%s", settings.api_port)

        while True:
            conn, addr = server.accept()
            LOGGER.info("Accepted API connection from %s:%s", *addr[:2])
            process = ctx.Process(target=_serve_connection, args=(conn, settings), daemon=True)
            process.start()
            conn.close()
