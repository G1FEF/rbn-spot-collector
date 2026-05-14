from __future__ import annotations

from dataclasses import dataclass
import logging
from typing import Any
from urllib.parse import urlencode
from urllib.request import urlopen
import xml.etree.ElementTree as ET

from .config import Settings

LOGGER = logging.getLogger("rbn_spot_collector.qrz")
QRZ_XML_URL = "https://xmldata.qrz.com/xml/current/"
CALLSIGN_FIELDS = (
    "callsign",
    "aliases",
    "dxcc",
    "fname",
    "name",
    "addr1",
    "addr2",
    "state",
    "zip",
    "country",
    "lat",
    "lon",
    "grid",
    "county",
    "ccode",
    "fips",
    "land",
    "email",
    "class",
    "areacode",
    "timezone",
    "gmtoffset",
    "dst",
    "cqzone",
    "ituzone",
    "iota",
    "geoloc",
)
XML_TO_DB_FIELD = {
    "call": "callsign",
    "AreaCode": "areacode",
    "TimeZone": "timezone",
    "GMTOffset": "gmtoffset",
    "DST": "dst",
}


class QrzLookupError(RuntimeError):
    pass


def _local_name(tag: str) -> str:
    return tag.split("}", 1)[-1]


def _find_child(element: ET.Element, name: str) -> ET.Element | None:
    for child in element:
        if _local_name(child.tag) == name:
            return child
    return None


def _find_text(element: ET.Element, name: str) -> str | None:
    child = _find_child(element, name)
    if child is None or child.text is None:
        return None
    return child.text


def normalize_qrz_callsign(callsign: str) -> str:
    normalized = callsign.strip().upper()
    for separator in ("-", "/"):
        normalized = normalized.split(separator, 1)[0]
    return normalized.strip()


@dataclass
class QrzClient:
    settings: Settings
    session_key: str | None = None

    def enabled(self) -> bool:
        return bool(self.settings.qrz_username and self.settings.qrz_password)

    def _fetch_xml(self, params: dict[str, str]) -> ET.Element:
        query = urlencode(params, safe=";")
        with urlopen(f"{QRZ_XML_URL}?{query}", timeout=self.settings.socket_timeout_seconds) as response:
            return ET.fromstring(response.read())

    def _session_error(self, root: ET.Element) -> str | None:
        session = _find_child(root, "Session")
        if session is None:
            return "QRZ response did not include a Session node"
        error = _find_text(session, "Error")
        if error:
            return error.strip()
        return None

    def request_session_code(self) -> str:
        root = self._fetch_xml(
            {
                "username": self.settings.qrz_username,
                "password": self.settings.qrz_password,
                "agent": self.settings.qrz_agent,
            }
        )
        error = self._session_error(root)
        if error:
            raise QrzLookupError(f"QRZ login failed: {error}")

        session = _find_child(root, "Session")
        key = None if session is None else _find_text(session, "Key")
        if not key:
            raise QrzLookupError("QRZ login succeeded without a session key")
        return key.strip()

    def login(self) -> None:
        self.session_key = self.request_session_code()

    def _lookup_with_session(self, callsign: str, session_key: str) -> ET.Element:
        return self._fetch_xml({"s": session_key, "callsign": callsign})

    def lookup_callsign(self, callsign: str) -> dict[str, str | None] | None:
        if not self.enabled():
            return None

        normalized_callsign = normalize_qrz_callsign(callsign)
        if not normalized_callsign:
            return None

        if not self.session_key:
            self.login()

        root = self._lookup_with_session(normalized_callsign, self.session_key or "")
        error = self._session_error(root)
        if error:
            if "Session Timeout" in error or "Invalid session key" in error:
                self.session_key = None
                self.login()
                root = self._lookup_with_session(normalized_callsign, self.session_key or "")
                error = self._session_error(root)
            if error:
                raise QrzLookupError(f"QRZ callsign lookup failed for {normalized_callsign}: {error}")

        callsign_node = _find_child(root, "Callsign")
        if callsign_node is None:
            return None

        result: dict[str, str | None] = {}
        for child in callsign_node:
            tag_name = _local_name(child.tag)
            db_field = XML_TO_DB_FIELD.get(tag_name, tag_name.lower())
            if db_field in CALLSIGN_FIELDS:
                text = child.text.strip() if child.text else ""
                result[db_field] = text or None

        if not result.get("callsign"):
            return None
        return result


class CallsignLookupService:
    def __init__(self, settings: Settings, database) -> None:
        self.database = database
        self.qrz_client = QrzClient(settings)
        self._warning_logged = False

    def ensure_callsign(self, callsign: str) -> None:
        normalized_callsign = callsign.strip().upper()
        if not normalized_callsign:
            return

        if not self.database.callsign_needs_refresh(
            normalized_callsign,
            self.qrz_client.settings.callsign_expiry_seconds,
            self.qrz_client.settings.callsign_nack_seconds,
        ):
            return

        if not self.qrz_client.enabled():
            if not self._warning_logged:
                LOGGER.warning("QRZ enrichment skipped because QRZ_USERNAME or QRZ_PASSWORD is not configured")
                self._warning_logged = True
            return

        try:
            metadata = self.qrz_client.lookup_callsign(normalized_callsign)
        except Exception as exc:
            LOGGER.warning("QRZ lookup failed for %s: %s", normalized_callsign, exc)
            self.database.upsert_callsign({"callsign": normalized_callsign})
            return

        if metadata is None:
            LOGGER.info("No QRZ data found for %s", normalized_callsign)
            self.database.upsert_callsign({"callsign": normalized_callsign})
            return

        metadata["callsign"] = normalized_callsign
        self.database.upsert_callsign(metadata)
