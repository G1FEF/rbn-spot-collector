from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    rbn_host: str = os.getenv("RBN_HOST", "telnet.reversebeacon.net")
    rbn_port: int = int(os.getenv("RBN_PORT", "7000"))
    callsign: str = os.getenv("callsign", os.getenv("CALLSIGN", "")).strip()
    api_port: int = int(os.getenv("api_port", os.getenv("API_PORT", "9000")))
    qrz_username: str = os.getenv("QRZ_USERNAME", "").strip()
    qrz_password: str = os.getenv("QRZ_PASSWORD", "").strip()
    qrz_agent: str = os.getenv("QRZ_AGENT", "rbn-spot-collector").strip()
    callsign_expiry_seconds: int = int(os.getenv("CALLSIGN_EXPIRY", "86400"))
    callsign_nack_seconds: int = int(os.getenv("CALLSIGN_NACK", "3600"))
    socket_timeout_seconds: float = float(os.getenv("RBN_SOCKET_TIMEOUT_SECONDS", "30"))
    reconnect_delay_seconds: float = float(os.getenv("RBN_RECONNECT_DELAY_SECONDS", "10"))
    db_host: str = os.getenv("MARIADB_HOST", "127.0.0.1")
    db_port: int = int(os.getenv("MARIADB_PORT", "3306"))
    db_user: str = os.getenv("MARIADB_USER", "rbn")
    db_password: str = os.getenv("MARIADB_PASSWORD", "")
    db_name: str = os.getenv("MARIADB_DATABASE", "rbn")
    db_charset: str = os.getenv("MARIADB_CHARSET", "utf8mb4")
    cleanup_interval_seconds: int = int(os.getenv("CLEANUP_INTERVAL_SECONDS", "300"))


def load_settings() -> Settings:
    return Settings()
