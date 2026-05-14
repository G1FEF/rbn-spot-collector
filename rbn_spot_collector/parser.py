from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import re

CW_RTTY_SPOT_PATTERN = re.compile(
    r"^DX de (?P<spotter>[^:]+):\s+"
    r"(?P<frequency>\d+(?:\.\d+)?)\s+"
    r"(?P<dx_call>\S+)\s+"
    r"(?P<mode>\S+)\s+"
    r"(?P<snr>-?\d+)\s+dB\s+"
    r"(?P<speed>\d+)\s+"
    r"(?P<speed_unit>WPM|BPS)\s+"
    r"(?P<report>.+?)\s+"
    r"(?P<time>\d{4})Z"
    r"(?:\s+(?P<extra>.*))?$"
)

FT_SPOT_PATTERN = re.compile(
    r"^DX de (?P<spotter>[^:]+):\s+"
    r"(?P<frequency>\d+(?:\.\d+)?)\s+"
    r"(?P<dx_call>\S+)\s+"
    r"(?P<mode>FT8|FT4)\s+"
    r"(?P<snr>-?\d+)\s+dB\s+"
    r"(?P<grid>[A-R]{2}\d{2}(?:[A-X]{2})?)\s+"
    r"(?P<report>.+?)\s+"
    r"(?P<time>\d{4})Z"
    r"(?:\s+(?P<extra>.*))?$",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class Spot:
    spotter_call: str
    dx_call: str
    frequency_khz: float
    mode: str
    snr_db: int
    speed: int | None
    speed_unit: str | None
    grid: str | None
    report: str
    spotted_at: datetime
    raw_line: str
    extra: str | None = None


def infer_spotted_at(time_token: str, now_utc: datetime | None = None) -> datetime:
    now = now_utc or datetime.now(timezone.utc)
    hour = int(time_token[:2])
    minute = int(time_token[2:])
    candidate = now.replace(hour=hour, minute=minute, second=0, microsecond=0)

    # RBN lines only carry HHMMZ, so adjust if a spot from just before midnight
    # arrives after UTC day rollover.
    if candidate - now > timedelta(hours=1):
        candidate -= timedelta(days=1)

    return candidate


def normalize_spotter_call(spotter_call: str) -> str:
    if spotter_call.endswith("-#"):
        return spotter_call[:-2]
    return spotter_call


def parse_spot_line(line: str, now_utc: datetime | None = None) -> Spot | None:
    normalized_line = line.strip()

    match = CW_RTTY_SPOT_PATTERN.match(normalized_line)
    if match:
        groups = match.groupdict()
        return Spot(
            spotter_call=normalize_spotter_call(groups["spotter"]),
            dx_call=groups["dx_call"],
            frequency_khz=float(groups["frequency"]),
            mode=groups["mode"],
            snr_db=int(groups["snr"]),
            speed=int(groups["speed"]),
            speed_unit=groups["speed_unit"],
            grid=None,
            report=groups["report"].strip(),
            spotted_at=infer_spotted_at(groups["time"], now_utc=now_utc),
            raw_line=line.rstrip("\r\n"),
            extra=groups.get("extra") or None,
        )

    match = FT_SPOT_PATTERN.match(normalized_line)
    if match:
        groups = match.groupdict()
        return Spot(
            spotter_call=normalize_spotter_call(groups["spotter"]),
            dx_call=groups["dx_call"],
            frequency_khz=float(groups["frequency"]),
            mode=groups["mode"].upper(),
            snr_db=int(groups["snr"]),
            speed=None,
            speed_unit=None,
            grid=groups["grid"].upper(),
            report=groups["report"].strip(),
            spotted_at=infer_spotted_at(groups["time"], now_utc=now_utc),
            raw_line=line.rstrip("\r\n"),
            extra=groups.get("extra") or None,
        )

    return None
