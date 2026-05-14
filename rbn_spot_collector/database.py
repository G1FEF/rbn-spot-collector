from __future__ import annotations

from datetime import datetime, timedelta, timezone
from contextlib import contextmanager
from typing import Any, Iterator

import pymysql

from .config import Settings
from .parser import Spot

CALLSIGN_COLUMNS = (
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


class Database:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    @contextmanager
    def connection(self) -> Iterator[pymysql.connections.Connection]:
        conn = pymysql.connect(
            host=self.settings.db_host,
            port=self.settings.db_port,
            user=self.settings.db_user,
            password=self.settings.db_password,
            database=self.settings.db_name,
            charset=self.settings.db_charset,
            autocommit=False,
            cursorclass=pymysql.cursors.DictCursor,
        )
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def initialize(self) -> None:
        with self.connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS rbn_spots (
                        id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
                        spotter_call VARCHAR(32) NOT NULL,
                        dx_call VARCHAR(32) NOT NULL,
                        frequency_khz DECIMAL(10,3) NOT NULL,
                        mode VARCHAR(16) NOT NULL,
                        snr_db SMALLINT NOT NULL,
                        speed SMALLINT NULL,
                        speed_unit VARCHAR(8) NULL,
                        dx_grid VARCHAR(8) NULL,
                        spotter_grid VARCHAR(8) NULL,
                        report_text VARCHAR(128) NOT NULL,
                        extra_text VARCHAR(255) NULL,
                        spotted_at DATETIME NOT NULL,
                        received_at DATETIME NOT NULL DEFAULT UTC_TIMESTAMP(),
                        raw_line VARCHAR(255) NOT NULL,
                        PRIMARY KEY (id),
                        KEY idx_received_at (received_at),
                        KEY idx_spotted_at (spotted_at),
                        KEY idx_dx_call (dx_call),
                        KEY idx_spotter_call (spotter_call)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                    """
                )
                cursor.execute(
                    """
                    ALTER TABLE rbn_spots
                    MODIFY speed SMALLINT NULL,
                    MODIFY speed_unit VARCHAR(8) NULL,
                    ADD COLUMN IF NOT EXISTS dx_grid VARCHAR(8) NULL AFTER speed_unit,
                    ADD COLUMN IF NOT EXISTS spotter_grid VARCHAR(8) NULL AFTER dx_grid
                    """
                )
                cursor.execute(
                    """
                    SELECT COUNT(*) AS column_count
                    FROM information_schema.COLUMNS
                    WHERE TABLE_SCHEMA = DATABASE()
                    AND TABLE_NAME = 'rbn_spots'
                    AND COLUMN_NAME = 'grid'
                    """
                )
                row = cursor.fetchone()
                if row and row["column_count"]:
                    cursor.execute(
                        """
                        UPDATE rbn_spots
                        SET dx_grid = COALESCE(dx_grid, grid)
                        WHERE grid IS NOT NULL
                        """
                    )
                    cursor.execute(
                        """
                        ALTER TABLE rbn_spots
                        DROP COLUMN grid
                        """
                    )
                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS callsigns (
                        callsign VARCHAR(32) NOT NULL,
                        aliases TEXT NULL,
                        dxcc VARCHAR(16) NULL,
                        fname VARCHAR(128) NULL,
                        name VARCHAR(128) NULL,
                        addr1 VARCHAR(255) NULL,
                        addr2 VARCHAR(255) NULL,
                        state VARCHAR(64) NULL,
                        zip VARCHAR(32) NULL,
                        country VARCHAR(128) NULL,
                        lat VARCHAR(32) NULL,
                        lon VARCHAR(32) NULL,
                        grid VARCHAR(16) NULL,
                        county VARCHAR(128) NULL,
                        ccode VARCHAR(16) NULL,
                        fips VARCHAR(32) NULL,
                        land VARCHAR(128) NULL,
                        email VARCHAR(255) NULL,
                        class VARCHAR(32) NULL,
                        areacode VARCHAR(32) NULL,
                        timezone VARCHAR(64) NULL,
                        gmtoffset VARCHAR(16) NULL,
                        dst VARCHAR(8) NULL,
                        cqzone VARCHAR(16) NULL,
                        ituzone VARCHAR(16) NULL,
                        iota VARCHAR(32) NULL,
                        geoloc VARCHAR(32) NULL,
                        updated_at DATETIME NOT NULL DEFAULT UTC_TIMESTAMP(),
                        PRIMARY KEY (callsign)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                    """
                )
                cursor.execute(
                    """
                    ALTER TABLE callsigns
                    ADD COLUMN IF NOT EXISTS updated_at DATETIME NOT NULL DEFAULT UTC_TIMESTAMP()
                    """
                )
                cursor.execute(
                    """
                    UPDATE rbn_spots
                    LEFT JOIN callsigns AS spotter_callsign
                        ON spotter_callsign.callsign = rbn_spots.spotter_call
                    SET rbn_spots.spotter_grid = COALESCE(rbn_spots.spotter_grid, spotter_callsign.grid)
                    WHERE rbn_spots.spotter_grid IS NULL
                    AND spotter_callsign.grid IS NOT NULL
                    """
                )

    def insert_spot(self, spot: Spot) -> None:
        with self.connection() as conn:
            with conn.cursor() as cursor:
                cached_grids = self._fetch_callsign_grids(
                    cursor,
                    [spot.spotter_call, spot.dx_call],
                )
                spotter_grid = cached_grids.get(spot.spotter_call)
                dx_grid = spot.grid or cached_grids.get(spot.dx_call)
                cursor.execute(
                    """
                    INSERT INTO rbn_spots (
                        spotter_call,
                        dx_call,
                        frequency_khz,
                        mode,
                        snr_db,
                        speed,
                        speed_unit,
                        dx_grid,
                        spotter_grid,
                        report_text,
                        extra_text,
                        spotted_at,
                        raw_line
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        spot.spotter_call,
                        spot.dx_call,
                        spot.frequency_khz,
                        spot.mode,
                        spot.snr_db,
                        spot.speed,
                        spot.speed_unit,
                        dx_grid,
                        spotter_grid,
                        spot.report,
                        spot.extra,
                        spot.spotted_at.strftime("%Y-%m-%d %H:%M:%S"),
                        spot.raw_line,
                    ),
                )

    def _fetch_callsign_grids(
        self,
        cursor: pymysql.cursors.DictCursor,
        callsigns: list[str],
    ) -> dict[str, str]:
        unique_callsigns = [callsign for callsign in dict.fromkeys(callsigns) if callsign]
        if not unique_callsigns:
            return {}

        placeholders = ", ".join(["%s"] * len(unique_callsigns))
        cursor.execute(
            f"""
            SELECT callsign, grid
            FROM callsigns
            WHERE callsign IN ({placeholders})
            AND grid IS NOT NULL
            """,
            unique_callsigns,
        )
        rows = cursor.fetchall()
        return {
            row["callsign"]: row["grid"]
            for row in rows
            if row.get("callsign") and row.get("grid")
        }

    def purge_old_spots(self) -> int:
        with self.connection() as conn:
            with conn.cursor() as cursor:
                deleted = cursor.execute(
                    """
                    DELETE FROM rbn_spots
                    WHERE received_at < (UTC_TIMESTAMP() - INTERVAL 24 HOUR)
                    """
                )
        return deleted

    def get_callsign_updated_at(self, callsign: str) -> datetime | None:
        cache_entry = self.get_callsign_cache_entry(callsign)
        if cache_entry is None:
            return None
        return cache_entry.get("updated_at")

    def get_callsign_cache_entry(self, callsign: str) -> dict[str, Any] | None:
        with self.connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    "SELECT callsign, geoloc, updated_at FROM callsigns WHERE callsign = %s LIMIT 1",
                    (callsign,),
                )
                row = cursor.fetchone()

        if row is None:
            return None

        updated_at = row.get("updated_at")
        if updated_at is None:
            normalized_updated_at = None
        elif updated_at.tzinfo is None:
            normalized_updated_at = updated_at.replace(tzinfo=timezone.utc)
        else:
            normalized_updated_at = updated_at.astimezone(timezone.utc)

        return {
            "callsign": row.get("callsign"),
            "geoloc": row.get("geoloc"),
            "updated_at": normalized_updated_at,
        }

    def callsign_needs_refresh(
        self,
        callsign: str,
        expiry_seconds: int,
        nack_seconds: int,
    ) -> bool:
        cache_entry = self.get_callsign_cache_entry(callsign)
        if cache_entry is None:
            return True

        updated_at = cache_entry.get("updated_at")
        if updated_at is None:
            return True

        age_limit = expiry_seconds
        if cache_entry.get("geoloc") is None:
            age_limit = nack_seconds

        return updated_at < (datetime.now(timezone.utc) - timedelta(seconds=age_limit))

    def upsert_callsign(self, metadata: dict[str, str | None]) -> None:
        columns = ", ".join(CALLSIGN_COLUMNS)
        placeholders = ", ".join(["%s"] * len(CALLSIGN_COLUMNS))
        updates = ", ".join(
            f"{column} = VALUES({column})" for column in CALLSIGN_COLUMNS if column != "callsign"
        )
        values = [metadata.get(column) for column in CALLSIGN_COLUMNS]

        with self.connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    f"""
                    INSERT INTO callsigns ({columns}, updated_at)
                    VALUES ({placeholders}, UTC_TIMESTAMP())
                    ON DUPLICATE KEY UPDATE {updates}, updated_at = UTC_TIMESTAMP()
                    """,
                    values,
                )

    def find_spots(
        self,
        field_name: str,
        field_value: str,
        max_age_seconds: int,
        mode: str | None,
    ) -> list[dict[str, Any]]:
        if field_name not in {"dx_call", "spotter_call", "dx_grid", "spotter_grid"}:
            raise ValueError(f"Unsupported field name {field_name!r}")

        query = f"""
            SELECT
                rbn_spots.id,
                rbn_spots.spotter_call,
                rbn_spots.dx_call,
                rbn_spots.frequency_khz,
                rbn_spots.mode,
                rbn_spots.snr_db,
                rbn_spots.speed,
                rbn_spots.speed_unit,
                rbn_spots.dx_grid AS grid,
                COALESCE(rbn_spots.spotter_grid, spotter_callsign.grid) AS spotter_grid,
                rbn_spots.report_text,
                rbn_spots.extra_text,
                rbn_spots.spotted_at,
                rbn_spots.received_at,
                rbn_spots.raw_line,
                dx_callsign.grid AS dx_grid
            FROM rbn_spots
            LEFT JOIN callsigns AS spotter_callsign
                ON spotter_callsign.callsign = rbn_spots.spotter_call
            LEFT JOIN callsigns AS dx_callsign
                ON dx_callsign.callsign = rbn_spots.dx_call
            WHERE rbn_spots.{field_name} = %s
            AND received_at >= (UTC_TIMESTAMP() - INTERVAL %s SECOND)
        """
        params: list[Any] = [field_value, max_age_seconds]
        if mode is not None:
            query += "\nAND mode = %s"
            params.append(mode)
        query += "\nORDER BY spotted_at DESC, id DESC"

        with self.connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, params)
                rows = cursor.fetchall()

        for row in rows:
            for key in ("spotted_at", "received_at"):
                value = row.get(key)
                if value is not None and hasattr(value, "isoformat"):
                    row[key] = value.isoformat()

            frequency = row.get("frequency_khz")
            if frequency is not None:
                row["frequency_khz"] = float(frequency)

        return rows
