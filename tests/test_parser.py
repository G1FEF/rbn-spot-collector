from datetime import datetime, timezone
import unittest
import xml.etree.ElementTree as ET
from unittest.mock import Mock

from rbn_spot_collector.api_server import SUPPORTED_FIELDS, encode_response, format_results, parse_request_payload
from rbn_spot_collector.main import send_callsign, wait_for_prompt
from rbn_spot_collector.parser import infer_spotted_at, parse_spot_line
from rbn_spot_collector.qrz import CallsignLookupService, QrzClient


class FakeSocket:
    def __init__(self, chunks: list[bytes]):
        self._chunks = list(chunks)
        self.sent = bytearray()

    def recv(self, _: int) -> bytes:
        if self._chunks:
            return self._chunks.pop(0)
        return b""

    def sendall(self, data: bytes) -> None:
        self.sent.extend(data)


class ParserTests(unittest.TestCase):
    def test_parse_cw_spot(self) -> None:
        line = "DX de VE6AO-#: 28009.0 VE6AMR CW 09 dB 22 WPM CQ 2047Z"
        now = datetime(2026, 5, 10, 20, 50, tzinfo=timezone.utc)
        spot = parse_spot_line(line, now_utc=now)

        self.assertIsNotNone(spot)
        assert spot is not None
        self.assertEqual(spot.spotter_call, "VE6AO")
        self.assertEqual(spot.dx_call, "VE6AMR")
        self.assertEqual(spot.frequency_khz, 28009.0)
        self.assertEqual(spot.mode, "CW")
        self.assertEqual(spot.snr_db, 9)
        self.assertEqual(spot.speed, 22)
        self.assertEqual(spot.speed_unit, "WPM")
        self.assertIsNone(spot.grid)
        self.assertEqual(spot.report, "CQ")
        self.assertEqual(spot.spotted_at, datetime(2026, 5, 10, 20, 47, tzinfo=timezone.utc))

    def test_parse_psk_spot(self) -> None:
        line = "DX de DL9GTB-#: 3581.3 F4BQS BPSK 26 dB 31 BPS CQ 2047Z"
        now = datetime(2026, 5, 10, 20, 50, tzinfo=timezone.utc)
        spot = parse_spot_line(line, now_utc=now)

        self.assertIsNotNone(spot)
        assert spot is not None
        self.assertEqual(spot.mode, "BPSK")
        self.assertEqual(spot.speed_unit, "BPS")

    def test_parse_ft8_spot_with_grid(self) -> None:
        line = "DX de W3OA-#: 14074.0 VE6AMR FT8 -12 dB DO33 CQ 2047Z"
        now = datetime(2026, 5, 10, 20, 50, tzinfo=timezone.utc)
        spot = parse_spot_line(line, now_utc=now)

        self.assertIsNotNone(spot)
        assert spot is not None
        self.assertEqual(spot.mode, "FT8")
        self.assertEqual(spot.grid, "DO33")
        self.assertIsNone(spot.speed)
        self.assertIsNone(spot.speed_unit)
        self.assertEqual(spot.report, "CQ")

    def test_parse_ft4_spot_with_six_character_grid(self) -> None:
        line = "DX de N0CALL-#: 7047.5 M0ABC FT4 -7 dB IO91WM TEST 2047Z"
        now = datetime(2026, 5, 10, 20, 50, tzinfo=timezone.utc)
        spot = parse_spot_line(line, now_utc=now)

        self.assertIsNotNone(spot)
        assert spot is not None
        self.assertEqual(spot.mode, "FT4")
        self.assertEqual(spot.spotter_call, "N0CALL")
        self.assertEqual(spot.grid, "IO91WM")

    def test_rolls_back_across_midnight(self) -> None:
        now = datetime(2026, 5, 10, 0, 5, tzinfo=timezone.utc)
        spotted_at = infer_spotted_at("2359", now_utc=now)
        self.assertEqual(spotted_at, datetime(2026, 5, 9, 23, 59, tzinfo=timezone.utc))

    def test_wait_for_prompt_reads_until_prompt_arrives(self) -> None:
        sock = FakeSocket([b"Welcome\r\nPlease enter ", b"your call:"])
        wait_for_prompt(sock, "Please enter your call:")

    def test_send_callsign_writes_callsign_and_newline(self) -> None:
        sock = FakeSocket([])
        send_callsign(sock, "M0ABC")
        self.assertEqual(sock.sent, b"M0ABC\n")

    def test_parse_request_payload_accepts_expected_shape(self) -> None:
        field, data, maxage, mode, detail = parse_request_payload(
            b'{"field":"bycall","data":"VE6AMR","maxage":3600,"mode":"ft8","detail":true}'
        )
        self.assertEqual(field, "bycall")
        self.assertEqual(data, "VE6AMR")
        self.assertEqual(maxage, 3600)
        self.assertEqual(mode, "FT8")
        self.assertTrue(detail)

    def test_supported_fields_include_grid_lookups(self) -> None:
        self.assertEqual(SUPPORTED_FIELDS["dx_grid"], "dx_grid")
        self.assertEqual(SUPPORTED_FIELDS["spotter_grid"], "spotter_grid")

    def test_parse_request_payload_rejects_invalid_json(self) -> None:
        with self.assertRaises(ValueError):
            parse_request_payload(b"not-json")

    def test_parse_request_payload_requires_maxage(self) -> None:
        with self.assertRaises(ValueError):
            parse_request_payload(b'{"field":"bycall","data":"VE6AMR"}')

    def test_parse_request_payload_accepts_missing_mode(self) -> None:
        field, data, maxage, mode, detail = parse_request_payload(
            b'{"field":"bycall","data":"VE6AMR","maxage":3600}'
        )
        self.assertEqual(field, "bycall")
        self.assertEqual(data, "VE6AMR")
        self.assertEqual(maxage, 3600)
        self.assertIsNone(mode)
        self.assertFalse(detail)

    def test_parse_request_payload_accepts_null_mode(self) -> None:
        _, _, _, mode, _ = parse_request_payload(
            b'{"field":"bycall","data":"VE6AMR","maxage":3600,"mode":null}'
        )
        self.assertIsNone(mode)

    def test_parse_request_payload_accepts_empty_mode(self) -> None:
        _, _, _, mode, _ = parse_request_payload(
            b'{"field":"bycall","data":"VE6AMR","maxage":3600,"mode":"   "}'
        )
        self.assertIsNone(mode)

    def test_parse_request_payload_accepts_null_detail(self) -> None:
        _, _, _, _, detail = parse_request_payload(
            b'{"field":"bycall","data":"VE6AMR","maxage":3600,"detail":null}'
        )
        self.assertFalse(detail)

    def test_format_results_returns_summary_fields_by_default(self) -> None:
        results = format_results(
            [
                {
                    "id": 1,
                    "spotter_call": "VE6AO",
                    "spotter_grid": "DO33",
                    "dx_call": "VE6AMR",
                    "dx_grid": "DO32",
                    "frequency_khz": 14074.0,
                    "mode": "FT8",
                    "snr_db": -12,
                    "spotted_at": "2026-05-10T20:47:00+00:00",
                    "raw_line": "raw",
                }
            ],
            detail=False,
        )

        self.assertEqual(
            results,
            [
                {
                    "spotter_call": "VE6AO",
                    "spotter_grid": "DO33",
                    "dx_call": "VE6AMR",
                    "dx_grid": "DO32",
                    "frequency_khz": 14074.0,
                    "mode": "FT8",
                    "snr_db": -12,
                    "spotted_at": "2026-05-10T20:47:00+00:00",
                }
            ],
        )

    def test_format_results_returns_all_fields_when_detail_true(self) -> None:
        rows = [{"spotter_call": "VE6AO", "id": 1}]
        self.assertEqual(format_results(rows, detail=True), rows)

    def test_encode_response_serializes_json_line(self) -> None:
        payload = encode_response("ok", {"results": [{"dx_call": "VE6AMR"}]})
        self.assertEqual(
            payload,
            b'{"status":"ok","version":"0.3.7","results":[{"dx_call":"VE6AMR"}]}\n',
        )

    def test_qrz_callsign_fields_are_mapped(self) -> None:
        xml_root = ET.fromstring(
            """
            <QRZDatabase version="1.34">
              <Callsign>
                <call>AA7BQ</call>
                <aliases>N6UFT,KJ6RK</aliases>
                <dxcc>291</dxcc>
                <fname>FRED L</fname>
                <name>LLOYD</name>
                <addr1>8711 E PINNACLE PEAK RD 193</addr1>
                <addr2>SCOTTSDALE</addr2>
                <state>AZ</state>
                <zip>85255</zip>
                <country>United States</country>
                <lat>34.23456</lat>
                <lon>-112.34356</lon>
                <grid>DM32af</grid>
                <county>Maricopa</county>
                <ccode>291</ccode>
                <fips>04013</fips>
                <land>USA</land>
                <email>flloyd@example.com</email>
                <class>E</class>
                <AreaCode>602</AreaCode>
                <TimeZone>Mountain</TimeZone>
                <GMTOffset>-7</GMTOffset>
                <DST>N</DST>
                <cqzone>3</cqzone>
                <ituzone>2</ituzone>
                <iota>NA-001</iota>
                <geoloc>user</geoloc>
              </Callsign>
              <Session><Key>abc</Key></Session>
            </QRZDatabase>
            """
        )
        client = QrzClient.__new__(QrzClient)
        mapped = {}
        for child in xml_root.find("Callsign"):
            from rbn_spot_collector.qrz import CALLSIGN_FIELDS, XML_TO_DB_FIELD

            db_field = XML_TO_DB_FIELD.get(child.tag, child.tag.lower())
            if db_field in CALLSIGN_FIELDS:
                mapped[db_field] = child.text

        self.assertEqual(mapped["callsign"], "AA7BQ")
        self.assertEqual(mapped["areacode"], "602")
        self.assertEqual(mapped["timezone"], "Mountain")
        self.assertEqual(mapped["gmtoffset"], "-7")
        self.assertEqual(mapped["geoloc"], "user")

    def test_qrz_requests_session_code_before_lookup(self) -> None:
        client = QrzClient.__new__(QrzClient)
        client.settings = Mock()
        client.settings.qrz_username = "demo"
        client.settings.qrz_password = "secret"
        client.settings.qrz_agent = "my-agent"
        client.settings.socket_timeout_seconds = 30
        client.session_key = None
        client.enabled = Mock(return_value=True)
        client._fetch_xml = Mock(
            side_effect=[
                ET.fromstring(
                    """
                    <QRZDatabase version="1.34">
                      <Session><Key>session-123</Key></Session>
                    </QRZDatabase>
                    """
                ),
                ET.fromstring(
                    """
                    <QRZDatabase version="1.34">
                      <Callsign><call>VE6AMR</call></Callsign>
                      <Session><Key>session-123</Key></Session>
                    </QRZDatabase>
                    """
                ),
            ]
        )

        result = client.lookup_callsign("VE6AMR-9")

        self.assertEqual(client.session_key, "session-123")
        self.assertEqual(result, {"callsign": "VE6AMR"})
        first_call = client._fetch_xml.call_args_list[0].args[0]
        second_call = client._fetch_xml.call_args_list[1].args[0]
        self.assertIn("username", first_call)
        self.assertIn("password", first_call)
        self.assertEqual(first_call["agent"], "my-agent")
        self.assertEqual(second_call, {"s": "session-123", "callsign": "VE6AMR"})

        client._fetch_xml.reset_mock(
            side_effect=True,
            return_value=True,
        )
        client._fetch_xml.side_effect = [
            ET.fromstring(
                """
                <QRZDatabase version="1.34">
                  <Callsign><call>VE6AMR</call></Callsign>
                  <Session><Key>session-123</Key></Session>
                </QRZDatabase>
                """
            )
        ]

        client.lookup_callsign("VE6AMR/P")

        slash_lookup_call = client._fetch_xml.call_args_list[0].args[0]
        self.assertEqual(slash_lookup_call, {"s": "session-123", "callsign": "VE6AMR"})

    def test_qrz_handles_namespaced_session_node(self) -> None:
        client = QrzClient.__new__(QrzClient)
        client.settings = Mock()
        client.settings.qrz_username = "demo"
        client.settings.qrz_password = "secret"
        client.settings.qrz_agent = "my-agent"
        client.settings.socket_timeout_seconds = 30
        client._fetch_xml = Mock(
            return_value=ET.fromstring(
                """
                <QRZDatabase version="1.34" xmlns="http://xmldata.qrz.com">
                  <Session>
                    <Key>session-456</Key>
                  </Session>
                </QRZDatabase>
                """
            )
        )

        session_code = client.request_session_code()

        self.assertEqual(session_code, "session-456")

    def test_parse_request_payload_accepts_false_detail(self) -> None:
        _, _, _, _, detail = parse_request_payload(
            b'{"field":"bycall","data":"VE6AMR","maxage":3600,"detail":false}'
        )
        self.assertFalse(detail)

    def test_callsign_lookup_service_negative_caches_failed_lookup(self) -> None:
        settings = Mock()
        settings.callsign_expiry_seconds = 86400
        settings.callsign_nack_seconds = 3600
        settings.qrz_username = "demo"
        settings.qrz_password = "secret"
        settings.qrz_agent = "agent"
        settings.socket_timeout_seconds = 30

        database = Mock()
        database.callsign_needs_refresh.return_value = True
        service = CallsignLookupService(settings, database)
        service.qrz_client.lookup_callsign = Mock(side_effect=RuntimeError("boom"))

        service.ensure_callsign("VE6AMR")

        database.upsert_callsign.assert_called_once_with({"callsign": "VE6AMR"})

    def test_callsign_lookup_service_negative_caches_empty_lookup(self) -> None:
        settings = Mock()
        settings.callsign_expiry_seconds = 86400
        settings.callsign_nack_seconds = 3600
        settings.qrz_username = "demo"
        settings.qrz_password = "secret"
        settings.qrz_agent = "agent"
        settings.socket_timeout_seconds = 30

        database = Mock()
        database.callsign_needs_refresh.return_value = True
        service = CallsignLookupService(settings, database)
        service.qrz_client.lookup_callsign = Mock(return_value=None)

        service.ensure_callsign("VE6AMR")

        database.upsert_callsign.assert_called_once_with({"callsign": "VE6AMR"})


if __name__ == "__main__":
    unittest.main()
