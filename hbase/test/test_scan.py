import struct
import json
from unittest import TestCase
from hbase.admin import HBaseAdmin
from hbase.rest_client import HBaseRESTClient
from hbase.put import Put
from hbase.scan import Scan
from hbase.scan_filter_helper import (
    build_base_scanner,
    build_prefix_filter,
    build_row_filter,
    build_value_filter,
    build_single_column_value_filter,
)
from hbase.utils import result_parser


class TestScan(TestCase):
    @classmethod
    def setUpClass(cls):
        client = HBaseRESTClient(hosts_list=["http://localhost:8080"])
        admin = HBaseAdmin(client)
        put = Put(client=client)

        admin.table_create_or_update("test_scan", [{"name": "cf"}])
        for i in range(1, 101):
            put.put(
                tbl_name="test_scan",
                row_key=f"row-{i}",
                cell_map={
                    "cf:age": i,
                    "cf:email": f"ex{i}@grr.la",
                    "cf:text": "this is just text test",
                },
            )
        client.session.close()

    @classmethod
    def tearDownClass(cls):
        client = HBaseRESTClient(hosts_list=["http://localhost:8080"])
        admin = HBaseAdmin(client)
        admin.table_delete("test_scan")
        client.session.close()

    def setUp(self):
        self.client = HBaseRESTClient(hosts_list=["http://localhost:8080"])
        self.admin = HBaseAdmin(client=self.client)
        self.put = Put(client=self.client)
        self.scan = Scan(client=self.client)

    def tearDown(self):
        self.client.session.close()

    def test_scan_default(self):
        next, data = self.scan.scan("test_scan")
        self.assertEqual(len(data["row"]), 100)
        while next:
            next, data = self.scan.scan_next()
            assert next is False
            assert data is None
        assert self.scan.scanner is None

    def test_scan_with_params(self):
        scanner_def = build_base_scanner(
            startRow="row-9", endRow="row-99", column=["cf:email"]
        )
        _, data = self.scan.scan(tbl_name="test_scan", scanner_payload=scanner_def)
        self.assertEqual(len(data["row"]), 10)
        self.assertEqual(self.scan.delete_scanner(), 200)

    def test_scan_prefix_filter(self):
        filter = build_prefix_filter(row_perfix="row-8", column=["cf:email"])
        _, data = self.scan.scan(tbl_name="test_scan", scanner_payload=filter)
        self.assertEqual(len(data["row"]), 11)
        self.assertEqual(self.scan.delete_scanner(), 200)

    def test_scan_row_filter(self):
        filter = build_row_filter(
            row_value="row-1",
            operation="EQUAL",
        )
        _, data = self.scan.scan(tbl_name="test_scan", scanner_payload=filter)
        rows = data["row"]
        self.assertEqual(len(rows), 1)
        self.assertEqual(self.scan.delete_scanner(), 200)

    def test_scan_value_filter(self):
        filter = build_value_filter(
            value=10,
            operation="LESS",
        )
        _, data = self.scan.scan(tbl_name="test_scan", scanner_payload=filter)
        rows = data["row"]
        values = []
        for r in rows:
            values.append(struct.unpack(">q", r["cell"][0]["$"])[0])
        for v in values:
            assert v < 10
        self.assertEqual(self.scan.delete_scanner(), 200)

    def test_singe_column_value_filter(self):
        filter = build_single_column_value_filter(
            family="cf", qualifier="age", value=10, operation="LESS"
        )
        _, data = self.scan.scan(tbl_name="test_scan", scanner_payload=filter)
        rows = data["row"]
        self.assertEqual(len(rows), 9)
        values = []
        for r in rows:
            values.append(struct.unpack(">q", r["cell"][0]["$"])[0])
        for v in values:
            assert v < 10
        self.assertEqual(self.scan.delete_scanner(), 200)

    def test_table_scan(self):
        url_sufix = "/test_scan/*?column=cf:email&startrow=row-99&endrow=row-9&reversed=true"
        data = self.client.send_request(
            method="GET",
            url_suffix=url_sufix
        )
        result = result_parser(json.loads(data))
        self.assertEqual(result['row'][0]['key'], b'row-99')

    def test_value_filter_substring(self):
        filter = build_value_filter(
            value="this", operation="EQUAL", comparator="substring"
        )
        _, result = self.scan.scan(tbl_name="test_scan", scanner_payload=filter)
        self.assertEqual(len(result["row"]), 100)
