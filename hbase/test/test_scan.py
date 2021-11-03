import json
from unittest import TestCase, skip
from base64 import b64decode, b64encode
from time import time
import struct
import pickle
from hbase.utils import b64_encoder
from hbase.admin import HBaseAdmin
from hbase.rest_client import HBaseRESTClient
from hbase.put import Put
from hbase.get import Get
from hbase.delete import Delete
from hbase.scan import Scan
import xml.etree.ElementTree as et
import xml
from hbase.scan_filter_helper import build_base_scanner, build_prefix_filter


class TestScan(TestCase):
    def setUp(self):
        self.client = HBaseRESTClient(hosts_list=['http://localhost:8080'])
        self.admin = HBaseAdmin(client=self.client)
        self.put = Put(client=self.client)
        self.get = Get(client=self.client)
        self.delete = Delete(client=self.client)
        self.scan= Scan(client=self.client)

    def tearDown(self):
        self.client.session.close()

    def test_scan_default(self):
        data = self.scan.scan('users')
        print(data)
        print(len(data['row']))

    def test_scan_with_params(self):
        scanner_def = build_base_scanner(
            batch=100,
            type='row',
            startRow="user1",
            endRow="user9"
        )
        data= self.scan.scan(
            tbl_name="users",
            scanner_payload=scanner_def
        )
        print(data)
        print(len(data['row']))

    def test_scan_prefix_filter(self):
        filter = build_prefix_filter(row_perfix="user9")
        data = self.scan.scan(
            tbl_name="users",
            scanner_payload=filter
        )
        print(data)
        print(len(data['row']))



    def test_xml_builder(self):
        xml_test = et.Element("Scanner", batch="100")
        filter = et.SubElement(xml_test, "filter")
        filter.text ='{"%s":%s, "%s":%s}'%("a", 1, "b", 2)
        print(et.tostring(xml_test))

    def test_xml_builder2(self):
        print(build_base_scanner(
            batch=100,
            type='row',
            startRow="user1",
            endRow="user9"
        ))

        print(build_prefix_filter(
            row_perfix="user1"
        ))
