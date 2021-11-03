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


class TestDelete(TestCase):
    def setUp(self):
        self.client = HBaseRESTClient(hosts_list=['http://localhost:8080'])
        self.admin = HBaseAdmin(client=self.client)
        self.put = Put(client=self.client)
        self.get = Get(client=self.client)
        self.delete = Delete(client=self.client)

    def tearDown(self):
        self.client.session.close()

    def test_delete(self):
        self.admin.table_create_or_update(table_name='test_tbl',
                                          params_list=[{"name":"cf"}, {"name":"cf2"}])

        self.put.put('test_tbl', "samir@example.com", "cf:gender", "F")
        self.put.put('test_tbl', "samir@example.com", "cf:countries", ["US", "BA", "DE", "SE"])
        self.put.put('test_tbl', "samir@example.com", "cf:id", 13455)
        self.put.put('test_tbl', "samir@example.com", "cf2:name", "samir")

        self.delete.delete('test_tbl', "samir@example.com", "cf:gender")

        # Assert column not found after delete
        result = self.get.get('test_tbl', "samir@example.com", "cf:gender")
        assert result['status_code'] == 404

        # Delete column family
        self.delete.delete('test_tbl', "samir@example.com", 'cf2')
        result = self.get.get('test_tbl', "samir@example.com", "cf2:name")
        assert result['status_code'] == 404

        # Delete row
        print(self.delete.delete('test_tbl', "samir@example.com"))
        result = self.get.get('test_tbl', "samir@example.com")
        assert result['status_code'] == 404




