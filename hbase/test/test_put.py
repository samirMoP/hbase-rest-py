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


class TestPut(TestCase):
    def setUp(self):
        self.client = HBaseRESTClient(hosts_list=['http://localhost:8080'])
        self.admin = HBaseAdmin(client=self.client)
        self.put = Put(client=self.client)
        self.get = Get(client=self.client)
        self.delete = Delete(client=self.client)


    def tearDown(self):
        self.client.session.close()

    def test_put(self):
        self.admin.table_create_or_update(
            table_name="test_tbl",
            params_list=[{"name": "cf"}]
        )

        self.put.put(
            tbl_name='test_tbl',
            row_key="samir@example.com",
            column_family="cf:amount",
            value=250.511234
        )

        self.put.put('test_tbl', "samir@example.com", "cf:gender", "F")
        self.put.put('test_tbl', "samir@example.com", "cf:countries", ["US", "BA", "DE", "SE"])
        self.put.put('test_tbl', "samir@example.com", "cf:id", 13455)

        # Test int value put/get
        cf_id = self.get.get("test_tbl", "samir@example.com", "cf:id")
        print("***************")
        print(cf_id)
        byte_value = cf_id.get('row')[0]['cell'][0]['$']
        assert struct.unpack(">q", byte_value)[0] == 13455

        # Test float value put/get
        cf_amount = self.get.get("test_tbl", "samir@example.com", "cf:amount")
        byte_value = cf_amount.get('row')[0]['cell'][0]['$']
        assert struct.unpack(">d", byte_value)[0] == 250.511234

        # Test string value put/get
        cf_gender = self.get.get("test_tbl", "samir@example.com", "cf:gender")
        assert cf_gender.get('row')[0]['cell'][0]['$'].decode('utf-8') == 'F'

        # Test python complex type: list
        cf_countries = self.get.get("test_tbl", "samir@example.com", "cf:countries")
        byte_value = cf_countries.get('row')[0]['cell'][0]['$']
        assert pickle.loads(byte_value) == ["US", "BA", "DE", "SE"]

        # Test multiple cf put
        self.put.put_multiple_cf(tbl_name='test_tbl',
                                 row_key="test@example.com",
                                 cf_value_map= {"cf:gender": "M",
                                                "cf:amount": 34.567,
                                                "cf:countries": ["US", "BA", "DE", "SE"],
                                                "cf:id":4567890})

        # Test int value put/get
        cf_id = self.get.get("test_tbl", "test@example.com", "cf:id")
        byte_value = cf_id.get('row')[0]['cell'][0]['$']
        assert struct.unpack(">q", byte_value)[0] == 4567890

        # Test float value put/get
        cf_amount = self.get.get("test_tbl", "test@example.com", "cf:amount")
        byte_value = cf_amount.get('row')[0]['cell'][0]['$']
        assert struct.unpack(">d", byte_value)[0] == 34.567

        # Test string value put/get
        cf_gender = self.get.get("test_tbl", "test@example.com", "cf:gender")
        assert cf_gender.get('row')[0]['cell'][0]['$'].decode('utf-8') == 'M'

        # Test python complex type: list
        cf_countries = self.get.get("test_tbl", "test@example.com", "cf:countries")
        byte_value = cf_countries.get('row')[0]['cell'][0]['$']
        assert pickle.loads(byte_value) == ["US", "BA", "DE", "SE"]


        print(self.delete.delete("test_tbl", "test@example.com", "cf:countries"))





