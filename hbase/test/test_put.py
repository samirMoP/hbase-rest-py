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
        self.client = HBaseRESTClient(hosts_list=["http://localhost:8080"])
        self.admin = HBaseAdmin(client=self.client)
        self.put = Put(client=self.client)
        self.get = Get(client=self.client)
        self.delete = Delete(client=self.client)

    def tearDown(self):
        self.client.session.close()

    def test_put(self):
        self.admin.table_create_or_update(
            table_name="test_tbl", params_list=[{"name": "cf"}]
        )

        self.put.put(
            tbl_name="test_tbl",
            row_key="samir@example.com",
            cell_map={"cf:amount": 250.511234},
        )

        self.put.put("test_tbl", "samir@example.com", {"cf:gender": "F"})
        self.put.put(
            "test_tbl", "samir@example.com", {"cf:countries": ["US", "BA", "DE", "SE"]}
        )
        self.put.put("test_tbl", "samir@example.com", {"cf:id": 13455})

        # Test int value put/get
        cf_id = self.get.get("test_tbl", "samir@example.com", "cf:id")
        byte_value = cf_id.get("row")[0]["cell"][0]["$"]
        assert struct.unpack(">q", byte_value)[0] == 13455

        # Test float value put/get
        cf_amount = self.get.get("test_tbl", "samir@example.com", "cf:amount")
        byte_value = cf_amount.get("row")[0]["cell"][0]["$"]
        assert struct.unpack(">d", byte_value)[0] == 250.511234

        # Test string value put/get
        cf_gender = self.get.get("test_tbl", "samir@example.com", "cf:gender")
        assert cf_gender.get("row")[0]["cell"][0]["$"].decode("utf-8") == "F"

        # Test python complex type: list
        cf_countries = self.get.get("test_tbl", "samir@example.com", "cf:countries")
        byte_value = cf_countries.get("row")[0]["cell"][0]["$"]
        assert pickle.loads(byte_value) == ["US", "BA", "DE", "SE"]

        # Test multiple cf put
        self.put.put(
            tbl_name="test_tbl",
            row_key="test@example.com",
            cell_map={
                "cf:gender": "M",
                "cf:amount": 34.567,
                "cf:countries": ["US", "BA", "DE", "SE"],
                "cf:id": 4567890,
            },
        )

        # Test int value put/get
        cf_id = self.get.get("test_tbl", "test@example.com", "cf:id")
        byte_value = cf_id.get("row")[0]["cell"][0]["$"]
        assert struct.unpack(">q", byte_value)[0] == 4567890

        # Test float value put/get
        cf_amount = self.get.get("test_tbl", "test@example.com", "cf:amount")
        byte_value = cf_amount.get("row")[0]["cell"][0]["$"]
        assert struct.unpack(">d", byte_value)[0] == 34.567

        # Test string value put/get
        cf_gender = self.get.get("test_tbl", "test@example.com", "cf:gender")
        assert cf_gender.get("row")[0]["cell"][0]["$"].decode("utf-8") == "M"

        # Test python complex type: list
        cf_countries = self.get.get("test_tbl", "test@example.com", "cf:countries")
        byte_value = cf_countries.get("row")[0]["cell"][0]["$"]
        assert pickle.loads(byte_value) == ["US", "BA", "DE", "SE"]

        # Test put with explicit timestamp
        status = self.put.put(
            tbl_name="test_tbl",
            row_key="timestamp_test",
            cell_map={
                "cf:gender": "M",
                "cf:amount": 34.567,
                "cf:countries": ["US", "BA", "DE", "SE"],
                "cf:id": 4567890,
            },
            timestamp=5555,
        )
        data = self.get.get("test_tbl", "timestamp_test")
        timestamp = data["row"][0]["cell"][0]["timestamp"]
        self.assertEqual(timestamp, 5555)

    def test_bulk_put_raw(self):
        p = {
            "Row": [
                {
                    "key": b64_encoder("timestamp6"),
                    "Cell": [
                        {
                            "column": b64_encoder("cf:t"),
                            "timestamp": round(time() * 1000),
                            "$": b64_encoder("a6"),
                        }
                    ],
                },
                {
                    "key": b64_encoder("timestamp7"),
                    "Cell": [
                        {
                            "column": b64_encoder("cf:t"),
                            "timestamp": round(time() * 1000),
                            "$": b64_encoder("a6"),
                        }
                    ],
                },
            ]
        }
        self.client.send_request(
            method="PUT",
            url_suffix="/test_tbl/fakekey",
            payload=p,
        )

    def test_build_bulk_payload(self):
        test_data = [("timestamp77", {"cf:col1": "value1"})]
        paylod = self.put._build_bulk_payload(test_data)
        paylod3 = self.put._build_payload(
            row_key="timestamp77", cell_map={"cf:col1": "value1"}
        )
        self.assertEqual(paylod, paylod3)
        self.assertTrue(isinstance(paylod["Row"], list))
        self.assertTrue(isinstance(json.dumps(paylod), str))

    def test_bulk_put(self):
        self.admin.table_create_or_update(
            table_name="test_tbl", params_list=[{"name": "cf"}]
        )
        bulk_payload = [
            ("row_key1", {"cf:col1": "value1", "cf:col2": "value2"}),
            ("row_key2", {"cf:col1": "value1", "cf:col2": "value2"}),
        ]
        self.put.bulk("test_tbl", rows=bulk_payload, timestamp=round(time() * 1000))
        data = self.get.get("test_tbl", "row_key2", "cf:col1")
        self.assertEqual(data["row"][0]["cell"][0]["$"], b"value1")

        bulk_payload2 = [
            ("test_bulk1", {"cf:age": 28, "cf:gender": "M", "cf:amount": 2500.45}),
            ("test_bulk2", {"cf:age": 29, "cf:gender": "F", "cf:amount": 2501.45}),
            ("test_bulk3", {"cf:age": 30, "cf:gender": "M", "cf:amount": 2502.45}),
            ("test_bulk4", {"cf:age": 30, "cf:gender": "M", "cf:amount": 2502.45}),
            ("test_bulk5", {"cf:age": 30, "cf:gender": "M", "cf:amount": 2502.45}),
            ("test_bulk6", {"cf:age": 30, "cf:gender": "M", "cf:amount": 2502.45}),
            ("test_bulk7", {"cf:age": 30, "cf:gender": "M", "cf:amount": 2502.45}),
            ("test_bulk8", {"cf:age": 30, "cf:gender": "M", "cf:amount": 2502.45}),
            ("test_bulk9", {"cf:age": 30, "cf:gender": "M", "cf:amount": 2502.45}),
            ("test_bulk10", {"cf:age": 30, "cf:gender": "M", "cf:amount": 2502.45}),
        ]
        self.put.bulk("test_tbl", rows=bulk_payload2)
        data = self.get.get("test_tbl", "test_bulk7", "cf:gender")
        self.assertEqual(data["row"][0]["cell"][0]["$"], b"M")
