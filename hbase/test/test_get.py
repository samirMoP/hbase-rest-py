import time
from unittest import TestCase
from hbase.admin import HBaseAdmin
from hbase.rest_client import HBaseRESTClient
from hbase.put import Put
from hbase.get import Get
from hbase.utils import Bytes


class TestGet(TestCase):
    @classmethod
    def setUpClass(cls):
        client = HBaseRESTClient(hosts_list=["http://localhost:8080"])
        admin = HBaseAdmin(client)
        put = Put(client=client)

        admin.table_create_or_update("test_get", [{"name": "cf", "VERSIONS": 3}])
        for i in range(1, 101):
            put.put(
                tbl_name="test_get",
                row_key=f"row-{i}",
                cell_map={"cf:age": i, "cf:amount": 34.56, "cf:name": "John"},
            )
        client.session.close()

    @classmethod
    def tearDownClass(cls):
        client = HBaseRESTClient(hosts_list=["http://localhost:8080"])
        admin = HBaseAdmin(client)
        admin.table_delete("test_get")
        client.session.close()

    def setUp(self):
        self.client = HBaseRESTClient(hosts_list=["http://localhost:8080"])
        self.admin = HBaseAdmin(client=self.client)
        self.put = Put(client=self.client)
        self.get = Get(client=self.client)

    def tearDown(self):
        self.client.session.close()

    def test_get(self):
        result = self.get.get(
            tbl_name="test_get",
            row_key="row-5",
        )
        value_int = Bytes.to_int(result["row"][0]["cell"][0]["$"])
        self.assertEqual(value_int, 5)
        value_float = Bytes.to_float(result["row"][0]["cell"][1]["$"])
        self.assertEqual(value_float, 34.56)
        string_value = Bytes.to_string(result["row"][0]["cell"][2]["$"])
        self.assertEqual(string_value, "John")

        # Add more recent version for "row-1"
        self.put.put(
            tbl_name="test_get",
            row_key="row-1",
            cell_map={"cf:age": 50, "cf:amount": 50.1, "cf:name": "JohnNew"},
        )
        timestamp = round(time.time() * 1000) - 200
        result_2_versions = self.get.get(
            "test_get", "row-1", "cf:name", number_of_versions=3
        )
        result_timestamp = self.get.get(
            "test_get", "row-1", "cf:name", timestamp=timestamp
        )
        name = Bytes.to_string(result_2_versions["row"][0]["cell"][0]["$"])
        self.assertEqual(name, "JohnNew")
        self.assertEqual(
            Bytes.to_string(result_timestamp["row"][0]["cell"][0]["$"]), "John"
        )
