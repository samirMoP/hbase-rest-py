import json
from unittest import TestCase, skip
from base64 import b64decode, b64encode
from time import time
import pickle
from hbase.utils import b64_encoder
from hbase.admin import HBaseAdmin
from hbase.rest_client import HBaseRESTClient


class TestAdmin(TestCase):
    def setUp(self):
        self.client = HBaseRESTClient(hosts_list=['http://localhost:8080'])
        self.admin = HBaseAdmin(client=self.client)

    def tearDown(self):
        self.client.session.close()

    def test_cluster_status(self):
        result = self.admin.cluster_status()
        assert isinstance(result, str)
        result_dict = json.loads(result)
        assert "LiveNodes" in result
        assert isinstance(result_dict.get("LiveNodes"), list)

    def test_cluster_version(self):
        result=self.admin.cluster_version()
        assert "Version" in json.loads(result)

    def test_namespaces(self):
        result=self.admin.namespaces()
        result_dict = json.loads(result)
        assert "Namespace" in json.loads(result)
        assert "default" in result_dict.get("Namespace")

    def test_namespace(self):
        result = self.admin.namespace(namespace_name="default")
        assert isinstance(json.loads(result), dict)

    def test_create_namespace(self):
        self.admin.namespace_create(namespace_name="new_namespace3")
        result = self.admin.namespaces()
        result_dict = json.loads(result)
        assert "new_namespace3" in result_dict.get("Namespace")

    def test_namespace_delete(self):
        self.admin.namespace_create(namespace_name="new_namespace3")
        self.admin.namespace_delete(namespace_name="new_namespace3")
        result = self.admin.namespaces()
        result_dict = json.loads(result)
        assert "new_namespace3" not in result_dict.get("Namespace")

    def test_tables(self):
        result = self.admin.tables()
        result_dict = json.loads(result)
        assert "table" in result_dict
        assert isinstance(result_dict.get("table"), list)

    def test_table_schema(self):
        self.admin.table_create_or_update(table_name="test_tbl", params_list=[{"name":"cfTest"}])
        result = self.admin.table_schema(table_name='test_tbl')
        result_dict = json.loads(result)
        self.assertEqual(result_dict.get("name"), 'test_tbl')
        self.assertEqual(result_dict.get("ColumnSchema")[0]['name'], 'cfTest')

    def test_table_create_or_update(self):
        self.admin.table_create_or_update(
            table_name="test_tbl",
            params_list=[{"name": "cfTest", "BLOCKSIZE": "65537"}]
        )
        result = self.admin.table_schema(table_name='test_tbl')
        result_dict = json.loads(result)
        self.assertEqual(result_dict.get("name"), 'test_tbl')
        self.assertEqual(result_dict.get("ColumnSchema")[0]['BLOCKSIZE'], '65537')

    def test_table_regions(self):
        self.admin.table_create_or_update(
            table_name="test_tbl",
            params_list=[{"name": "cfTest", "BLOCKSIZE": "65537"}]
        )
        result = self.admin.table_regions(table_name="test_tbl")
        result_dict = json.loads(result)
        assert result_dict['name'] == 'test_tbl'
        assert isinstance(result_dict["Region"], list)

    def test_table_delete(self):
        self.admin.table_create_or_update(
            table_name="test_tbl2",
            params_list=[{"name": "cfTest", "BLOCKSIZE": "65537"}]
        )
        self.admin.table_delete(table_name="test_tbl2")
        result = self.admin.tables()
        result_dict = json.loads(result)
        tbls_list = [a['name'] for a in result_dict['table']]
        assert "test_tbl2" not in tbls_list





