import requests
from requests import Session
import json
from unittest import TestCase, skip
from base64 import b64decode, b64encode
from time import time
import pickle
from hbase.utils import b64_encoder

class TestOne(TestCase):

    def setUp(self):
        self.hbase_rest_server='localhost'
        self.hbase_rest_server_port=8080
        self.header = {"accept":"application/json", "content-type": "application/json"}
        self.namespace = "default"
        self.test_table = "users"
        self.session = Session()

    def test_simple(self):
        a = 20
        b = 30
        self.assertEqual(a + b, 50)

    def test_cluster_version(self):
        url = f"http://{self.hbase_rest_server}:{self.hbase_rest_server_port}/version/cluster"
        response = self.session.get(url=url, headers=self.header)
        print(type(response.content))
        print(response.content.decode('utf-8'))
        print(response.text)
        print(f"Headers {response.headers}")

    def test_cluster_status_call(self):
        url = f"http://{self.hbase_rest_server}:{self.hbase_rest_server_port}/status/cluster"
        response = self.session.get(url=url, headers = self.header)
        print(type(response.text))
        print(response.content.decode('utf-8'))

    def test_list_namespaces(self):
        url = f"http://{self.hbase_rest_server}:{self.hbase_rest_server_port}/namespaces"
        response = self.session.get(url=url, headers=self.header)
        print(type(response.content))
        decoded = response.content.decode('utf-8')
        print(response.content.decode('utf-8'))
        print(type(decoded))

    def test_list_tables(self):
        url = f"http://{self.hbase_rest_server}:{self.hbase_rest_server_port}/namespaces/{self.namespace}/tables"
        response = self.session.get(url=url, headers=self.header)
        print(response.json())

    def test_get_table_schema(self):
        url = f"http://{self.hbase_rest_server}:{self.hbase_rest_server_port}/{self.test_table}/schema"
        response = self.session.get(url=url, headers=self.header)
        print(response.content)

    def test_table_update_schema(self):
        url = f"http://{self.hbase_rest_server}:{self.hbase_rest_server_port}/{self.test_table}/schema"
        payload = {
            "name": "users",
            "ColumnSchema": [{
                "name": "cfInfo",
                "BLOCKSIZE": "65536"
            }
            ]
        }
        j_payload = json.dumps(payload)
        response = self.session.post(url=url, data=j_payload, headers=self.header)
        print(response.content)

    def test_create_table(self):
        new_table = "py_test"
        url = f"http://{self.hbase_rest_server}:{self.hbase_rest_server_port}/{new_table}/schema"
        payload = {
            "name": new_table,
            "ColumnSchema": [{
                "name": "cfInfo",
                "BLOCKSIZE": "65536"
            }
            ]
        }
        j_payload = json.dumps(payload)
        response = self.session.put(url=url, data=j_payload, headers=self.header)
        print(response.content)

    @skip('To speed up testing')
    def test_delete_table(self):
        # Create/update table
        new_table = "py_test"
        url = f"http://{self.hbase_rest_server}:{self.hbase_rest_server_port}/{new_table}/schema"
        payload = {
            "name": new_table,
            "ColumnSchema": [{
                "name": "cfInfo",
                "BLOCKSIZE": "65536"
            }
            ]
        }
        j_payload = json.dumps(payload)
        response = requests.put(url=url, data=j_payload, headers=self.header)
        print(response.status_code)
        assert response.status_code == 201
        time.sleep(5)
        response = self.session.delete(f"http://{self.hbase_rest_server}:{self.hbase_rest_server_port}/{new_table}/schema")
        assert response.status_code == 200
        print(response.status_code)

    def test_list_table_regions(self):
        url = f"http://{self.hbase_rest_server}:{self.hbase_rest_server_port}/{self.test_table}/regions"
        response = self.session.get(url=url, headers=self.header)
        print(response.content)
        assert response.status_code == 200

    def test_put_and_get(self):
        row = b64encode("samir@example.com".encode("utf-8")).decode('utf-8')
        cf_q = b64encode("cfInfo:age".encode("utf-8")).decode("utf-8")
        cf_q2 = b64encode("cfInfo:sex".encode("utf-8")).decode("utf-8")
        value = b64encode("16".encode("utf-8")).decode("utf-8")
        value2 = b64encode("M".encode("utf-8")).decode("utf-8")
        payload = {"Row":[{"key":row, "Cell": [{"column":cf_q, "$":value},{"column":cf_q2, "$":value2}]}]}

        url = f"http://{self.hbase_rest_server}:{self.hbase_rest_server_port}/{self.test_table}/samir@example.com"
        response = self.session.put(url=url, data=json.dumps(payload), headers=self.header)
        print(response.status_code)

        # HBase Get
        response = requests.get(url=url, headers=self.header)
        print("** Get row  **")
        print(response.json())

        # Get value single column
        url = f"http://{self.hbase_rest_server}:{self.hbase_rest_server_port}/{self.test_table}/samir@example.com/cfInfo:age"
        response = self.session.get(url=url, headers=self.header)
        print(response.json())

    def test_scan(self):
        #self._data_generator(total_rows=1000)
        url = f"http://{self.hbase_rest_server}:{self.hbase_rest_server_port}/{self.test_table}/scanner/"
        payload = {"batch":20}
        response = self.session.put(url=url, data=json.dumps(payload), headers=self.header)
        scan_url = response.headers.get("Location")
        data = self.session.get(url=scan_url, headers=self.header)
        parsed = self._result_parser(data.json())
        print(parsed)
        assert len(parsed['row']) == len(data.json()['Row'])

    def _parse_value(self, value, value_type):
        if isinstance(value,  bytes) and value_type == int:
            return int.from_bytes(value,  "big")


    def _result_parser(self, json_result):
        row = json_result["Row"]
        result = []
        key = {}
        cells = []
        start = time()
        for k in row:
            key["key"] = b64decode(k['key'])
            cell = k['Cell']
            for i in cell:
                cells.append({'column': b64decode(i['column']),
                              'timestamp': i['timestamp'],
                              "$": b64decode(i['$'])})
            key['cell'] = cells
            result.append(key)
            cells=[]
            key = {}
        end = time()
        diff = end - start
        print(f"{diff} execution time")
        return {"row": result}

    def _data_generator(self, total_rows):
        cf_q = b64encode("cfInfo:age".encode("utf-8")).decode("utf-8")
        cf_q2 = b64encode("cfInfo:sex".encode("utf-8")).decode("utf-8")
        for i in range(1, total_rows):
            key = b64encode(f"samir@example.com#{i}".encode("utf-8")).decode('utf-8')
            value = b64encode(f"16{i}".encode("utf-8")).decode("utf-8")
            value2 = b64encode("M".encode("utf-8")).decode("utf-8")
            row = {"Row":[{"key":key, "Cell": [{"column":cf_q, "$":value},{"column":cf_q2, "$":value2}]}]}
            url = f"http://{self.hbase_rest_server}:{self.hbase_rest_server_port}/{self.test_table}/samir@example.com#{i}"
            self.session.put(url=url, data=json.dumps(row), headers=self.header)

    def test_put_type(self):
        key = b64_encoder("test_key")
        cf_int = b64_encoder("cfInfo:age")
        cf_float = b64_encoder("cfInfo:salary")
        cf_list = b64_encoder("cfInfo:countries")
        float_value = b'1500.55'
        float_value_b64 = b64_encoder(float_value)
        t_list = ['CA', 'US',  'SE']
        list_encoded = b64_encoder(t_list)

        int_value_b64 = b64_encoder(12)
        row = {"Row": [{"key": key, "Cell": [{"column": cf_int, "$": int_value_b64},
                                             {"column": cf_float, "$": float_value_b64},
                                             {"column": cf_list, "$": list_encoded}]}]}
        url = f"http://{self.hbase_rest_server}:{self.hbase_rest_server_port}/{self.test_table}/test_int/?check=put"
        self.session.put(url=url, data=json.dumps(row), headers=self.header)

        #Get data (data is sorted by alphabetic order per cf:q descriptot)
        data = self.session.get(url=url, headers=self.header)
        parsed = self._result_parser(data.json())
        print(parsed)
        value = parsed['row'][0]['cell'][0]['$']
        f_value = parsed['row'][0]['cell'][2]['$']
        list_value = parsed['row'][0]['cell'][1]['$']
        print(list_value)
        #print(type(list_value))
        print(float_value)
        #print(type(float_value))
        print(float(f_value))
        #print(type(value))
        print(int.from_bytes(value, "big"))
        print(pickle.loads(list_value))


        data = self.session.get(url=url+"/cfInfo:countries", headers=self.header)
        print(self._result_parser(data.json()))





#if __name__ == '__main__':
#    unittest.main()