import json
import responses
from unittest import TestCase
from mock import patch
from hbase.rest_client import HBaseRESTClient


class TestRESTClient(TestCase):
    def setUp(self):
        self.client = HBaseRESTClient(hosts_list=["http://localhost:8080"])

    def tearDown(self):
        self.client.session.close()

    def test_get_hbase_host(self):
        assert self.client.get_hbase_host() == "http://localhost:8080"

    @patch("requests.Session.get")
    def test_GET_method(self, mocked_fnc):
        self.client.send_request("GET", "/test")
        mocked_fnc.assert_called_with(
            url="http://localhost:8080/test",
            headers={"accept": "application/json", "content-type": "application/json"},
            timeout=10,
        )

    @patch("requests.Session.post")
    def test_POST_method(self, mocked_fnc):
        self.client.send_request("POST", "/test", {"a": 1})
        mocked_fnc.assert_called_with(
            url="http://localhost:8080/test",
            data='{"a": 1}',
            headers={"accept": "application/json", "content-type": "application/json"},
            timeout=10,
        )

    @patch("requests.Session.put")
    def test_PUT_method(self, mocked_fnc):
        self.client.send_request("PUT", "/test", {"a": 1})
        mocked_fnc.assert_called_with(
            url="http://localhost:8080/test",
            data='{"a": 1}',
            headers={"accept": "application/json", "content-type": "application/json"},
            timeout=10,
        )

    @patch("requests.Session.delete")
    def test_DELETE_method(self, mocked_fnc):
        self.client.send_request("DELETE", "/test")
        mocked_fnc.assert_called_with(
            url="http://localhost:8080/test",
            headers={"accept": "application/json", "content-type": "application/json"},
            timeout=10,
        )

    @responses.activate
    def test_send_request_success(self):
        expected = {"table": [{"name": "message"}, {"name": "new_table"}]}
        responses.add(
            responses.GET,
            "http://localhost:8080/namespaces/default/tables",
            json=expected,
            status=200,
        )
        result = self.client.send_request(
            method="GET", url_suffix="/namespaces/default/tables"
        )
        assert json.loads(result) == expected

    @responses.activate
    def test_send_request_failedd(self):
        payload = "Bad request"
        responses.add(
            responses.GET,
            "http://localhost:8080/namespaces/default/tables",
            json=payload,
            status=400,
        )
        result = self.client.send_request(
            method="GET", url_suffix="/namespaces/default/tables"
        )
        assert result == {"error": '"Bad request"', "status_code": 400}
