import json
from hbase.utils import result_parser


class Get(object):

    def __init__(self, client):
        self.client = client

    @staticmethod
    def _build_url_suffix(tbl_name, row_key, column_family=None, timestamp=None, number_of_versions=None):
        if timestamp is not None:
            return f"/{tbl_name}/{row_key}/{column_family}/{timestamp}"
        if number_of_versions is not None:
            return f"/{tbl_name}/{row_key}/{column_family}/?v={number_of_versions}"
        if column_family is not None:
            return f"/{tbl_name}/{row_key}/{column_family}"
        return f"/{tbl_name}/{row_key}"

    def get(self, tbl_name, row_key, column_family=None, timestamp=None, number_of_versions=None):
        result = self.client.send_request(
            method="GET",
            url_suffix=self._build_url_suffix(tbl_name, row_key, column_family, timestamp,number_of_versions)
        )
        if isinstance(result, dict):
            return result
        return result_parser(json.loads(result))
