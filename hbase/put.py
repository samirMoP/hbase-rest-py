from hbase.utils import b64_encoder


class Put(object):
    def __init__(self, client):
        self.client = client

    @staticmethod
    def _build_url_suffix(tbl_name):
        return f"/{tbl_name}/fakerow"

    @staticmethod
    def _build_payload(row_key, cell_map):
        cf_value_list = []
        for key, value in cell_map.items():
            cf_value_list.append({"column": b64_encoder(key), "$": b64_encoder(value)})

        return {"Row": [{"key": b64_encoder(row_key), "Cell": cf_value_list}]}

    def put(self, tbl_name, row_key, cell_map):
        return self.client.send_request(
            method="PUT",
            url_suffix=self._build_url_suffix(tbl_name),
            payload=self._build_payload(row_key, cell_map),
        )
