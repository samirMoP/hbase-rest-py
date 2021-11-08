from hbase.utils import b64_encoder


class Put(object):
    def __init__(self, client):
        self.client = client

    @staticmethod
    def _build_url_suffix(tbl_name, row_key):
        return f"/{tbl_name}/{row_key}"

    @staticmethod
    def _build_payload(row_key, column_family, value):
        return {
            "Row": [
                {
                    "key": b64_encoder(row_key),
                    "Cell": [
                        {"column": b64_encoder(column_family), "$": b64_encoder(value)}
                    ],
                }
            ]
        }

    @staticmethod
    def _build_payload_multipe_cf(row_key, cf_value_map):
        cf_value_list = []
        for key, value in cf_value_map.items():
            cf_value_list.append({"column": b64_encoder(key), "$": b64_encoder(value)})

        return {"Row": [{"key": b64_encoder(row_key), "Cell": cf_value_list}]}

    def put(self, tbl_name, row_key, column_family, value):
        return self.client.send_request(
            method="PUT",
            url_suffix=self._build_url_suffix(tbl_name, row_key),
            payload=self._build_payload(row_key, column_family, value),
        )

    def put_multiple_cf(self, tbl_name, row_key, cf_value_map):
        return self.client.send_request(
            method="PUT",
            url_suffix=self._build_url_suffix(tbl_name, row_key),
            payload=self._build_payload_multipe_cf(row_key, cf_value_map),
        )
