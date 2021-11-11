from hbase.utils import b64_encoder


class Put(object):
    def __init__(self, client):
        self.client = client

    @staticmethod
    def _build_url_suffix(tbl_name):
        return f"/{tbl_name}/fakerow"

    @staticmethod
    def _build_payload(row_key, cell_map, timestamp=None):
        cf_value_list = []
        for key, value in cell_map.items():
            if timestamp is None:
                cf_value_list.append(
                    {"column": b64_encoder(key), "$": b64_encoder(value)}
                )
            else:
                cf_value_list.append(
                    {
                        "column": b64_encoder(key),
                        "timestamp": timestamp,
                        "$": b64_encoder(value),
                    }
                )

        return {"Row": [{"key": b64_encoder(row_key), "Cell": cf_value_list}]}

    def _build_bulk_payload(self, rows, timestamp=None):
        row_list = []
        for r in rows:
            row_key = r[0]
            cell_map = r[1]
            if timestamp is None:
                row_list.append(self._build_payload(row_key, cell_map)["Row"][0])
            else:
                row_list.append(
                    self._build_payload(row_key, cell_map, timestamp=timestamp)["Row"][
                        0
                    ]
                )
        return {"Row": row_list}

    def put(self, tbl_name, row_key, cell_map, timestamp=None):
        return self.client.send_request(
            method="PUT",
            url_suffix=self._build_url_suffix(tbl_name),
            payload=self._build_payload(row_key, cell_map, timestamp),
        )

    def bulk(self, tbl_name, rows, timestamp=None):
        return self.client.send_request(
            method="PUT",
            url_suffix=self._build_url_suffix(tbl_name),
            payload=self._build_bulk_payload(rows, timestamp),
        )
