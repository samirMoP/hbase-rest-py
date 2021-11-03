
class Delete(object):
    def __init__(self, client):
        self.client = client

    @staticmethod
    def _build_url_suffix(tbl_name, row_key, column_family=None):
        if column_family is not None:
            return f"/{tbl_name}/{row_key}/{column_family}"
        return f"/{tbl_name}/{row_key}"

    def delete(self, tbl_name, row_key, column_family=None):
        return self.client.send_request(
            method="DELETE",
            url_suffix=self._build_url_suffix(tbl_name, row_key, column_family)
        )
