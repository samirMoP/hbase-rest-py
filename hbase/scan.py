import json
from hbase.utils import result_parser


class Scan(object):

    def __init__(self, client):
        self.client = client
        self.scanner = None

    @staticmethod
    def _build_url_suffix(tbl_name):
        return f"/{tbl_name}/scanner"

    def _get_scanner(self, tbl_name, scanner_payload='<Scanner batch="1000"/>'):
        hbase_host = self.client.get_hbase_host()
        url_suffix = self._build_url_suffix(tbl_name)
        response = self.client.session.put(
            url=f"{hbase_host}{url_suffix}",
            data=scanner_payload,
            headers={"Content-Type":"text/xml", "accept":"text/xml"}
        )
        if response.status_code == 201:
            self.scanner = response.headers.get("Location")
            return self.scanner
        return {'error': response.content, 'status_code':response.status_code}

    def scan_next(self):
        response = self.client.session.get(
            url=self.scanner,
            headers=self.client.headers,
        )
        if response.status_code == 204:
            self.delete_scanner()
            self.scanner = None
            return False, None
        return True, result_parser(response.json()) if response.content is not None else None

    def scan(self, tbl_name, scanner_payload='<Scanner batch="1000"/>'):
        scan_url = None
        if self.scanner is None:
            scan_url = self._get_scanner(tbl_name, scanner_payload)
        if isinstance(scan_url, dict):
            return scan_url
        return  self.scan_next()

    def delete_scanner(self):
        response = self.client.session.delete(
            url=self.scanner,
            headers=self.client.headers
        )
        return response.status_code


