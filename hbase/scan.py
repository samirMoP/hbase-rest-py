import json
from hbase.utils import result_parser, b64_encoder

#{"op":"LESS","type":"RowFilter","comparator":{"value":"dGVzdFJvd09uZS0y","type":"BinaryComparator"}}
#{"op":"LESS_OR_EQUAL","type":"RowFilter","comparator":{"value":"dGVzdFJvd09uZS0y","type":"BinaryComparator"}}
#{"op":"NOT_EQUAL","type":"RowFilter","comparator":{"value":"dGVzdFJvd09uZS0y","type":"BinaryComparator"}}
#{"op":"GREATER_OR_EQUAL","type":"RowFilter","comparator":{"value":"dGVzdFJvd09uZS0y","type":"BinaryComparator"}}
#{"op":"GREATER","type":"RowFilter","comparator":{"value":"dGVzdFJvd09uZS0y","type":"BinaryComparator"}}
#{"op":"NOT_EQUAL","type":"RowFilter","comparator":{"value":"dGVzdFJvd09uZS0y","type":"BinaryComparator"}}
#{"op":"EQUAL","type":"RowFilter","comparator":{"value":".+-2","type":"RegexStringComparator"}}
#{"op":"EQUAL","type":"ValueFilter","comparator":{"value":"dGVzdFZhbHVlT25l","type":"BinaryComparator"}}
#{"op":"EQUAL","type":"ValueFilter","comparator":{"value":"dGVzdFZhbHVlVHdv","type":"BinaryComparator"}}
#{"op":"EQUAL","type":"ValueFilter","comparator":{"value":"testValue((One)|(Two))","type":"RegexStringComparator"}}
#{"op":"LESS","type":"ValueFilter","comparator":{"value":"dGVzdFZhbHVlVHdv","type":"BinaryComparator"}}
#{"op":"LESS_OR_EQUAL","type":"ValueFilter","comparator":{"value":"dGVzdFZhbHVlVHdv","type":"BinaryComparator"}}
#{"op":"LESS_OR_EQUAL","type":"ValueFilter","comparator":{"value":"dGVzdFZhbHVlT25l","type":"BinaryComparator"}}
#{"op":"NOT_EQUAL","type":"ValueFilter","comparator":{"value":"dGVzdFZhbHVlT25l","type":"BinaryComparator"}}
#{"op":"GREATER_OR_EQUAL","type":"ValueFilter","comparator":{"value":"dGVzdFZhbHVlT25l","type":"BinaryComparator"}}
#{"op":"GREATER","type":"ValueFilter","comparator":{"value":"dGVzdFZhbHVlT25l","type":"BinaryComparator"}}
#{"op":"NOT_EQUAL","type":"ValueFilter","comparator":{"value":"dGVzdFZhbHVlT25l","type":"BinaryComparator"}}
#{"type":"SkipFilter","filters":[{"op":"NOT_EQUAL","type":"QualifierFilter","comparator":{"value":"dGVzdFF1YWxpZmllck9uZS0y","type":"BinaryComparator"}}]}
#{"op":"MUST_PASS_ALL","type":"FilterList","filters":[{"op":"EQUAL","type":"RowFilter","comparator":{"value":".+-2","type":"RegexStringComparator"}},{"op":"EQUAL","type":"QualifierFilter","comparator":{"value":".+-2","type":"RegexStringComparator"}},{"op":"EQUAL","type":"ValueFilter","comparator":{"value":"one","type":"SubstringComparator"}}]}
#{"op":"MUST_PASS_ONE","type":"FilterList","filters":[{"op":"EQUAL","type":"RowFilter","comparator":{"value":".+Two.+","type":"RegexStringComparator"}},{"op":"EQUAL","type":"QualifierFilter","comparator":{"value":".+-2","type":"RegexStringComparator"}},{"op":"EQUAL","type":"ValueFilter","comparator":{"value":"one","type":"SubstringComparator"}}]}

class Scan(object):

    def __init__(self, client):
        self.client = client

    @staticmethod
    def _build_url_suffix(tbl_name):
        return f"/{tbl_name}/scanner"

    def _get_scanner(self, tbl_name, scanner_payload='<Scanner batch="1000"/>'):
        filter = """
            <Scanner batch="1000">
                <filter>
                    {
                    "type":"PrefixFilter",
                    "value":"dXNlcjE="
                    }
                </filter>
            </Scanner>
        """
        hbase_host = self.client.get_hbase_host()
        url_suffix = self._build_url_suffix(tbl_name)
        response = self.client.session.put(
            url=f"{hbase_host}{url_suffix}",
            data=scanner_payload,
            headers={"Content-Type":"text/xml", "accept":"text/xml"}
        )
        if response.status_code == 201:
            return response.headers.get("Location")
        return {'error': response.content, 'status_code':response.status_code}


    def scan(self, tbl_name, scanner_payload='<Scanner batch="1000"/>'):
        scan_url = self._get_scanner(tbl_name, scanner_payload)
        if isinstance(scan_url, dict):
            return scan_url
        response = self.client.session.get(
            url=scan_url,
            headers=self.client.headers,
            timeout=30
        )
        if response.status_code == 204:
            return
        return result_parser(response.json()) if response.content is not None else None



