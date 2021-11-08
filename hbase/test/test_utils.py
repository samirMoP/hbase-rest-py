from unittest import TestCase
from hbase.utils import b64_encoder, result_parser
import struct
from base64 import b64encode, b64decode


class TestUtils(TestCase):
    def test_b64_encoder(self):
        test_values = ["a", b"test", 10, 250.511234, [1, 3, 5]]
        for value in test_values:
            assert isinstance(b64_encoder(value), str)

    def test_result_parser(self):
        test_payload = {
            "Row": [
                {
                    "key": "c2FtaXJAZXhhbXBsZS5jb20=",
                    "Cell": [
                        {
                            "column": "Y2Y6YW1vdW50",
                            "timestamp": 1636391507135,
                            "$": "QG9QXAdn004=",
                        }
                    ],
                }
            ]
        }
        parsed = result_parser(test_payload)
        # {'row': [{'key': b'samir@example.com', 'cell': [{'column': b'cf:amount', 'timestamp': 1636391507135, '$': b'@oP\\\x07g\xd3N'}]}]}
        self.assertTrue("row" in parsed.keys())
        self.assertTrue(isinstance(parsed["row"], list))
        self.assertTrue("key" in parsed["row"][0].keys())
        self.assertTrue("cell" in parsed["row"][0].keys())
        self.assertEqual(parsed["row"][0]["key"].decode("utf-8"), "samir@example.com")
        self.assertEqual(
            parsed["row"][0]["cell"][0]["column"].decode("utf-8"), "cf:amount"
        )
        self.assertTrue(isinstance(parsed["row"][0]["cell"][0]["$"], bytes))
