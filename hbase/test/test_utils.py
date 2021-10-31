from unittest import TestCase
from hbase.utils import b64_encoder, result_parser
import struct
from base64 import b64encode, b64decode


class TestUtils(TestCase):

    def test_b64_encoder(self):
        test_values = ["a", b'test', 10, 250.511234, [1, 3, 5]]
        for value in test_values:
            assert isinstance(b64_encoder(value), str)

    def test_struct(self):
        f = 250.511234
        b = struct.pack("<d", f)
        print(type(b))
        u = struct.unpack("<d", b)
        print(u)
        print(type(u))
        b64 = b64encode(b).decode('utf-8')
        b64_d = b64decode(b64)
        print(struct.unpack("<d", b64_d))

