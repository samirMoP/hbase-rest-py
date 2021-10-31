import array
import pickle
from base64 import b64encode, b64decode
import struct

"""
HBase bytes int -> struct.unpack(">q", value) 8 bytes
HBase bytes float > struct.unpack(">d", value) 8 bytes
"""


def b64_encoder(value):
    value_type = type(value)
    if value_type in [bytes, bytearray, array.array]:
        return b64encode(value).decode('utf-8')
    elif value_type == str:
        return b64encode(value.encode('utf-8')).decode('utf-8')
    elif value_type == int:
        return b64encode(struct.pack(">q", value)).decode('utf-8')
    elif value_type == float:
        return b64encode(struct.pack(">d", value)).decode('utf-8')
    else:
        return b64encode(pickle.dumps(value)).decode('utf-8')


def result_parser(json_result):
    row = json_result["Row"]
    result = []
    key = {}
    cells = []
    for k in row:
        key["key"] = b64decode(k['key'])
        cell = k['Cell']
        for i in cell:
            cells.append({'column': b64decode(i['column']),
                          'timestamp': i['timestamp'],
                          "$": b64decode(i['$'])})
        key['cell'] = cells
        result.append(key)
        cells =[]
        key = {}
    return {"row": result}

