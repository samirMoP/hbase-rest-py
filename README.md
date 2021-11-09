# hbase-rest-py
**hbase-rest-py** is python library to interact with Apache HBase by using HBase REST API.

### Prerequisites
**hbase-rest-py** requires at minimum Python 3.6 and above. HBase versions 2.4.2 and 2.4.8 have been used for library development and testing and all unit tests have been passing on this versions.  

### Installation 
``$ pip install hbase-rest-py``

### Quick start
Make sure you have running instance of HBase REST server by running
``hbase rest start``

````
>>> from hbase.rest_client import HBaseRESTClient
>>> from hbase.admin import HBaseAdmin
>>> client = HBaseRESTClient(['http://localhost:8080'])
>>> admin = HBaseAdmin(client)
>>> admin.tables()
'{"table":[
            {"name":"message"},
            {"name":"py_test"},
            {"name":"tbl_int"},
            {"name":"test_scan"},
            {"name":"test_tb"},
            {"name":"test_tbl"},
            {"name":"users"}]}'

>>> admin.table_schema("message")
'{
	"name": "message",
	"ColumnSchema": [{
		"name": "d",
		"BLOOMFILTER": "ROW",
		"IN_MEMORY": "false",
		"VERSIONS": "1",
		"KEEP_DELETED_CELLS": "FALSE",
		"DATA_BLOCK_ENCODING": "NONE",
		"COMPRESSION": "NONE",
		"TTL": "2147483647",
		"MIN_VERSIONS": "0",
		"BLOCKCACHE": "true",
		"BLOCKSIZE": "65536",
		"REPLICATION_SCOPE": "0"
	}],
	"IS_META": "false"
}'
# Create table for example
>>> admin.table_create_or_update("messages", [{"name":"d"}])

# Write some data to messages table
>>> from hbase.put import Put
>>> put = Put(client=client)
>>> put.put('messages', "test@example.com:22345", "d:to", "test2@example.com")
>>> put.put_multiple_cf("messages", "test@example.com:22345", {"d:m_id":23445, "d:body":"This is some message"})
''
# Get data from messages table
>>> from hbase.get import Get
>>> get = Get(client)
>>> get.get("messages", "test@example.com:22345")
{'row': [{'key': b'test@example.com:22345', 'cell': [{'column': b'd:body', 'timestamp': 1636442504076, '$': b'This is some message'}, {'column': b'd:m_id', 'timestamp': 1636442504076, '$': b'\x00\x00\x00\x00\x00\x00[\x95'}, {'column': b'd:to', 'timestamp': 1636436005215, '$': b'test2@example.com'}]}]}
>>> get.get("messages", "test@example.com:22345", "d:body")
{'row': [{'key': b'test@example.com:22345', 'cell': [{'column': b'd:body', 'timestamp': 1636442504076, '$': b'This is some message'}]}]}

# Put some more testing data
>>> for i in range(1, 1000):
>>>     put.put_multiple_cf("messages", f"test{i}@example.com:{i}", {"d:m_id":i, "d:body":f"Message no {i}", "d:to":f"testx{i}@example.com"})

# Get data from messages table with scan 
# Get messages where m_id is LESS then 10
# First we build scanner filter payload
>>> from hbase.scan_filter_helper import *
>>> scan_filter= build_single_column_value_filter(family="d", qualifier="m_id", value=10, operation="LESS")
>>> scan_filter
'<Scanner batch="1000" maxVersions="1"><filter>{"op":"LESS","type":"SingleColumnValueFilter","family":"ZA==", "qualifier":"bV9pZA==", "comparator": {"value": "AAAAAAAAAAo=", "type": "BinaryComparator"}}</filter></Scanner>'
>>> from hbase.scan import Scan
>>> scan = Scan(client)
>>> result = scan.scan("messages", scan_filter)
>>> result
(True, {'row': [{'key': b'test1@example.com:1', 'cell': [{'column': b'd:body', 'timestamp': 1636445023120, '$': b'Message no 1'}, {'column': b'd:m_id', 'timestamp': 1636445023120, '$': b'\x00\x00\x00\x00\x00\x00\x00\x01'}, {'column': b'd:to', 'timestamp': 1636445023120, '$': b'testx1@example.com'}]}, {'key': b'test2@example.com:2', 'cell': [{'column': b'd:body', 'timestamp': 1636445023129, '$': b'Message no 2'}, {'column': b'd:m_id', 'timestamp': 1636445023129, '$': b'\x00\x00\x00\x00\x00\x00\x00\x02'}, {'column': b'd:to', 'timestamp': 1636445023129, '$': b'testx2@example.com'}]}, {'key': b'test3@example.com:3', 'cell': [{'column': b'd:body', 'timestamp': 1636445023142, '$': b'Message no 3'}, {'column': b'd:m_id', 'timestamp': 1636445023142, '$': b'\x00\x00\x00\x00\x00\x00\x00\x03'}, {'column': b'd:to', 'timestamp': 1636445023142, '$': b'testx3@example.com'}]}, {'key': b'test4@example.com:4', 'cell': [{'column': b'd:body', 'timestamp': 1636445023151, '$': b'Message no 4'}, {'column': b'd:m_id', 'timestamp': 1636445023151, '$': b'\x00\x00\x00\x00\x00\x00\x00\x04'}, {'column': b'd:to', 'timestamp': 1636445023151, '$': b'testx4@example.com'}]}, {'key': b'test5@example.com:5', 'cell': [{'column': b'd:body', 'timestamp': 1636445023164, '$': b'Message no 5'}, {'column': b'd:m_id', 'timestamp': 1636445023164, '$': b'\x00\x00\x00\x00\x00\x00\x00\x05'}, {'column': b'd:to', 'timestamp': 1636445023164, '$': b'testx5@example.com'}]}, {'key': b'test6@example.com:6', 'cell': [{'column': b'd:body', 'timestamp': 1636445023174, '$': b'Message no 6'}, {'column': b'd:m_id', 'timestamp': 1636445023174, '$': b'\x00\x00\x00\x00\x00\x00\x00\x06'}, {'column': b'd:to', 'timestamp': 1636445023174, '$': b'testx6@example.com'}]}, {'key': b'test7@example.com:7', 'cell': [{'column': b'd:body', 'timestamp': 1636445023182, '$': b'Message no 7'}, {'column': b'd:m_id', 'timestamp': 1636445023182, '$': b'\x00\x00\x00\x00\x00\x00\x00\x07'}, {'column': b'd:to', 'timestamp': 1636445023182, '$': b'testx7@example.com'}]}, {'key': b'test8@example.com:8', 'cell': [{'column': b'd:body', 'timestamp': 1636445023189, '$': b'Message no 8'}, {'column': b'd:m_id', 'timestamp': 1636445023189, '$': b'\x00\x00\x00\x00\x00\x00\x00\x08'}, {'column': b'd:to', 'timestamp': 1636445023189, '$': b'testx8@example.com'}]}, {'key': b'test9@example.com:9', 'cell': [{'column': b'd:body', 'timestamp': 1636445023197, '$': b'Message no 9'}, {'column': b'd:m_id', 'timestamp': 1636445023197, '$': b'\x00\x00\x00\x00\x00\x00\x00\t'}, {'column': b'd:to', 'timestamp': 1636445023197, '$': b'testx9@example.com'}]}]})

# Releaase scanner resource
>>> >>> scan.delete_scanner()
200
