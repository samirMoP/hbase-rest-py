# hbase-rest-py
**hbase-rest-py** is python library to interact with Apache HBase by using HBase REST API.

### Prerequisites
**hbase-rest-py** requires at minimum Python 3.6 and above.

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



