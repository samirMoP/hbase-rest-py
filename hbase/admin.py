class HBaseAdmin(object):
    def __init__(self, client):
        self.client = client

    def cluster_status(self):
        return self.client.send_request(method="GET", url_suffix=f"/status/cluster")

    def cluster_version(self):
        return self.client.send_request(method="GET", url_suffix=f"/version/cluster")

    def namespaces(self):
        return self.client.send_request(method="GET", url_suffix=f"/namespaces")

    def namespace(self, namespace_name):
        return self.client.send_request(
            method="GET", url_suffix=f"/namespaces/{namespace_name}"
        )

    def namespace_create(self, namespace_name):
        return self.client.send_request(
            method="POST", url_suffix=f"/namespaces/{namespace_name}"
        )

    def namespace_delete(self, namespace_name):
        return self.client.send_request(
            method="DELETE", url_suffix=f"/namespaces/{namespace_name}"
        )

    def delete_namespace(self):
        pass

    def tables(self, namespace="default"):
        return self.client.send_request(
            method="GET", url_suffix=f"/namespaces/{namespace}/tables"
        )

    def table_regions(self, table_name):
        return self.client.send_request(
            method="GET", url_suffix=f"/{table_name}/regions"
        )

    def table_schema(self, table_name):
        return self.client.send_request(
            method="GET", url_suffix=f"/{table_name}/schema"
        )

    def table_create_or_update(self, table_name, params_list):
        """
        Create table if not exist. Otherwise update table schema
        :param table_name:
        :param params_list: [{
                "name": "cfInfo",
                "BLOCKSIZE": "65536"
            }
            ]
        :return:
        """
        payload = {"name": table_name, "ColumnSchema": params_list}
        return self.client.send_request(
            method="PUT", url_suffix=f"/{table_name}/schema", payload=payload
        )

    def table_delete(self, table_name):
        return self.client.send_request(
            method="DELETE", url_suffix=f"/{table_name}/schema"
        )
