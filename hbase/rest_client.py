import json
from requests import Session


class HBaseRESTClient(object):

    def __init__(self, hosts_list):
        self.hosts = hosts_list
        self.session = Session()
        self.headers = {"accept":"application/json", "content-type": "application/json"}
        self.timeout = 10

    def get_hbase_host(self):
        return self.hosts[0]

    def send_request(self, method, url_suffix, payload=None):
        response = None
        host = self.get_hbase_host()
        if method == 'GET':
            response = self.session.get(
                url=f'{host}{url_suffix}',
                headers=self.headers,
                timeout=self.timeout,
            )
        if method == "PUT":
            response = self.session.put(
                url=f"{host}{url_suffix}",
                data=json.dumps(payload) if payload is not None else None,
                headers=self.headers,
                timeout=self.timeout,
            )
        if method == "POST":
            response = self.session.post(
                url=f"{host}{url_suffix}",
                data=json.dumps(payload) if payload is not None else None,
                headers=self.headers,
                timeout=self.timeout,
            )
        if method == "DELETE":
            response = self.session.delete(
                url=f"{host}{url_suffix}",
                headers=self.headers,
                timeout=self.timeout,
            )

        if response.status_code in [200, 201]:
            return response.content.decode('utf-8')
        else:
            return {"error": response.content.decode('utf-8'), "status_code":response.status_code}

