import random
import string
from queue import Queue
import random
from hbase.rest_client import HBaseRESTClient
from hbase.admin import HBaseAdmin
from hbase.put import Put
from hbase.scan import Scan
import logging
import threading
from threading import Thread

logger = logging.getLogger()


def queue_put_payload(queue, payload):
    queue.put(payload)


def buid_key():
    return "row-" + "".join(random.choices(string.ascii_letters + string.digits, k=6))


def get_random_county():
    return "".join(
        random.choices(
            [
                "US",
                "BA",
                "DE",
                "SE",
                "FR",
                "IN",
                "NO",
                "SK",
                "AU",
                "UK",
                "DK",
                "NL",
                "BE",
                "ES",
            ]
        )
    )


def get_random_int():
    return random.randint(1, 1000)


def buld_put_payload():
    return {
        "tbl": "tbl_int",
        "row-key": buid_key(),
        "cf_value_map": {
            "cf:country": get_random_county(),
            "cf:count": get_random_int(),
        },
    }


def send_put(queue, total, put_instance):
    while True:
        try:
            progress = queue.qsize()
            if progress == 0:
                break
            percents_done = (1.0 - (progress) / total) * 100
            if progress % 10000 == 0:
                print(
                    " %s - %s  [%s]%% "
                    % (threading.current_thread().name, progress, round(percents_done))
                )
            put_payload = queue.get()
            put_instance.put_multiple_cf(
                tbl_name=put_payload["tbl"],
                row_key=put_payload["row-key"],
                cf_value_map=put_payload["cf_value_map"],
            )
            queue.task_done()
        except Exception as e:
            logger.exception("Error updating releases_releaseartistrole table")
            queue.task_done()


if __name__ == "__main__":
    NUMBER_OF_THREDDS = 20
    client = HBaseRESTClient(hosts_list=["http://localhost:8080"])
    scan = Scan(client=client)
    # admin = HBaseAdmin(client=client)
    # admin.table_create_or_update(table_name='tbl_int', params_list=[{'name':'cf'}])
    # put = Put(client=client)
    # q = Queue()
    # for i in range(1, 200000):
    #    put_payload = buld_put_payload()
    #    print(put_payload)
    #    queue_put_payload(q, put_payload)
    # print(f"Starting {NUMBER_OF_THREDDS} ....")
    # for t in range(0, NUMBER_OF_THREDDS):
    #    worker = Thread(target=send_put, args=(q, 200000, put))
    #    worker.setDaemon(True)
    #    worker.start()
    # q.join()
    next, data = scan.scan("tbl_int")
    print(data)
    while next:
        next, data = scan.scan_next()
        print(data)
