import string
import time
from queue import Queue
import random
from hbase.rest_client import HBaseRESTClient
from hbase.admin import HBaseAdmin
from hbase.put import Put
from hbase.scan import Scan
import logging
import threading
from threading import Thread


logging.basicConfig(level=logging.INFO)
times = []


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
                logging.info(
                    " %s - %s  [%s]%% "
                    % (threading.current_thread().name, progress, round(percents_done))
                )
            put_payload = queue.get()
            start = time.time()
            put_instance.put(
                tbl_name=put_payload["tbl"],
                row_key=put_payload["row-key"],
                cell_map=put_payload["cf_value_map"],
            )
            end = time.time()
            times.append(end - start)
            queue.task_done()
        except Exception as e:
            logging.exception("Error updating releases_releaseartistrole table")
            queue.task_done()


if __name__ == "__main__":
    NUMBER_OF_THREDDS = 1
    WRITE_ROWS = 20000
    client = HBaseRESTClient(hosts_list=["http://localhost:8080"])
    scan = Scan(client=client)
    admin = HBaseAdmin(client=client)
    admin.table_create_or_update(table_name="tbl_int", params_list=[{"name": "cf"}])
    put = Put(client=client)
    q = Queue()
    for i in range(1, WRITE_ROWS):
        put_payload = buld_put_payload()
        queue_put_payload(q, put_payload)
    logging.info(f"Starting {NUMBER_OF_THREDDS} writer threads")
    for t in range(0, NUMBER_OF_THREDDS):
        worker = Thread(target=send_put, args=(q, WRITE_ROWS, put))
        worker.setDaemon(True)
        worker.start()
    q.join()

    avg_request_time = sum(times) / WRITE_ROWS
    print(f"Max request time [ms] {max(times)*1000}")
    print(f"Min request time [ms] {min(times) *1000}")
    print(f"Average put op time [ms] {avg_request_time *1000}")
