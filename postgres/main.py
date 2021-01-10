import psycopg2
from multiprocessing import Process
import time
import random
import argparse
import sys
import os
import select
import numpy as np


def listener(conn, channels):
    cur = conn.cursor()
    for channel in channels:
        cur.execute(f"LISTEN {channel};")
    while True:
        if select.select([conn], [], [], 2) == ([], [], []):
            print("Timeout")
            break
        else:
            conn.poll()
            while conn.notifies:
                yield conn.notifies.pop(0)


def get_connection():
    conn = psycopg2.connect(
        host="localhost", database="postgres", port=5432, user="postgres"
    )
    conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    return conn


def prepare_tables():
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute("DROP TABLE IF EXISTS advert;")
        cur.execute(
            """
            CREATE TABLE if not exists advert (
                id       SERIAL PRIMARY KEY,
                cookie   TEXT NOT NULL,
                ip       inet NOT NULL,
                city     TEXT,
                country  TEXT,
                time_in  timestamptz default current_timestamp,
                time_mid timestamptz,
                time_end timestamptz,
                emmit_type INT
            );
            """
        )


def process_one(
    num_iter,
    sleep_dur,
    prefix,
    proc1_proc2,
    proc1_proc3,
    more_info_proc3,
):
    with get_connection() as conn:
        cur = conn.cursor()
        cookie = "ala ma kota"
        ip = "192.168.1.1"
        for i in range(num_iter):
            i = i + prefix
            time.sleep(sleep_dur)
            cur.execute(
                f"insert into advert(id, cookie, ip) values ({i}, '{cookie}', '{ip}')"
            )

            proc2 = random.choice(proc1_proc2)
            proc3, proc3_forward = random.choice(
                list(zip(proc1_proc3, more_info_proc3))
            )

            cur.execute(f"notify {proc2}, '{i} {proc3_forward}'")
            cur.execute(f"notify {proc3}, '{i}'")


def process_two(channel):
    with get_connection() as conn:
        cur = conn.cursor()
        for notification in listener(conn, [channel]):
            city = "warszawa"
            country = "polska"

            id_, forward_channel = notification.payload.split()

            cur.execute(f"Select * from advert where id = '{id_}'")
            cur.execute(
                f"UPDATE advert SET time_mid = current_timestamp, city = '{city}', country = '{country}' WHERE id = {id_}"
            )
            cur.execute(f"notify {forward_channel}, '{id_}'")


def process_three(channel1, channel2):
    def update(cur, num, payload):
        cur.execute(
            f"UPDATE advert SET emmit_type={num}, time_end = current_timestamp WHERE id = {payload}"
        )

    def emmit(cur, payload):
        [a] = np.random.choice(2, 1, [0.3, 0.7])
        if a == 0:
            update(cur, 2, payload)
        else:
            update(cur, 1, payload)

    need_more_info = set()
    with get_connection() as conn:
        cur = conn.cursor()
        for notification in listener(conn, [channel1, channel2]):
            cur.execute(f"Select * from advert where id = '{notification.payload}'")
            if notification.channel == channel2:
                if notification.payload in need_more_info:
                    need_more_info.remove(notification.payload)
                    [a] = np.random.choice(2, 1, [0.3, 0.7])
                    emmit(cur, notification.payload)
            else:
                [a] = np.random.choice(2, 1, [0.1, 0.9])
                if a == 0:
                    update(cur, 0, notification.payload)
                else:
                    record = cur.fetchall()
                    if record[0][4] is not None:
                        emmit(cur, notification.payload)
                    else:
                        need_more_info.add(notification.payload)
    print(need_more_info)


def make_labels(n2, n3):
    return (
        ["proc1_proc2_" + str(x) for x in range(n2)],
        ["proc1_proc3_" + str(x) for x in range(n3)],
        ["more_info_proc3_" + str(x) for x in range(n3)],
    )


def main(args):
    prepare_tables()
    num_proc = args.num_proc

    (
        proc1_proc2,
        proc1_proc3,
        more_info_proc3,
    ) = make_labels(num_proc, num_proc)

    proc = [
        [Process(target=process_two, args=(a,)) for a in proc1_proc2],
        [
            Process(target=process_three, args=a)
            for a in zip(proc1_proc3, more_info_proc3)
        ],
        [
            Process(
                target=process_one,
                args=(
                    args.num_iter,
                    args.sleep_dur,
                    i * 10000000,
                    proc1_proc2,
                    proc1_proc3,
                    more_info_proc3,
                ),
            )
            for i in range(num_proc)
        ],
    ]
    proc = [x for y in proc for x in y]

    for p in proc:
        p.start()

    for p in proc:
        p.join()


def dump_results(args):
    if args.results:
        with get_connection() as conn:
            cur = conn.cursor()
            with open(
                f"{args.results}/result_{args.num_proc}_{args.num_iter}_{args.sleep_dur}.csv",
                "w",
            ) as f:
                cur.copy_expert(
                    "Copy (Select time_in, time_mid, time_end, emmit_type From advert) To STDOUT With CSV DELIMITER ',' HEADER;",
                    f,
                )


def parse_args():
    parser = argparse.ArgumentParser()

    parser.add_argument("--num-proc", type=int, default=5)
    parser.add_argument("--num-iter", type=int, default=1000)
    parser.add_argument("--sleep-dur", type=float, default=0.001)
    parser.add_argument("--results")

    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    try:
        main(args)
        dump_results(args)
    except KeyboardInterrupt:
        dump_results(args)
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)
