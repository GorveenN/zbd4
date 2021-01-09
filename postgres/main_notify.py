import psycopg2
from multiprocessing import Process
import time
import random
import argparse
import sys
import os
import select


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
                in_time  timestamptz default current_timestamp,
                mod_time timestamptz,
                end_time timestamptz,
                proc_type INT
            );
            """
        )


def process_one(num_iter, sleep_dur, prefix, channel_3_0, channel_3_1, channel_2):
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

            type3 = random.choice([0, 1, 2])
            label2 = random.choice(channel_2)
            if type3 == 2:
                cur.execute(f"notify forward_{label2}, '{i}'")
            elif type3 == 1:
                label3 = random.choice(channel_3_1)
                cur.execute(f"notify {label2}, '{i}'")
                cur.execute(f"notify {label3}, '{i}'")
            elif type3 == 0:
                label3 = random.choice(channel_3_0)
                cur.execute(f"notify {label2}, '{i}'")
                cur.execute(f"notify {label3}, '{i}'")


def process_two(channels_in, channels_out):
    with get_connection() as conn:
        cur = conn.cursor()
        channels_in = channels_in + ["forward_" + x for x in channels_in]
        for notification in listener(conn, channels_in):
            city = "warszawa"
            country = "polska"

            cur.execute(f"Select * from advert where id = '{notification.payload}'")
            cur.execute(
                f"UPDATE advert SET mod_time = current_timestamp, city = '{city}', country = '{country}' WHERE id = {notification.payload}"
            )
            if notification.channel.startswith("forward"):
                label = random.choice(channels_out)
                cur.execute(f"notify {label}, '{notification.payload}'")


def process_tree_0(channels):
    with get_connection() as conn:
        cur = conn.cursor()
        for notification in listener(conn, channels):
            cur.execute(f"Select * from advert where id = '{notification.payload}'")
            cur.execute(
                f"UPDATE advert SET proc_type=0, end_time = current_timestamp WHERE id = {notification.payload}"
            )


def process_tree_1(channels):
    with get_connection() as conn:
        cur = conn.cursor()
        for notification in listener(conn, channels):
            cur.execute(f"Select * from advert where id = '{notification.payload}'")
            cur.execute(
                f"UPDATE advert SET proc_type=1,  end_time = current_timestamp WHERE id = {notification.payload}"
            )


def process_tree_2(channels):
    with get_connection() as conn:
        cur = conn.cursor()
        for notification in listener(conn, channels):
            cur.execute(f"Select * from advert where id = '{notification.payload}'")
            cur.execute(
                f"UPDATE advert SET proc_type=2, end_time = current_timestamp WHERE id = {notification.payload}"
            )


def make_labels(n1, n2, n3, n4):
    return (
        ["q_from_1_to_3_0" + str(x) for x in range(n1)],
        ["q_from_1_to_3_1" + str(x) for x in range(n2)],
        ["q_from_1_to_2" + str(x) for x in range(n3)],
        ["q_from_2_to_3_2" + str(x) for x in range(n4)],
    )


def main(args):
    prepare_tables()
    num_proc = args.num_proc
    num_proc_three = round(0.5 + num_proc / 3)

    q_from_1_to_3_0, q_from_1_to_3_1, q_from_1_to_2, q_from_2_to_3_2 = make_labels(
        num_proc_three, num_proc_three, num_proc, num_proc_three
    )

    proc = [
        [
            Process(target=process_two, args=(q_from_1_to_2, q_from_2_to_3_2))
            for _ in range(num_proc)
        ],
        [Process(target=process_tree_0, args=([a],)) for a in q_from_1_to_3_0],
        [Process(target=process_tree_1, args=([a],)) for a in q_from_1_to_3_1],
        [Process(target=process_tree_2, args=([a],)) for a in q_from_2_to_3_2],
        [
            Process(
                target=process_one,
                args=(
                    args.num_iter,
                    args.sleep_dur,
                    i * 10000000,
                    q_from_1_to_3_0,
                    q_from_1_to_3_1,
                    q_from_1_to_2,
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
                    "Copy (Select * From advert) To STDOUT With CSV DELIMITER ',' HEADER;",
                    f,
                )


def parse_args():
    parser = argparse.ArgumentParser()

    parser.add_argument("--num-proc", type=int, default=5)
    parser.add_argument("--num-iter", type=int, default=1000)
    parser.add_argument("--sleep-dur", type=float, default=0.001)
    parser.add_argument("--results")

    print()

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
