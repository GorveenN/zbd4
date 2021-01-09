import psycopg2
from core import run
import argparse
import sys
import os


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


def post_f1():
    conn = get_connection()
    cur = conn.cursor()

    def f(id):
        cookie = "ala ma kota"
        ip = "192.168.1.1"
        cur.execute(
            f"insert into advert(id, cookie, ip) values ({id}, '{cookie}', '{ip}') returning id"
        )

    return f


def post_f2():
    conn = get_connection()
    cur = conn.cursor()

    def f(id):
        city = "warszawa"
        country = "polska"
        # print(f"Process 2 received {id}")
        cur.execute(f"Select * from advert WHERE id = {id}")
        cur.execute(
            f"UPDATE advert SET mod_time = current_timestamp, city = '{city}', country = '{country}' WHERE id = {id}"
        )

    return f


def post_f3_0():
    conn = get_connection()
    cur = conn.cursor()

    def f(id):
        # print(f"Process 3_0 received {id}")
        cur.execute(
            f"UPDATE advert SET proc_type=0, end_time = current_timestamp WHERE id = {id}"
        )

    return f


def post_f3_1():
    conn = get_connection()
    cur = conn.cursor()

    def f(id):
        # print(f"Process 3_1 received {id}")
        cur.execute(f"Select * from advert WHERE id = {id}")
        cur.execute(
            f"UPDATE advert SET proc_type=1, end_time = current_timestamp WHERE id = {id}"
        )

    return f


def post_f3_2():
    conn = get_connection()
    cur = conn.cursor()

    def f(id):
        # print(f"Process 3_2 received {id}")
        cur.execute(f"Select * from advert WHERE id = {id}")
        cur.execute(
            f"UPDATE advert SET proc_type=2, end_time = current_timestamp WHERE id = {id}"
        )

    return f


def dump_results(args):
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


def runPostgres(num_iter, sleep_dur, a):
    run(post_f1, num_iter, sleep_dur, post_f2, post_f3_0, post_f3_1, post_f3_2, *a)


def parse_args():
    parser = argparse.ArgumentParser()

    parser.add_argument("--num-proc", type=int, default=5)
    parser.add_argument("--num-iter", type=int, default=1000)
    parser.add_argument("--sleep-dur", type=float, default=0.001)
    parser.add_argument("--results")

    return parser.parse_args()


def main(args):
    prepare_tables()
    a = args.num_proc
    a_tree = round(0.5 + a / 3)
    runPostgres(args.num_iter, args.sleep_dur, [a, a, a_tree, a_tree, a_tree])


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
