import psycopg2
from multiprocessing import Process
import time
import random


def listener(conn, channels):
    while True:
        cur = conn.cursor()
        for channel in channels:
            cur.execute(f"LISTEN {channel};")
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
                end_time timestamptz
            );
            """
        )


def process_one(channel_3_0, channel_3_1, channel_2):
    with get_connection() as conn:
        cur = conn.cursor()
        cookie = "ala ma kota"
        ip = "192.168.1.1"
        for _ in range(1000):
            time.sleep(0.01)
            cur.execute(
                f"insert into advert(cookie, ip) values ('{cookie}', '{ip}') returning id"
            )

            id_ = cur.fetchone()[0]
            q = random.choice([0, 1, 2])
            label2 = random.choice(channel_2)
            if q == 2:
                label2 = random.choice(channel_2)
                cur.execute(f"notify forward_{label2}, '{id_}'")
            elif q == 1:
                cur.execute(f"notify {label2}, '{id_}'")
                label3 = random.choice(channel_3_1)
                cur.execute(f"notify {label3}, '{id_}'")
            elif q == 0:
                cur.execute(f"notify {label2}, '{id_}'")
                label3 = random.choice(channel_3_0)
                cur.execute(f"notify {label3}, '{id_}'")


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
            cur.execute(f"notify advert_channel, '{notification.payload}'")
            if notification.channel.startswith("forward"):
                label = random.choice(channels_out)
                cur.execute(f"notify {label}, '{notification.payload}'")


def process_tree_0(channels):
    with get_connection() as conn:
        cur = conn.cursor()
        for notification in listener(conn, channels):
            # print(notification)
            cur.execute(f"Select * from advert where id = '{notification.payload}'")
            cur.execute(
                f"UPDATE advert SET end_time = current_timestamp WHERE id = {notification.payload}"
            )


def process_tree_1(channels):
    with get_connection() as conn:
        cur = conn.cursor()
        for notification in listener(conn, channels):
            cur.execute(f"Select * from advert where id = '{notification.payload}'")
            cur.execute(
                f"UPDATE advert SET end_time = current_timestamp WHERE id = {notification.payload}"
            )


def process_tree_2(channels):
    with get_connection() as conn:
        cur = conn.cursor()
        for notification in listener(conn, channels):
            cur.execute(f"Select * from advert where id = '{notification.payload}'")
            cur.execute(
                f"UPDATE advert SET end_time = current_timestamp WHERE id = {notification.payload}"
            )


def make_labels(n1, n2, n3, n4):
    return (
        ["q_from_1_to_3_0" + str(x) for x in range(n1)],
        ["q_from_1_to_3_1" + str(x) for x in range(n2)],
        ["q_from_1_to_2" + str(x) for x in range(n3)],
        ["q_from_2_to_3_2" + str(x) for x in range(n4)],
    )


def main():
    prepare_tables()
    n_3_0 = 1  # 2 is faster than 3, too much processes
    n_3_1 = 1  # 2 is faster than 3, too much processes
    n_3_2 = 1  # 2 is faster than 3, too much processes
    n_1 = 2
    n_2 = 2

    # n_3_0 = 1  # 2 is faster than 3, too much processes
    # n_3_1 = 1  # 2 is faster than 3, too much processes
    # n_3_2 = 1  # 2 is faster than 3, too much processes
    # n_1 = 1
    # n_2 = 1

    q_from_1_to_3_0, q_from_1_to_3_1, q_from_1_to_2, q_from_2_to_3_2 = make_labels(
        n_3_0, n_3_1, n_2, n_3_2
    )

    proc = [
        [
            Process(target=process_two, args=(q_from_1_to_2, q_from_2_to_3_2))
            for _ in range(n_2)
        ],
        [Process(target=process_tree_0, args=([a],)) for a in q_from_1_to_3_0],
        [Process(target=process_tree_1, args=([a],)) for a in q_from_1_to_3_1],
        [Process(target=process_tree_2, args=([a],)) for a in q_from_2_to_3_2],
        [
            Process(
                target=process_one,
                args=(q_from_1_to_3_0, q_from_1_to_3_1, q_from_1_to_2),
            )
            for _ in range(n_1)
        ],
    ]
    proc = [x for y in proc for x in y]

    for p in proc:
        p.start()

    for p in proc:
        p.join()


if __name__ == "__main__":
    main()
