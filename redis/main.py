from multiprocessing import Process
from datetime import datetime
import time
import argparse
import sys
import os
import random
import numpy as np
import redis
import csv


def listener(r: redis.Redis, channels):
    p = r.pubsub()
    p.subscribe(channels)

    while True:
        message = p.get_message(timeout=2)
        if message is not None:
            if not isinstance(message["data"], int):
                yield message
        else:
            print("timeout")
            break


def get_connection():
    return redis.Redis(decode_responses=True)


def process_one(
    num_iter,
    sleep_dur,
    prefix,
    proc1_proc2,
    proc1_proc3,
    more_info_proc3,
):
    r = get_connection()
    cookie = "ala ma kota"
    ip = "192.168.1.1"
    for i in range(num_iter):
        i = i + prefix

        time.sleep(sleep_dur)

        r.eval(
            f"local time = redis.call('time'); \
                redis.call('HMSET', \
                        'advert:{i}', \
                        'cookie', '{cookie}', \
                        'ip', '{ip}', \
                        'seconds_in', time[1], \
                        'miliseconds_in', time[2])",
            0,
        )

        proc2 = random.choice(proc1_proc2)
        proc3, proc3_forward = random.choice(list(zip(proc1_proc3, more_info_proc3)))
        r.publish(proc2, f"{i} {proc3_forward}")
        r.publish(proc3, str(i))


def process_two(channel):
    r = get_connection()
    city = "warszawa"
    country = "polska"
    for notification in listener(get_connection(), [channel]):
        id_, forward_channel = notification["data"].split()
        r.hgetall(f"advert:{id_}")
        r.eval(
            f"local time = redis.call('time'); \
              redis.call('HMSET', \
                'advert:{id_}', \
                'city', '{city}', \
                'country', '{country}', \
                'seconds_mid', time[1], \
                'miliseconds_mid', time[2])",
            0,
        )
        r.publish(forward_channel, str(id_))


def process_three(channel1, channel2):
    def update(r, num, id_):
        r.eval(
            f"local time = redis.call('time'); \
              redis.call('HMSET', \
                'advert:{id_}', \
                'emmit_type', '{num}', \
                'seconds_end', time[1], \
                'miliseconds_end', time[2])",
            0,
        )

    def emmit(r, id_):
        [a] = np.random.choice(2, 1, p=[0.3, 0.7])
        if a == 0:
            update(r, 2, id_)
        else:
            update(r, 1, id_)

    need_more_info = set()
    r = get_connection()
    for notification in listener(r, [channel1, channel2]):
        # print(notification)
        id_ = notification["data"]
        channel = notification["channel"]
        record = r.hgetall(f"advert:{id_}")
        if channel == channel2:
            if id_ in need_more_info:
                need_more_info.remove(id_)
                emmit(r, id_)
        else:
            [a] = np.random.choice(2, 1, p=[0.1, 0.9])
            if a == 0:
                update(r, 0, id_)
            else:
                if b"seconds_mid" in record.keys():
                    emmit(r, id_)
                else:
                    need_more_info.add(id_)


def make_labels(n2, n3):
    return (
        ["proc1_proc2_" + str(x) for x in range(n2)],
        ["proc1_proc3_" + str(x) for x in range(n3)],
        ["more_info_proc3_" + str(x) for x in range(n3)],
    )


def main(args):
    r = redis.Redis()
    r.flushall()
    assert len(r.keys("*")) == 0
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
    r = redis.Redis()
    with open(
        f"{args.results}/result_{args.num_proc}_{args.num_iter}_{args.sleep_dur}.csv",
        "w",
    ) as f:
        writer = csv.writer(f)
        writer.writerow(["time_in", "time_mid", "time_end", "emmit_type"])
        a = r.keys("*")
        for x in a:
            d = r.hgetall(x)
            record = [
                datetime.fromtimestamp(
                    int(d[b"seconds_in"]) + int(d[b"miliseconds_in"]) / 1000000
                ),
                datetime.fromtimestamp(
                    int(d[b"seconds_mid"]) + int(d[b"miliseconds_mid"]) / 1000000
                ),
                datetime.fromtimestamp(
                    int(d[b"seconds_end"]) + int(d[b"miliseconds_end"]) / 1000000
                ),
                int(d[b"emmit_type"]),
            ]
            writer.writerow(record)


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
