from multiprocessing import Process, Queue
from queue import Empty
from time import sleep
import random


def process_one(f, num_iter, sleep_dur, prefix, q_3_0: Queue, q_3_1: Queue, q_2: Queue):
    f = f()
    print(prefix)
    for i in range(num_iter):
        sleep(sleep_dur)
        i = i + prefix
        f(i)
        q = random.choice([0, 1, 2])
        if q == 2:
            q_2.put((i, True))
        elif q == 1:
            q_3_1.put(i)
            q_2.put((i, False))
        elif q == 0:
            q_3_0.put(i)
            q_2.put((i, False))


def process_two(f, q_receive: Queue, q_forward: Queue):
    f = f()
    while True:
        try:
            (id, forward) = q_receive.get(block=True, timeout=3)
            f(id)
            if forward:
                q_forward.put(id)
        except Empty:
            break


def process_three(f, q: Queue):
    f = f()
    while True:
        try:
            id = q.get(block=True, timeout=3)
            f(id)
        except Empty:
            break


def run(
    p1,
    num_iter,
    sleep_dur,
    p2,
    p3_0,
    p3_1,
    p3_2,
    num_p1,
    num_p2,
    num_p3_0,
    num_p3_1,
    num_p3_2,
):
    q_from_1_to_3_0, q_from_1_to_3_1, q_from_1_to_2, q_from_2_to_3_2 = (
        Queue(),
        Queue(),
        Queue(),
        Queue(),
    )

    processes = (
        [
            Process(target=process_two, args=(p2, q_from_1_to_2, q_from_2_to_3_2))
            for _ in range(num_p2)
        ]
        + [
            Process(target=process_three, args=(p3_0, q_from_1_to_3_0))
            for _ in range(num_p3_0)
        ]
        + [
            Process(target=process_three, args=(p3_1, q_from_1_to_3_1))
            for _ in range(num_p3_1)
        ]
        + [
            Process(target=process_three, args=(p3_2, q_from_2_to_3_2))
            for _ in range(num_p3_2)
        ]
        + [
            Process(
                target=process_one,
                args=(
                    p1,
                    num_iter,
                    sleep_dur,
                    i * 1000000,
                    q_from_1_to_3_0,
                    q_from_1_to_3_1,
                    q_from_1_to_2,
                ),
            )
            for i in range(1, num_p1 + 1)
        ]
    )

    for p in processes:
        p.start()

    for p in processes:
        p.join()
