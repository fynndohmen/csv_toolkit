from __future__ import annotations

import csv
import threading
import time
from functools import wraps
from queue import Queue
from typing import Any, Callable, Dict, Iterable, Iterator, List, Optional, TypeVar, cast
from queue import Queue, Full

T = TypeVar("T")


def timeit(func: Callable[..., T]) -> Callable[..., T]:
    @wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> T:
        start = time.perf_counter()
        result = func(*args, **kwargs)
        end = time.perf_counter()
        print(f"[TIME] {func.__name__}: {(end - start):.4f}s")
        return result

    return wrapper


def log_calls(func: Callable[..., T]) -> Callable[..., T]:
    @wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> T:
        print(f"[CALL] {func.__name__}()")
        return func(*args, **kwargs)

    return wrapper


def read_rows(path: str) -> Iterator[Dict[str, str]]:
    with open(path, "r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            yield row


def parse_int(value: str) -> Optional[int]:
    value = (value or "").strip()
    if value == "":
        return None
    try:
        return int(value)
    except ValueError:
        return None


def parse_float(value: str) -> Optional[float]:
    value = (value or "").strip()
    if value == "":
        return None
    try:
        return float(value)
    except ValueError:
        return None


def clean_row(row: Dict[str, str], only_country: Optional[str] = None) -> Optional[Dict[str, str]]:
    name = (row.get("name") or "").strip()
    country = (row.get("country") or "").strip()
    age = parse_int(row.get("age", ""))
    salary = parse_float(row.get("salary", ""))

    if not name:
        return None
    if age is None:
        return None
    if salary is None:
        return None
    if only_country and country != only_country:
        return None

    cleaned = {k: v.strip() if isinstance(v, str) else v for k, v in row.items()}

    cleaned["name"] = name
    cleaned["country"] = country
    cleaned["age"] = str(age)
    cleaned["salary"] = str(salary)

    return cleaned


def batcher(items: Iterable[T], batch_size: int) -> Iterator[List[T]]:
    batch: List[T] = []
    for item in items:
        batch.append(item)
        if len(batch) >= batch_size:
            yield batch
            batch = []
    if batch:
        yield batch


@timeit
@log_calls
def process_csv(input_path: str, output_path: str, batch_size: int, only_country: Optional[str]) -> None:
    rows = read_rows(input_path)

    cleaned_rows = (clean_row(r, only_country=only_country) for r in rows)
    cleaned_rows = (r for r in cleaned_rows if r is not None)

    wrote_header = False
    total_written = 0

    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer: Optional[csv.DictWriter] = None

        for batch in batcher(cleaned_rows, batch_size=batch_size):
            if not wrote_header:
                fieldnames = list(batch[0].keys())
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                wrote_header = True

            assert writer is not None
            writer.writerows(batch)
            total_written += len(batch)

    print(f"[DONE] wrote={total_written} rows -> {output_path}")


@timeit
@log_calls
def process_csv_pipeline(
    input_path: str,
    output_path: str,
    batch_size: int,
    only_country: Optional[str],
    worker_count: int = 4,
    queue_size: int = 1000,
) -> None:

    if batch_size <= 0:
        raise ValueError("batch_size must be > 0")
    if worker_count <= 0:
        raise ValueError("worker_count must be > 0")
    if queue_size <= 0:
        raise ValueError("queue_size must be > 0")

    sentinel = object()

    in_q: Queue[object] = Queue(maxsize=queue_size)
    out_q: Queue[object] = Queue(maxsize=queue_size)

    stop_event = threading.Event()

    exc_lock = threading.Lock()
    first_exc: list[BaseException] = []

    stats_lock = threading.Lock()
    stats = {"read": 0, "dropped": 0, "written": 0}

    def record_exc(e: BaseException) -> None:
        with exc_lock:
            if not first_exc:
                first_exc.append(e)

    def reader_thread() -> None:
        try:
            for row in read_rows(input_path):
                if stop_event.is_set():
                    break
                in_q.put(row)
                with stats_lock:
                    stats["read"] += 1
        except BaseException as e:
            record_exc(e)
            stop_event.set()
        finally:
            for _ in range(worker_count):
                in_q.put(sentinel)

    def worker_thread() -> None:
        try:
            while True:
                item = in_q.get()

                if item is sentinel:
                    out_q.put(sentinel)
                    return

                if stop_event.is_set():
                    continue

                row = cast(Dict[str, str], item)
                cleaned = clean_row(row, only_country=only_country)
                if cleaned is None:
                    with stats_lock:
                        stats["dropped"] += 1
                    continue

                out_q.put(cleaned)
        except BaseException as e:
            record_exc(e)
            stop_event.set()
            try:
                out_q.put_nowait(sentinel)
            except Full:
                pass

    def writer_thread() -> None:
        wrote_header = False
        writer: Optional[csv.DictWriter] = None
        batch: List[Dict[str, str]] = []
        sentinels_seen = 0 

        try:
            with open(output_path, "w", newline="", encoding="utf-8") as f:
                while True:
                    item = out_q.get()

                    if item is sentinel:
                        sentinels_seen += 1
                        if sentinels_seen >= worker_count:
                            if batch and writer is not None:
                                writer.writerows(batch)
                                with stats_lock:
                                    stats["written"] += len(batch)
                            return
                        continue

                    row = cast(Dict[str, str], item)

                    if not wrote_header:
                        fieldnames = list(row.keys())
                        writer = csv.DictWriter(f, fieldnames=fieldnames)
                        writer.writeheader()
                        wrote_header = True

                    batch.append(row)

                    if len(batch) >= batch_size:
                        assert writer is not None
                        writer.writerows(batch)
                        with stats_lock:
                            stats["written"] += len(batch)
                        batch.clear()
        except BaseException as e:
            record_exc(e)
            stop_event.set()

    t_reader = threading.Thread(target=reader_thread, name="csv-reader", daemon=True)
    t_writer = threading.Thread(target=writer_thread, name="csv-writer", daemon=True)
    t_workers = [
        threading.Thread(target=worker_thread, name=f"csv-worker-{i+1}", daemon=True)
        for i in range(worker_count)
    ]

    t_writer.start()
    for t in t_workers:
        t.start()
    t_reader.start()

    t_reader.join()
    for t in t_workers:
        t.join()
    t_writer.join()

    if first_exc:
        raise RuntimeError(f"Pipeline failed: {first_exc[0]}") from first_exc[0]

    with stats_lock:
        read_ = stats["read"]
        dropped_ = stats["dropped"]
        written_ = stats["written"]

    print(f"[DONE] read={read_} dropped={dropped_} wrote={written_} -> {output_path}")


