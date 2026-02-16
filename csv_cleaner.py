from __future__ import annotations

import argparse
import csv
import time
from functools import wraps
from typing import Callable, Dict, Generator, Iterable, Iterator, List, Optional, TypeVar, Any

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
    """
    Gibt bereinigte Row zurück oder None, wenn sie verworfen wird.
    """
    name = (row.get("name") or "").strip()
    country = (row.get("country") or "").strip()
    age = parse_int(row.get("age", ""))
    salary = parse_float(row.get("salary", ""))

    # Filter-Regeln
    if not name:
        return None
    if age is None:
        return None
    if salary is None:
        return None
    if only_country and country != only_country:
        return None

    # Hier ein kleines list comprehension Beispiel:
    # wir normalisieren whitespace in allen string-feldern
    cleaned = {
        k: v.strip() if isinstance(v, str) else v
        for k, v in row.items()
    }

    # wir überschreiben mit unseren parsed Werten
    cleaned["name"] = name
    cleaned["country"] = country
    cleaned["age"] = str(age)
    cleaned["salary"] = str(salary)

    return cleaned


def batcher(items: Iterable[T], batch_size: int) -> Iterator[List[T]]:
    """
    Generator: sammelt Elemente in Listen-Batches.
    """
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

    # LIST COMPREHENSION + Generator style:
    # Wir erzeugen einen "cleaned_rows" generator, der nur gültige rows durchlässt.
    cleaned_rows = (
        clean_row(r, only_country=only_country)
        for r in rows
    )

    # Jetzt filtern wir None weg (das ist auch generator-friendly):
    cleaned_rows = (r for r in cleaned_rows if r is not None)

    # Wir schreiben streaming in batches:
    wrote_header = False
    total_written = 0
    total_seen = 0

    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer: Optional[csv.DictWriter] = None

        for batch in batcher(cleaned_rows, batch_size=batch_size):
            # batch enthält Dict[str,str] rows
            total_seen += len(batch)

            # Header/Writer initialisieren, wenn wir erste gültige Zeile haben
            if not wrote_header:
                fieldnames = list(batch[0].keys())
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                wrote_header = True

            assert writer is not None
            writer.writerows(batch)
            total_written += len(batch)

    print(f"[DONE] wrote={total_written} rows -> {output_path}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Streaming CSV Cleaner")
    parser.add_argument("--input", required=True, help="Input CSV path")
    parser.add_argument("--output", required=True, help="Output CSV path")
    parser.add_argument("--batch-size", type=int, default=2, help="Batch size for writing")
    parser.add_argument("--only-country", default=None, help="Keep only rows matching country (e.g. Germany)")
    args = parser.parse_args()

    process_csv(
        input_path=args.input,
        output_path=args.output,
        batch_size=args.batch_size,
        only_country=args.only_country
    )


if __name__ == "__main__":
    main()
