


from __future__ import annotations

import asyncio
import csv
from typing import Dict, Optional, List, Any, Iterable, Iterator
from core_pipeline import read_rows, clean_row


async def enrich_row(row: Dict[str, str]) -> Dict[str, str]:
    """
    Placeholder for an external I/O call (API/DB).
    Replace this later with real logic (e.g. aiohttp).
    """
    await asyncio.sleep(0.02)  # simulate network latency
    row["enriched"] = "true"
    return row


async def _process_csv_async_inner(
    input_path: str,
    output_path: str,
    batch_size: int,
    only_country: Optional[str],
    concurrency: int,
    chunk_size: int,
) -> None:
    if batch_size <= 0:
        raise ValueError("batch_size must be > 0")
    if concurrency <= 0:
        raise ValueError("concurrency must be > 0")
    if chunk_size <= 0:
        raise ValueError("chunk_size must be > 0")

    sem = asyncio.Semaphore(concurrency)

    total_read = 0
    total_dropped = 0
    total_written = 0

    async def handle_one(row: Dict[str, str]) -> Optional[Dict[str, str]]:
        nonlocal total_dropped
        cleaned = clean_row(row, only_country=only_country)
        if cleaned is None:
            total_dropped += 1
            return None

        async with sem:
            # The I/O part (API/DB call)
            enriched = await enrich_row(cleaned)
            return enriched

    wrote_header = False
    writer: Optional[csv.DictWriter] = None

    with open(output_path, "w", newline="", encoding="utf-8") as f:
        buffer: List[Dict[str, str]] = []

        def flush_results(results: List[Optional[Dict[str, str]]]) -> None:
            nonlocal wrote_header, writer, total_written
            # Keep input order: gather returns results in the same order as tasks passed in.
            rows_out = [r for r in results if r is not None]
            if not rows_out:
                return

            if not wrote_header:
                fieldnames = list(rows_out[0].keys())
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                wrote_header = True

            assert writer is not None

            # Still do batch writing for less Python overhead
            batch: List[Dict[str, str]] = []
            for r in rows_out:
                batch.append(r)
                if len(batch) >= batch_size:
                    writer.writerows(batch)
                    total_written += len(batch)
                    batch.clear()

            if batch:
                writer.writerows(batch)
                total_written += len(batch)

        for row in read_rows(input_path):
            total_read += 1
            buffer.append(row)

            if len(buffer) >= chunk_size:
                results = await asyncio.gather(*(handle_one(r) for r in buffer))
                flush_results(results)
                buffer.clear()

        if buffer:
            results = await asyncio.gather(*(handle_one(r) for r in buffer))
            flush_results(results)
            buffer.clear()

    print(f"[DONE] async read={total_read} dropped={total_dropped} wrote={total_written} -> {output_path}")


def process_csv_async(
    input_path: str,
    output_path: str,
    batch_size: int,
    only_country: Optional[str],
    concurrency: int = 20,
    chunk_size: int = 500,
) -> None:
    """
    Sync wrapper so CLI/executors can call it like the other functions.
    """
    asyncio.run(
        _process_csv_async_inner(
            input_path=input_path,
            output_path=output_path,
            batch_size=batch_size,
            only_country=only_country,
            concurrency=concurrency,
            chunk_size=chunk_size,
        )
    )


