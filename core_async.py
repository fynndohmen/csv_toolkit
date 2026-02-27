from __future__ import annotations

import asyncio
import csv
import random
from typing import Dict, Optional, List, Tuple

from core_pipeline import read_rows, clean_row


async def enrich_row(row: Dict[str, str]) -> Dict[str, str]:
    """
    Placeholder for an external I/O call (API/DB).
    Replace this later with real logic (e.g. aiohttp).
    """
    await asyncio.sleep(0.02)  # simulate network latency
    row["enriched"] = "true"
    return row


async def enrich_row_with_retry(
    row: Dict[str, str],
    *,
    timeout_s: float,
    max_retries: int,
    backoff_base_s: float,
    backoff_max_s: float,
) -> Tuple[Optional[Dict[str, str]], bool, str, int]:
    """
    Runs enrich_row(row) with:
      - timeout (asyncio.wait_for)
      - retry with exponential backoff + small jitter

    Returns: (result_row_or_None, ok, error_message, attempts_used)
    """
    attempts = 0
    last_err = ""

    for attempt in range(1, max_retries + 2):  # max_retries=2 -> attempts 1..3
        attempts = attempt
        try:
            # Shallow copy so a failed attempt cannot partially mutate the original row
            payload = dict(row)

            result = await asyncio.wait_for(enrich_row(payload), timeout=timeout_s)
            return result, True, "", attempts

        except asyncio.TimeoutError:
            last_err = f"TimeoutError: enrichment exceeded {timeout_s}s"
        except Exception as e:
            last_err = f"{type(e).__name__}: {e}"

        if attempt <= max_retries:
            delay = min(backoff_base_s * (2 ** (attempt - 1)), backoff_max_s)
            delay += random.uniform(0.0, delay * 0.1)  # up to +10% jitter
            await asyncio.sleep(delay)

    return None, False, last_err, attempts


class RateLimiter:
    """
    Simple global (per file/job) rate limiter: max N "starts" per second.

    This enforces a minimum interval between allowed starts:
      interval = 1 / rate_limit_rps
    """

    def __init__(self, rate_limit_rps: float) -> None:
        if rate_limit_rps <= 0:
            raise ValueError("rate_limit_rps must be > 0")
        self._interval = 1.0 / rate_limit_rps
        self._lock = asyncio.Lock()
        self._next_time = 0.0

    async def wait_turn(self) -> None:
        loop = asyncio.get_running_loop()
        async with self._lock:
            now = loop.time()
            if self._next_time <= 0.0:
                self._next_time = now

            wait_s = self._next_time - now
            if wait_s > 0:
                await asyncio.sleep(wait_s)
                now = loop.time()

            # schedule the next allowed start
            self._next_time = max(self._next_time, now) + self._interval


async def _process_csv_async_inner(
    input_path: str,
    output_path: str,
    batch_size: int,
    only_country: Optional[str],
    concurrency: int,
    chunk_size: int,
    *,
    enrich_timeout_s: float,
    enrich_max_retries: int,
    enrich_backoff_base_s: float,
    enrich_backoff_max_s: float,
    rate_limit_rps: float,
) -> None:
    if batch_size <= 0:
        raise ValueError("batch_size must be > 0")
    if concurrency <= 0:
        raise ValueError("concurrency must be > 0")
    if chunk_size <= 0:
        raise ValueError("chunk_size must be > 0")
    if enrich_timeout_s <= 0:
        raise ValueError("enrich_timeout_s must be > 0")
    if enrich_max_retries < 0:
        raise ValueError("enrich_max_retries must be >= 0")
    if enrich_backoff_base_s < 0:
        raise ValueError("enrich_backoff_base_s must be >= 0")
    if enrich_backoff_max_s < 0:
        raise ValueError("enrich_backoff_max_s must be >= 0")
    if rate_limit_rps < 0:
        raise ValueError("rate_limit_rps must be >= 0 (0 disables rate limiting)")

    sem = asyncio.Semaphore(concurrency)
    limiter: Optional[RateLimiter] = RateLimiter(rate_limit_rps) if rate_limit_rps > 0 else None

    total_read = 0
    total_dropped = 0
    total_written = 0
    total_enrich_failed = 0

    async def handle_one(row: Dict[str, str]) -> Optional[Dict[str, str]]:
        nonlocal total_dropped, total_enrich_failed

        cleaned = clean_row(row, only_country=only_country)
        if cleaned is None:
            total_dropped += 1
            return None

        # External I/O gate: concurrency + optional rate limiting
        async with sem:
            if limiter is not None:
                await limiter.wait_turn()

            enriched, ok, err, attempts = await enrich_row_with_retry(
                cleaned,
                timeout_s=enrich_timeout_s,
                max_retries=enrich_max_retries,
                backoff_base_s=enrich_backoff_base_s,
                backoff_max_s=enrich_backoff_max_s,
            )

        # Error policy: do NOT drop the row if enrichment fails.
        out = enriched if ok and enriched is not None else dict(cleaned)

        if ok:
            out["enriched"] = out.get("enriched", "true")
            out["enrich_ok"] = "true"
            out["enrich_error"] = ""
        else:
            total_enrich_failed += 1
            out["enriched"] = "false"
            out["enrich_ok"] = "false"
            out["enrich_error"] = err

        out["enrich_attempts"] = str(attempts)
        return out

    wrote_header = False
    writer: Optional[csv.DictWriter] = None

    with open(output_path, "w", newline="", encoding="utf-8") as f:
        pending_batch: List[Dict[str, str]] = []

        def ensure_writer(first_row: Dict[str, str]) -> None:
            nonlocal wrote_header, writer
            if wrote_header:
                return
            fieldnames = list(first_row.keys())
            writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
            writer.writeheader()
            wrote_header = True

        def write_row(row_out: Optional[Dict[str, str]]) -> None:
            nonlocal total_written
            if row_out is None:
                return

            if not wrote_header:
                ensure_writer(row_out)

            assert writer is not None
            pending_batch.append(row_out)

            if len(pending_batch) >= batch_size:
                writer.writerows(pending_batch)
                total_written += len(pending_batch)
                pending_batch.clear()

        def flush_pending() -> None:
            nonlocal total_written
            if not pending_batch:
                return
            assert writer is not None
            writer.writerows(pending_batch)
            total_written += len(pending_batch)
            pending_batch.clear()

        buffer: List[Dict[str, str]] = []

        async def process_buffer(buf: List[Dict[str, str]]) -> None:
            """
            Start tasks for the current chunk and consume them as they finish.
            We preserve input order by storing results by index and writing in-order
            when possible.
            """
            tasks: List[asyncio.Task[Optional[Dict[str, str]]]] = [
                asyncio.create_task(handle_one(r)) for r in buf
            ]
            task_to_idx = {t: i for i, t in enumerate(tasks)}

            ready: dict[int, Optional[Dict[str, str]]] = {}
            next_idx = 0

            try:
                for fut in asyncio.as_completed(tasks):
                    res = await fut
                    idx = task_to_idx[fut]  # type: ignore[index]
                    ready[idx] = res

                    # write any contiguous results in order
                    while next_idx in ready:
                        write_row(ready.pop(next_idx))
                        next_idx += 1

            except Exception:
                # If something unexpected happens, cancel remaining tasks
                for t in tasks:
                    if not t.done():
                        t.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)
                raise

            finally:
                # Ensure any remaining in-order data is written (should be empty)
                while next_idx in ready:
                    write_row(ready.pop(next_idx))
                    next_idx += 1

        for row in read_rows(input_path):
            total_read += 1
            buffer.append(row)

            if len(buffer) >= chunk_size:
                await process_buffer(buffer)
                buffer.clear()

        if buffer:
            await process_buffer(buffer)
            buffer.clear()

        # Flush last partial batch
        if wrote_header:
            flush_pending()

    print(
        f"[DONE] async read={total_read} dropped={total_dropped} "
        f"enrich_failed={total_enrich_failed} wrote={total_written} -> {output_path}"
    )


def process_csv_async(
    input_path: str,
    output_path: str,
    batch_size: int,
    only_country: Optional[str],
    concurrency: int = 20,
    chunk_size: int = 500,
    *,
    enrich_timeout_s: float = 5.0,
    enrich_max_retries: int = 2,
    enrich_backoff_base_s: float = 0.2,
    enrich_backoff_max_s: float = 2.0,
    rate_limit_rps: float = 0.0,
) -> None:
    """
    Sync wrapper so CLI/executors can call it like the other functions.

    - concurrency: max in-flight enrichment calls per file
    - chunk_size: how many rows are scheduled per cycle
    - rate_limit_rps: max enrichment starts per second (0 disables)

    Timeout/retry:
    - enrich_timeout_s: timeout per enrichment attempt
    - enrich_max_retries: retries after the first attempt (0 = no retries)
    """
    asyncio.run(
        _process_csv_async_inner(
            input_path=input_path,
            output_path=output_path,
            batch_size=batch_size,
            only_country=only_country,
            concurrency=concurrency,
            chunk_size=chunk_size,
            enrich_timeout_s=enrich_timeout_s,
            enrich_max_retries=enrich_max_retries,
            enrich_backoff_base_s=enrich_backoff_base_s,
            enrich_backoff_max_s=enrich_backoff_max_s,
            rate_limit_rps=rate_limit_rps,
        )
    )