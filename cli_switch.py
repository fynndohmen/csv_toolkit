from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from typing import Optional, Literal

from core import process_csv


@dataclass(frozen=True)
class Job:
    in_file: Path
    out_file: Path
    batch_size: int
    only_country: Optional[str]


@dataclass(frozen=True)
class JobResult:
    job: Job
    ok: bool
    error: Optional[str] = None


def run_job(job: Job) -> JobResult:
    """
    Must be top-level so it can be pickled when using ProcessPoolExecutor (Windows).
    """
    try:
        process_csv(
            input_path=str(job.in_file),
            output_path=str(job.out_file),
            batch_size=job.batch_size,
            only_country=job.only_country,
        )
        return JobResult(job=job, ok=True)
    except Exception as e:
        return JobResult(job=job, ok=False, error=str(e))


def main() -> None:
    parser = argparse.ArgumentParser(description="Streaming CSV Cleaner (directory mode, executor selectable)")
    parser.add_argument("--input-dir", required=True, help="Folder containing CSV files (searched recursively)")
    parser.add_argument("--output-dir", required=True, help="Folder to write cleaned CSV files into")
    parser.add_argument("--batch-size", type=int, default=2, help="Batch size for writing")
    parser.add_argument("--only-country", default=None, help="Keep only rows matching country (e.g. Germany)")
    parser.add_argument("--workers", type=int, default=4, help="Max amount of parallel executed jobs")
    parser.add_argument(
        "--executor",
        choices=["thread", "process"],
        default="thread",
        help="How to parallelize file jobs: thread (default) or process",
    )
    args = parser.parse_args()

    input_dir = Path(args.input_dir)
    output_dir = Path(args.output_dir)

    if not input_dir.exists() or not input_dir.is_dir():
        raise SystemExit(f"Input dir does not exist or is not a directory: {input_dir}")

    output_dir.mkdir(parents=True, exist_ok=True)

    csv_files = sorted(input_dir.rglob("*.csv"))
    if not csv_files:
        print(f"[INFO] No .csv files found under: {input_dir}")
        return

    workers = max(1, int(args.workers))

    jobs: list[Job] = []
    for input_file in csv_files:
        rel_path = input_file.relative_to(input_dir)
        out_subdir = output_dir / rel_path.parent
        out_subdir.mkdir(parents=True, exist_ok=True)

        out_name = f"{input_file.stem}_cleaned{input_file.suffix}"
        output_file = out_subdir / out_name

        jobs.append(
            Job(
                in_file=input_file,
                out_file=output_file,
                batch_size=int(args.batch_size),
                only_country=args.only_country,
            )
        )

    failed = 0

    ExecutorCls = ThreadPoolExecutor if args.executor == "thread" else ProcessPoolExecutor
    with ExecutorCls(max_workers=workers) as executor:
        for result in executor.map(run_job, jobs):
            in_file = result.job.in_file
            out_file = result.job.out_file

            if result.ok:
                print(f"[OK] {in_file} -> {out_file}")
            else:
                failed += 1
                print(f"[FAIL] {in_file} -> {out_file}: {result.error}")

    print(
        f"[DONE] jobs={len(jobs)} failed={failed} executor={args.executor} workers={workers} "
        f"from {input_dir} -> {output_dir}"
    )


if __name__ == "__main__":
    main()