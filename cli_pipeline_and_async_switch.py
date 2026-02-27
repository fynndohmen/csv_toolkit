

from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from typing import Optional

from core_pipeline import process_csv, process_csv_pipeline
from core_async import process_csv_async

@dataclass(frozen=True)
class Job:
    in_file: Path
    out_file: Path
    batch_size: int
    only_country: Optional[str]
    mode: str  # "simple" | "pipeline" | "async"
    pipeline_workers: int
    queue_size: int
    async_concurrency: int
    async_chunk_size: int


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
        if job.mode == "pipeline":
            process_csv_pipeline(
                input_path=str(job.in_file),
                output_path=str(job.out_file),
                batch_size=job.batch_size,
                only_country=job.only_country,
                worker_count=job.pipeline_workers,
                queue_size=job.queue_size,
            )
        elif job.mode == "async":
            process_csv_async(
                input_path=str(job.in_file),
                output_path=str(job.out_file),
                batch_size=job.batch_size,
                only_country=job.only_country,
                concurrency=job.async_concurrency,
                chunk_size=job.async_chunk_size,
            )
        else:
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
    parser = argparse.ArgumentParser(description="Streaming CSV Cleaner (directory mode)")
    parser.add_argument("--input-dir", required=True, help="Folder containing CSV files (searched recursively)")
    parser.add_argument("--output-dir", required=True, help="Folder to write cleaned CSV files into")
    parser.add_argument("--batch-size", type=int, default=2, help="Batch size for writing")
    parser.add_argument("--only-country", default=None, help="Keep only rows matching country (e.g. Germany)")

    # File-level parallelism (jobs)
    parser.add_argument("--workers", type=int, default=4, help="Max amount of parallel executed file jobs")
    parser.add_argument(
        "--executor",
        choices=["thread", "process"],
        default="thread",
        help="How to parallelize file jobs: thread (default) or process",
    )

    # Which processing mode to use per file
    parser.add_argument(
        "--mode",
        choices=["simple", "pipeline", "async"],
        default="simple",
        help="Row processing mode per file: simple (generator stream), pipeline (Reader/Worker/Writer queues) or async (using waiting time caused by latency of network/OS for API/DB calls)",
    )

    # Pipeline-specific knobs (only used if --mode pipeline)
    parser.add_argument("--pipeline-workers", type=int, default=4, help="Worker threads inside the pipeline per file")
    parser.add_argument("--queue-size", type=int, default=1000, help="Queue maxsize inside the pipeline per file")

    # async-specific knobs (only used if --mode async)
    parser.add_argument("--async-concurrency", type=int, default=20, help="Max number of in-flight API/DB calls per file (async mode)")
    parser.add_argument("--async-chunk-size", type=int, default=500, help="How many rows to buffer before processing them asynchronously (async mode)")

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

    # File-level workers
    file_workers = max(1, int(args.workers))

    # Avoid stacking too many threads: pipeline already spawns (pipeline_workers + 2) threads per file
    if args.mode == "pipeline" and file_workers > 2:
        print(
            f"[WARN] --mode pipeline spawns ~({int(args.pipeline_workers)} + 2) threads per file. "
            f"Capping file-level --workers from {file_workers} to 2 to avoid thread explosion."
        )
        file_workers = 2

    # Optional: discourage process executor + pipeline threads inside each process
    if args.mode == "pipeline" and args.executor == "process":
        print(
            "[WARN] Using ProcessPoolExecutor with --mode pipeline means: processes per file job + threads inside each process. "
            "This can be heavy. Consider --executor thread for pipeline mode."
        )

    # Avoid stacking too much concurrency in async mode:
    # total in-flight calls ≈ file_workers * async_concurrency
    if args.mode == "async" and file_workers > 2:
        print(
            f"[WARN] --mode async runs up to {int(args.async_concurrency)} in-flight calls per file. "
            f"With {file_workers} file workers that can be ~{file_workers * int(args.async_concurrency)} concurrent calls. "
            f"Capping file-level --workers from {file_workers} to 2 to avoid overload."
        )
        file_workers = 2

    # Optional: discourage process executor + async mode (processes * async concurrency)
    if args.mode == "async" and args.executor == "process":
        print(
            f"[WARN] Using ProcessPoolExecutor with --mode async means: processes per file job + async concurrency per process. "
            f"Total in-flight calls can become ~{file_workers * int(args.async_concurrency)}. "
            "This can be heavy. Consider --executor thread for async mode."
        )

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
                mode=args.mode,
                pipeline_workers=max(1, int(args.pipeline_workers)),
                queue_size=max(1, int(args.queue_size)),
                async_concurrency=max(1, int(args.async_concurrency)),
                async_chunk_size=max(1, int(args.async_chunk_size))
            )
        )

    failed = 0
    ExecutorCls = ThreadPoolExecutor if args.executor == "thread" else ProcessPoolExecutor

    with ExecutorCls(max_workers=file_workers) as executor:
        for result in executor.map(run_job, jobs):
            in_file = result.job.in_file
            out_file = result.job.out_file

            if result.ok:
                print(f"[OK] {in_file} -> {out_file}")
            else:
                failed += 1
                print(f"[FAIL] {in_file} -> {out_file}: {result.error}")

    print(
        f"[DONE] jobs={len(jobs)} failed={failed} "
        f"mode={args.mode} executor={args.executor} file_workers={file_workers} "
        f"from {input_dir} -> {output_dir}"
    )


if __name__ == "__main__":
    main()


    