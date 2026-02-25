

import argparse
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

from core import process_csv


def main() -> None:
    parser = argparse.ArgumentParser(description="Streaming CSV Cleaner (directory mode)")
    parser.add_argument("--input-dir", required=True, help="Folder containing CSV files (searched recursively)")
    parser.add_argument("--output-dir", required=True, help="Folder to write cleaned CSV files into")
    parser.add_argument("--batch-size", type=int, default=2, help="Batch size for writing")
    parser.add_argument("--only-country", default=None, help="Keep only rows matching country (e.g. Germany)")
    parser.add_argument("--threads", type=int, default=4, help="Max amount of parallel executed jobs")
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

    jobs: list[tuple[Path, Path]] = []
    for input_file in csv_files:
        rel_path = input_file.relative_to(input_dir)
        out_subdir = output_dir / rel_path.parent
        out_subdir.mkdir(parents=True, exist_ok=True)

        out_name = f"{input_file.stem}_cleaned{input_file.suffix}"
        output_file = out_subdir / out_name
        jobs.append((input_file, output_file))

    def run_job(job: tuple[Path, Path]) -> tuple[Path, Path]:
        in_file, out_file = job
        process_csv(
            input_path=str(in_file),
            output_path=str(out_file),
            batch_size=args.batch_size,
            only_country=args.only_country,
        )
        return in_file, out_file

    failed = 0

    with ThreadPoolExecutor(max_workers=max(1, int(args.threads))) as executor:
        future_to_job = {executor.submit(run_job, job): job for job in jobs}

        for future in as_completed(future_to_job):
            in_file, out_file = future_to_job[future]
            try:
                future.result()
                print(f"[OK] {in_file} -> {out_file}")
            except Exception as e:
                failed += 1
                print(f"[FAIL] {in_file} -> {out_file}: {e}")

    print(
        f"[DONE] jobs={len(jobs)} failed={failed} threads={max(1, int(args.threads))} "
        f"from {input_dir} -> {output_dir}"
    )


if __name__ == "__main__":
    main()


 