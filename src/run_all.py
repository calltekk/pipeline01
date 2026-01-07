from __future__ import annotations

import subprocess
import sys


def run(cmd: list[str]) -> None:
    print("\n>>", " ".join(cmd))
    subprocess.check_call(cmd)


def main() -> None:
    run([sys.executable, "-m", "src.ingest_generate_events"])
    run([sys.executable, "-m", "src.load_raw_to_duckdb"])

    # dbt commands (run from dbt/ directory)
    run(["dbt", "deps", "--project-dir", "dbt"])
    run(["dbt", "run", "--project-dir", "dbt", "--profiles-dir", "profiles"])

    run([sys.executable, "-m", "src.export_parquet"])


if __name__ == "__main__":
    main()
