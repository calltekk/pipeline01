from __future__ import annotations

from pathlib import Path
import duckdb

DB_PATH = "warehouse/warehouse.duckdb"


def main() -> None:
    out_dir = Path("data/exports")
    out_dir.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(DB_PATH)

    # Export a few tables if they exist
    tables = ["dim_users", "dim_products", "fct_purchases_daily"]

    for t in tables:
        exists = con.execute(
            "select count(*) from information_schema.tables where table_name = ?;",
            [t],
        ).fetchone()[0]
        if not exists:
            print(f"Skipping {t} (not found). Did dbt run?")
            continue

        out_path = out_dir / f"{t}.parquet"
        con.execute(f"copy {t} to '{out_path.as_posix()}' (format parquet);")
        print(f"Exported {t} -> {out_path}")

    con.close()


if __name__ == "__main__":
    main()
