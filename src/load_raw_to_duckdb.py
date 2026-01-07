from __future__ import annotations

from pathlib import Path
import duckdb


DB_PATH = "warehouse/warehouse.duckdb"


def main() -> None:
    Path("warehouse").mkdir(exist_ok=True)

    con = duckdb.connect(DB_PATH)

    con.execute(
        """
        create table if not exists raw_events (
            event_id varchar,
            event_ts varchar,
            user_id varchar,
            event_type varchar,
            product_id varchar,
            price_gbp double,
            session_id varchar,
            _ingested_at timestamp default now()
        );
        """
    )

    raw_dir = Path("data/raw")
    files = sorted(raw_dir.glob("events_*.jsonl"))

    if not files:
        raise SystemExit("No raw files found in data/raw. Run ingest first.")

    # Load all JSONL files (DuckDB can read JSON directly)
    # Use read_json_auto for schema inference; then insert into raw_events
    for fp in files:
        con.execute(
            """
            insert into raw_events (event_id, event_ts, user_id, event_type, product_id, price_gbp, session_id)
            select event_id, event_ts, user_id, event_type, product_id, price_gbp, session_id
            from read_json_auto(?);
            """,
            [str(fp)],
        )
        print(f"Loaded {fp.name}")

    # Optional: basic de-dupe on event_id
    con.execute(
        """
        create or replace table raw_events_dedup as
        select * exclude(rn)
        from (
            select *, row_number() over (partition by event_id order by _ingested_at desc) as rn
            from raw_events
        )
        where rn = 1;
        """
    )

    print("Done. Database:", DB_PATH)
    con.close()


if __name__ == "__main__":
    main()
