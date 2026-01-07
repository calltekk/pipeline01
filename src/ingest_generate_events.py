from __future__ import annotations

import json
import random
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import uuid4


@dataclass
class Event:
    event_id: str
    event_ts: str
    user_id: str
    event_type: str
    product_id: str | None
    price_gbp: float | None
    session_id: str


EVENT_TYPES = ["page_view", "add_to_cart", "purchase"]
PRODUCT_IDS = [f"SKU-{i:04d}" for i in range(1, 301)]


def generate_events(n: int, start_ts: datetime, minutes_span: int) -> list[Event]:
    events: list[Event] = []

    for _ in range(n):
        user_id = f"U-{random.randint(1, 500):04d}"
        session_id = str(uuid4())
        event_type = random.choices(EVENT_TYPES, weights=[80, 15, 5], k=1)[0]

        dt = start_ts + timedelta(minutes=random.randint(0, minutes_span))
        product_id = random.choice(PRODUCT_IDS) if event_type != "page_view" else None

        price = None
        if event_type == "purchase":
            price = round(random.uniform(4.99, 199.99), 2)

        events.append(
            Event(
                event_id=str(uuid4()),
                event_ts=dt.replace(tzinfo=timezone.utc).isoformat(),
                user_id=user_id,
                event_type=event_type,
                product_id=product_id,
                price_gbp=price,
                session_id=session_id,
            )
        )

    return events


def main() -> None:
    random.seed(42)

    out_dir = Path("data/raw")
    out_dir.mkdir(parents=True, exist_ok=True)

    # One file per “run”
    run_ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    out_path = out_dir / f"events_{run_ts}.jsonl"

    start_ts = datetime.now(timezone.utc) - timedelta(days=1)
    events = generate_events(n=5000, start_ts=start_ts, minutes_span=24 * 60)

    with out_path.open("w", encoding="utf-8") as f:
        for e in events:
            f.write(json.dumps(asdict(e)) + "\n")

    print(f"Wrote {len(events)} events to {out_path}")


if __name__ == "__main__":
    main()
