#!/usr/bin/env python3
"""Generate deterministic mock data for ADP testing.

Uses only stdlib modules — no random, no external dependencies.
All values computed from deterministic arithmetic formulas.
"""

import csv
import math
from datetime import datetime, timedelta
from pathlib import Path

MOCK_DIR = Path(__file__).resolve().parent.parent / "data" / "mock_data"


def generate_ohlcv() -> list[dict[str, str]]:
    """Generate 3,600 OHLCV rows for BTCUSDT — 1-second bars with ms-precision timestamps.

    Price walk: sine wave (period ~15min) + linear drift from 42,150.00.
    """
    rows: list[dict[str, str]] = []
    base_time = datetime(2026, 1, 15, 10, 0, 0)
    base_price = 42150.0

    for i in range(3600):
        ts = base_time + timedelta(seconds=i)
        mid = base_price + 50.0 * math.sin(i * 2 * math.pi / 900) + 0.05 * i

        open_p = round(mid + ((i * 7) % 13) * 0.1 - 0.65, 2)
        high_p = round(mid + ((i * 17) % 13) * 0.5 + 1.0, 2)
        low_p = round(mid - ((i * 13) % 11) * 0.5 - 1.0, 2)
        close_p = round(mid + ((i * 31) % 17) * 0.3 - 2.55, 2)

        high_p = round(max(high_p, open_p, close_p) + 0.01, 2)
        low_p = round(min(low_p, open_p, close_p) - 0.01, 2)

        vol = round(0.5 + (i * 23 % 100) / 10.0, 4)

        rows.append({
            "timestamp": ts.strftime("%Y-%m-%d %H:%M:%S.") + f"{ts.microsecond // 1000:03d}",
            "symbol": "BTCUSDT",
            "open": f"{open_p:.2f}",
            "high": f"{high_p:.2f}",
            "low": f"{low_p:.2f}",
            "close": f"{close_p:.2f}",
            "volume": f"{vol:.4f}",
        })

    return rows


def generate_rfq_events() -> list[dict[str, str]]:
    """Generate 20 RFQ lifecycles with 2-4 events each (~60 rows)."""
    counterparties = ["citadel", "jump", "wintermute", "flow_traders"]
    instruments = ["BTCUSDT", "BTCUSDT", "ETHUSDT", "BTCUSDT",
                   "ETHUSDT", "BTCUSDT", "BTCUSDT", "ETHUSDT",
                   "BTCUSDT", "ETHUSDT", "BTCUSDT", "BTCUSDT",
                   "ETHUSDT", "BTCUSDT", "BTCUSDT", "ETHUSDT",
                   "BTCUSDT", "ETHUSDT", "BTCUSDT", "BTCUSDT"]
    sides = ["buy", "sell", "buy", "buy", "sell",
             "sell", "buy", "buy", "sell", "buy",
             "buy", "sell", "buy", "sell", "buy",
             "sell", "buy", "buy", "sell", "buy"]
    quantities = [0.5, 1.0, 5.0, 0.25, 10.0,
                  2.0, 0.75, 8.0, 1.5, 3.0,
                  0.1, 4.0, 0.5, 2.5, 1.0,
                  6.0, 0.3, 7.0, 1.0, 0.8]
    # 14 accepted, 6 rejected (indices 3, 7, 11, 14, 17, 19 are rejected)
    rejected_indices = {3, 7, 11, 14, 17, 19}

    base_prices_btc = [42150.0 + i * 5.0 for i in range(20)]
    base_prices_eth = [3250.0 + i * 2.0 for i in range(20)]

    rows: list[dict[str, str]] = []
    event_counter = 0
    base_time = datetime(2026, 1, 15, 9, 0, 0)

    for rfq_idx in range(20):
        rfq_id = f"RFQ_{rfq_idx + 1:03d}"
        instrument = instruments[rfq_idx]
        side = sides[rfq_idx]
        qty = quantities[rfq_idx]
        cp = counterparties[rfq_idx % 4]
        is_rejected = rfq_idx in rejected_indices

        base_price = base_prices_btc[rfq_idx] if "BTC" in instrument else base_prices_eth[rfq_idx]
        spread_bps = 5.0 + (rfq_idx * 3 % 10)
        quoted_price = round(base_price * (1 + spread_bps / 10000.0), 2)

        # Event 1: requested
        event_counter += 1
        req_time = base_time + timedelta(minutes=rfq_idx * 6, seconds=rfq_idx * 13 % 30)
        rows.append({
            "event_id": f"EVT_{event_counter:04d}",
            "rfq_id": rfq_id,
            "instrument": instrument,
            "side": side,
            "requested_qty": f"{qty:.4f}",
            "counterparty": cp,
            "timestamp": req_time.strftime("%Y-%m-%d %H:%M:%S.") + f"{(rfq_idx * 137 % 1000):03d}",
            "status": "requested",
            "quoted_price": "",
        })

        # Event 2: quoted (15-45s after request)
        event_counter += 1
        quote_time = req_time + timedelta(seconds=15 + rfq_idx * 2)
        rows.append({
            "event_id": f"EVT_{event_counter:04d}",
            "rfq_id": rfq_id,
            "instrument": instrument,
            "side": side,
            "requested_qty": f"{qty:.4f}",
            "counterparty": cp,
            "timestamp": quote_time.strftime("%Y-%m-%d %H:%M:%S.") + f"{(rfq_idx * 251 % 1000):03d}",
            "status": "quoted",
            "quoted_price": f"{quoted_price:.2f}",
        })

        # Event 3: accepted or rejected (5-20s after quote)
        event_counter += 1
        decision_time = quote_time + timedelta(seconds=5 + rfq_idx * 3 % 15)
        final_status = "rejected" if is_rejected else "accepted"
        rows.append({
            "event_id": f"EVT_{event_counter:04d}",
            "rfq_id": rfq_id,
            "instrument": instrument,
            "side": side,
            "requested_qty": f"{qty:.4f}",
            "counterparty": cp,
            "timestamp": decision_time.strftime("%Y-%m-%d %H:%M:%S.") + f"{(rfq_idx * 389 % 1000):03d}",
            "status": final_status,
            "quoted_price": f"{quoted_price:.2f}",
        })

    return rows


def generate_trade_records(rfq_events: list[dict[str, str]]) -> list[dict[str, str]]:
    """Generate trade records from accepted RFQs."""
    accepted = [e for e in rfq_events if e["status"] == "accepted"]
    rows: list[dict[str, str]] = []

    for idx, evt in enumerate(accepted):
        # Parse the acceptance timestamp
        ts_str = evt["timestamp"]
        ts = datetime.strptime(ts_str[:23], "%Y-%m-%d %H:%M:%S.%f")

        # Execution: 1-5 seconds after acceptance
        exec_offset = 1 + (idx * 3 % 5)
        exec_ts = ts + timedelta(seconds=exec_offset)

        # Settlement: T+2 business days from execution date
        exec_date = exec_ts.date()
        settle_date = exec_date + timedelta(days=2)
        # Skip weekends
        while settle_date.weekday() >= 5:
            settle_date += timedelta(days=1)

        # Price: quoted price with slight slippage (0-2 bps deterministic)
        quoted = float(evt["quoted_price"])
        slippage_bps = (idx * 7 % 3) * 0.5  # 0, 0.5, or 1.0 bps
        side = evt["side"]
        if side == "buy":
            exec_price = round(quoted * (1 + slippage_bps / 10000.0), 2)
        else:
            exec_price = round(quoted * (1 - slippage_bps / 10000.0), 2)

        rows.append({
            "trade_id": f"TRD_{idx + 1:03d}",
            "rfq_id": evt["rfq_id"],
            "instrument": evt["instrument"],
            "side": side,
            "price": f"{exec_price:.2f}",
            "quantity": evt["requested_qty"],
            "counterparty": evt["counterparty"],
            "execution_timestamp": exec_ts.strftime("%Y-%m-%d %H:%M:%S.") + f"{(idx * 173 % 1000):03d}",
            "settlement_date": settle_date.strftime("%Y-%m-%d"),
        })

    return rows


def write_csv(path: Path, rows: list[dict[str, str]]) -> None:
    """Write rows to CSV."""
    if not rows:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)
    print(f"  Wrote {len(rows)} rows to {path}")


def main() -> None:
    print("Generating mock data...")

    ohlcv = generate_ohlcv()
    write_csv(MOCK_DIR / "ohlcv_btcusdt_1s.csv", ohlcv)

    rfq_events = generate_rfq_events()
    write_csv(MOCK_DIR / "rfq_events.csv", rfq_events)

    trades = generate_trade_records(rfq_events)
    write_csv(MOCK_DIR / "trade_records.csv", trades)

    print("Done.")


if __name__ == "__main__":
    main()
