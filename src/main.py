"""CLI entry-point for generating Bitcoin price fingerprints."""
from __future__ import annotations

import argparse
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Sequence

import pandas as pd

from .data_fetcher import download_bitcoin_prices
from .fingerprint import FingerprintGenerator, WINDOWS_MINUTES, fingerprints_to_dataframe
from .storage import DEFAULT_DB_PATH


WINDOW_ORDER: Sequence[str] = list(WINDOWS_MINUTES.keys())


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("output/fingerprints.json"),
        help="Path where the fingerprint catalog should be stored (JSON).",
    )
    parser.add_argument(
        "--period",
        default="7d",
        help="Yahoo Finance period argument (default: 7d).",
    )
    parser.add_argument(
        "--interval",
        default="1m",
        help="Yahoo Finance interval argument (default: 1m).",
    )
    parser.add_argument(
        "--start-date",
        dest="start_date",
        help=(
            "ISO-8601 timestamp indicating the earliest candle to download. "
            "Defaults to now minus the requested period."
        ),
    )
    parser.add_argument(
        "--db-path",
        dest="db_path",
        default=DEFAULT_DB_PATH,
        type=Path,
        help="Location of the SQLite database used to archive raw candles.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    start_date = _resolve_start_date(args.start_date, args.period)
    price_data = download_bitcoin_prices(
        period=args.period,
        interval=args.interval,
        start_date=start_date,
        db_path=args.db_path,
    )

    generator = FingerprintGenerator(price_data.candles)
    fingerprints = generator.generate(WINDOW_ORDER)
    fingerprint_df = fingerprints_to_dataframe(fingerprints)

    _print_preview(fingerprint_df)
    _save_output(args.output, fingerprint_df, price_data.change_index)


def _print_preview(df: pd.DataFrame, rows: int = 5) -> None:
    print("Fingerprint preview:")
    with pd.option_context("display.max_columns", None):
        print(df.head(rows).to_string(index=False))


def _save_output(output_path: Path, fp_df: pd.DataFrame, change_df: pd.DataFrame) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "fingerprints": fp_df.to_dict(orient="records"),
        "price_changes": change_df.to_dict(orient="records"),
    }
    output_path.write_text(json.dumps(payload, indent=2, default=str))
    print(f"Saved {len(fp_df)} fingerprints to {output_path}")


def _resolve_start_date(value: str | None, period: str) -> datetime:
    if value:
        parsed = datetime.fromisoformat(value)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        else:
            parsed = parsed.astimezone(timezone.utc)
        return parsed

    delta = _period_to_timedelta(period)
    return datetime.now(tz=timezone.utc) - delta


def _period_to_timedelta(period: str) -> timedelta:
    unit_multipliers = {
        "m": timedelta(minutes=1),
        "h": timedelta(hours=1),
        "d": timedelta(days=1),
        "w": timedelta(weeks=1),
        "y": timedelta(days=365),
    }
    count = "".join(ch for ch in period if ch.isdigit())
    unit = period[len(count):] or "d"
    if not count or unit not in unit_multipliers:
        raise ValueError(
            f"Unable to parse period '{period}'. Expected formats like '7d', '24h', or '60m'."
        )
    return int(count) * unit_multipliers[unit]


if __name__ == "__main__":
    main()
