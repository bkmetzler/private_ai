"""CLI helper to match current price action to existing fingerprints."""
from __future__ import annotations

import argparse
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List

from .data_fetcher import download_bitcoin_prices
from .fingerprint import Fingerprint, FingerprintGenerator, WINDOWS_MINUTES
from .storage import DEFAULT_DB_PATH


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--fingerprints",
        type=Path,
        default=Path("output/fingerprints.json"),
        help="Path to the fingerprints JSON catalog.",
    )
    parser.add_argument(
        "--window",
        choices=WINDOWS_MINUTES.keys(),
        required=True,
        help="Time window label to match (e.g. 1m, 5m, 1h, 7d).",
    )
    parser.add_argument(
        "--period",
        default="7d",
        help="Yahoo Finance period argument used for fetching candles (default: 7d).",
    )
    parser.add_argument(
        "--interval",
        default="1m",
        help="Yahoo Finance interval argument used for fetching candles (default: 1m).",
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
    latest_fp = _generate_latest_fingerprint(
        window=args.window,
        period=args.period,
        interval=args.interval,
        db_path=args.db_path,
    )

    catalog = _load_catalog(args.fingerprints)
    match = _find_matching_fingerprint(catalog["fingerprints"], latest_fp["fingerprint_id"])

    if match:
        print(
            "Found matching fingerprint for window {window}: {fingerprint_id}".format(
                window=args.window, fingerprint_id=match["fingerprint_id"]
            )
        )
    else:
        catalog["fingerprints"].append(latest_fp)
        _save_catalog(args.fingerprints, catalog)
        print(
            "Added new fingerprint for window {window}: {fingerprint_id}".format(
                window=args.window, fingerprint_id=latest_fp["fingerprint_id"]
            )
        )


def _generate_latest_fingerprint(
    *, window: str, period: str, interval: str, db_path: Path | str
) -> Dict[str, Any]:
    window_minutes = WINDOWS_MINUTES[window]
    start_date = datetime.now(tz=timezone.utc) - timedelta(minutes=window_minutes)
    price_data = download_bitcoin_prices(
        period=period,
        interval=interval,
        start_date=start_date,
        db_path=db_path,
    )

    generator = FingerprintGenerator(price_data.candles)
    fingerprints = generator.generate([window])
    if not fingerprints:
        raise RuntimeError(
            f"Unable to generate fingerprints for window '{window}'. Not enough candles were returned."
        )

    latest = fingerprints[-1]
    return _fingerprint_to_dict(latest)


def _fingerprint_to_dict(fp: Fingerprint) -> Dict[str, Any]:
    return {
        "window": fp.window_label,
        "start": fp.start_timestamp.isoformat(),
        "end": fp.end_timestamp.isoformat(),
        "duration_minutes": fp.duration_minutes,
        "start_close": fp.start_close,
        "end_close": fp.end_close,
        "absolute_change": fp.absolute_change,
        "pct_change": fp.pct_change,
        "volatility": fp.volatility,
        "average_volume": fp.average_volume,
        "fingerprint_id": fp.fingerprint_id,
    }


def _load_catalog(path: Path) -> Dict[str, List[Dict[str, Any]]]:
    if not path.exists():
        return {"fingerprints": [], "price_changes": []}

    try:
        data = json.loads(path.read_text())
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"Unable to parse fingerprint catalog at {path}") from exc

    fingerprints = data.get("fingerprints") or []
    price_changes = data.get("price_changes") or []
    return {"fingerprints": fingerprints, "price_changes": price_changes}


def _save_catalog(path: Path, catalog: Dict[str, List[Dict[str, Any]]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(catalog, indent=2))


def _find_matching_fingerprint(
    fingerprints: List[Dict[str, Any]], fingerprint_id: str
) -> Dict[str, Any] | None:
    for fp in fingerprints:
        if fp.get("fingerprint_id") == fingerprint_id:
            return fp
    return None


if __name__ == "__main__":
    main()
