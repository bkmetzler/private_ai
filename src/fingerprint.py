"""Fingerprint generator for Bitcoin price movements."""
from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import Iterable, List

import pandas as pd

WINDOWS_MINUTES = {
    "1m": 1,
    "5m": 5,
    "10m": 10,
    "30m": 30,
    "1h": 60,
    "6h": 6 * 60,
    "12h": 12 * 60,
    "24h": 24 * 60,
    "7d": 7 * 24 * 60,
}


@dataclass
class Fingerprint:
    """Represents the summarized movement for a time window."""

    window_label: str
    start_timestamp: pd.Timestamp
    end_timestamp: pd.Timestamp
    duration_minutes: int
    start_close: float
    end_close: float
    absolute_change: float
    pct_change: float
    volatility: float
    average_volume: float
    fingerprint_id: str


class FingerprintGenerator:
    """Create fingerprints for a dataframe of BTC candles."""

    def __init__(self, candles: pd.DataFrame):
        self._candles = candles.copy()
        self._candles.sort_values("timestamp", inplace=True)
        self._candles.set_index("timestamp", inplace=True)

    def generate(self, window_labels: Iterable[str]) -> List[Fingerprint]:
        fingerprints: List[Fingerprint] = []
        for label in window_labels:
            window_minutes = WINDOWS_MINUTES[label]
            if len(self._candles) < window_minutes:
                continue

            for end_idx in range(window_minutes - 1, len(self._candles)):
                frame = self._candles.iloc[end_idx - window_minutes + 1 : end_idx + 1]
                if frame.empty:
                    continue

                start_ts = frame.index[0]
                end_ts = frame.index[-1]
                start_close = float(frame["Close"].iloc[0])
                end_close = float(frame["Close"].iloc[-1])
                absolute_change = end_close - start_close
                pct_change = (absolute_change / start_close) * 100 if start_close else 0.0
                volatility = float(frame["Close"].std(ddof=0))
                average_volume = float(frame["Volume"].mean())
                fingerprint_id = _hash_fingerprint(
                    label,
                    start_ts.isoformat(),
                    end_ts.isoformat(),
                    start_close,
                    end_close,
                    volatility,
                )
                fingerprints.append(
                    Fingerprint(
                        window_label=label,
                        start_timestamp=start_ts,
                        end_timestamp=end_ts,
                        duration_minutes=window_minutes,
                        start_close=start_close,
                        end_close=end_close,
                        absolute_change=absolute_change,
                        pct_change=pct_change,
                        volatility=volatility,
                        average_volume=average_volume,
                        fingerprint_id=fingerprint_id,
                    )
                )
        return fingerprints


def fingerprints_to_dataframe(fingerprints: Iterable[Fingerprint]) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "window": fp.window_label,
                "start": fp.start_timestamp,
                "end": fp.end_timestamp,
                "duration_minutes": fp.duration_minutes,
                "start_close": fp.start_close,
                "end_close": fp.end_close,
                "absolute_change": fp.absolute_change,
                "pct_change": fp.pct_change,
                "volatility": fp.volatility,
                "average_volume": fp.average_volume,
                "fingerprint_id": fp.fingerprint_id,
            }
            for fp in fingerprints
        ]
    )


def _hash_fingerprint(*values: object) -> str:
    hasher = hashlib.sha256()
    for value in values:
        hasher.update(str(value).encode("utf-8"))
    return hasher.hexdigest()
