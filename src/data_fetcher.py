"""Utility helpers to download Bitcoin prices from Yahoo Finance."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Literal

import pandas as pd
import yfinance as yf

from .storage import DEFAULT_DB_PATH, save_bitcoin_prices


Interval = Literal["1m", "2m", "5m", "15m", "30m", "60m", "90m", "1h"]


@dataclass
class PriceData:
    """Bundle of OHLCV candles and price deltas."""

    candles: pd.DataFrame
    change_index: pd.DataFrame


def download_bitcoin_prices(
    *,
    period: str = "7d",
    interval: Interval = "1m",
    start_date: datetime = datetime.now(tz=timezone.utc),
    db_path: Path | str = DEFAULT_DB_PATH,
) -> PriceData:
    """Download recent Bitcoin candles and compute the change index.

    Parameters
    ----------
    period: str
        Duration to download (e.g. ``"7d"`` or ``"30d"``). ``7d`` is the
        maximum period Yahoo Finance allows for 1-minute candles, which is
        perfect for the requested fingerprint windows.
    interval: Interval
        Candle interval supported by Yahoo Finance.

    Returns
    -------
    PriceData
        A :class:`PriceData` containing the candle dataframe and a dataframe
        with the price deltas for each timestamp.
    """

    ticker = yf.Ticker("BTC-USD")
    df = ticker.history(period=period, interval=interval)
    if df.empty:
        raise RuntimeError(
            "Yahoo Finance returned no data for BTC-USD. Try a different period or interval."
        )

    df = df.tz_convert("UTC") if df.index.tzinfo else df.tz_localize("UTC")
    df = df.rename_axis("timestamp").reset_index()

    start_ts = pd.Timestamp(start_date)
    if start_ts.tzinfo is None:
        start_ts = start_ts.tz_localize("UTC")
    else:
        start_ts = start_ts.tz_convert("UTC")
    df = df[df["timestamp"] >= start_ts]
    if df.empty:
        raise RuntimeError(
            "No Bitcoin candles were available on or after the requested start_date."
        )

    # Index all changes in the close price.
    df["price_change"] = df["Close"].diff().fillna(0.0)

    change_index = df[["timestamp", "price_change"]].copy()

    save_bitcoin_prices(df, db_path=db_path)

    return PriceData(candles=df, change_index=change_index)
