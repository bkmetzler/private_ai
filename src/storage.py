"""SQLite persistence helpers for Bitcoin price candles."""
from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Iterable

import pandas as pd
from sqlalchemy import Float, create_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column
from sqlalchemy.sql.sqltypes import DateTime


class Base(DeclarativeBase):
    """Base class for SQLAlchemy models."""


class BitcoinPrice(Base):
    """SQLAlchemy model for raw BTC price candles."""

    __tablename__ = "bitcoin_prices"

    timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), primary_key=True)
    open: Mapped[float] = mapped_column("open", Float, nullable=False)
    high: Mapped[float] = mapped_column("high", Float, nullable=False)
    low: Mapped[float] = mapped_column("low", Float, nullable=False)
    close: Mapped[float] = mapped_column("close", Float, nullable=False)
    volume: Mapped[float] = mapped_column("volume", Float, nullable=False)


DEFAULT_DB_PATH = Path("data/bitcoin_prices.db")


def save_bitcoin_prices(candles: pd.DataFrame, db_path: Path | str = DEFAULT_DB_PATH) -> None:
    """Persist raw candle rows into SQLite using SQLAlchemy."""

    if candles.empty:
        return

    engine = _create_engine(db_path)
    Base.metadata.create_all(engine)

    records = _dataframe_to_records(candles)
    with Session(engine) as session:
        for record in records:
            session.merge(BitcoinPrice(**record))
        session.commit()


def _create_engine(db_path: Path | str):
    path = Path(db_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    return create_engine(f"sqlite:///{path}", future=True)


def _dataframe_to_records(candles: pd.DataFrame) -> Iterable[dict[str, float | datetime]]:
    for _, row in candles.iterrows():
        timestamp = row["timestamp"]
        if isinstance(timestamp, pd.Timestamp):
            timestamp = timestamp.to_pydatetime()
        yield {
            "timestamp": timestamp,
            "open": float(row["Open"]),
            "high": float(row["High"]),
            "low": float(row["Low"]),
            "close": float(row["Close"]),
            "volume": float(row["Volume"]),
        }
