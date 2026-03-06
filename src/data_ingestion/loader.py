"""
src/data_ingestion/loader.py
-----------------------------
Load Bloomberg-exported Excel files for:
  - ADR implied volatilities
  - Local equity implied volatilities
  - FX implied volatilities (ATM, 25-delta BF, 25-delta RR)
  - FX spot rates

Each loader returns a tidy long-format DataFrame indexed by date.

Expected Excel layout (each file):
  - Row 1 (index 0): Bloomberg header / metadata  (skipped)
  - Row 2 (index 1): column names  → used as headers
  - Row 3+ : data rows, first column = date

Column naming conventions expected in the Excel files
------------------------------------------------------
ADR / Local vol files:
    <TICKER>_<MATURITY>   e.g.  VALE_1M, VALE_3M, VALE_1Y

FX vol file columns (all in one sheet or separate sheets):
    <PAIR>_ATM_<TENOR>    e.g.  USDBRL_ATM_1M
    <PAIR>_BF25_<TENOR>   e.g.  USDBRL_BF25_1M   (25-delta butterfly)
    <PAIR>_RR25_<TENOR>   e.g.  USDBRL_RR25_1M   (25-delta risk reversal)

FX spot file columns:
    <PAIR>_SPOT           e.g.  USDBRL_SPOT

All vol columns are expected in decimal form (e.g. 0.25 = 25%).
Spot columns are raw price levels.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional

import pandas as pd

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

_SKIP_ROWS = 1          # Bloomberg Excel exports have one metadata header row
_DATE_COL  = 0          # Date is always in the first column


def _read_bbg_excel(
    path: Path,
    sheet_name: str | int = 0,
    skip_rows: int = _SKIP_ROWS,
) -> pd.DataFrame:
    """
    Read a Bloomberg-exported Excel file and return a clean DataFrame with:
      - A DatetimeIndex named 'date'
      - All remaining columns as float64 (non-parseable values → NaN)
      - Rows that are entirely NaN dropped
    """
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"Excel file not found: {path}")

    log.info("Loading %s (sheet=%s) …", path.name, sheet_name)

    df = pd.read_excel(
        path,
        sheet_name=sheet_name,
        header=skip_rows,          # row after the Bloomberg metadata line
        index_col=_DATE_COL,
        parse_dates=True,
        na_values=["#N/A N/A", "N/A", "#N/A", ""],
    )

    df.index.name = "date"
    df.index = pd.to_datetime(df.index, errors="coerce")

    # Drop the Bloomberg metadata/footer rows that have non-date indices.
    df = df[df.index.notna()]

    # Convert all data columns to float; coerce bad strings to NaN.
    df = df.apply(pd.to_numeric, errors="coerce")

    # Drop rows where every value is NaN.
    df = df.dropna(how="all")

    # Normalise column names: strip whitespace, uppercase.
    df.columns = [str(c).strip().upper() for c in df.columns]

    log.info("  → %d rows, %d columns loaded.", len(df), df.shape[1])
    return df


# ---------------------------------------------------------------------------
# Public loaders
# ---------------------------------------------------------------------------

def load_adr_vols(path: str | Path, sheet_name: str | int = 0) -> pd.DataFrame:
    """
    Load ADR implied volatility data from a Bloomberg Excel export.

    Returns
    -------
    pd.DataFrame
        DatetimeIndex='date', columns like ['VALE_1M', 'VALE_3M', 'VALE_1Y'].
        Values are decimal implied vols (e.g. 0.30 for 30%).
    """
    df = _read_bbg_excel(Path(path), sheet_name=sheet_name)
    log.info("ADR vol columns: %s", df.columns.tolist())
    return df


def load_local_vols(path: str | Path, sheet_name: str | int = 0) -> pd.DataFrame:
    """
    Load local equity implied volatility data from a Bloomberg Excel export.

    Returns
    -------
    pd.DataFrame
        DatetimeIndex='date', columns like ['VALE3_1M', 'VALE3_3M', 'VALE3_1Y'].
        Values are decimal implied vols.
    """
    df = _read_bbg_excel(Path(path), sheet_name=sheet_name)
    log.info("Local equity vol columns: %s", df.columns.tolist())
    return df


def load_fx_vols(path: str | Path, sheet_name: str | int = 0) -> pd.DataFrame:
    """
    Load FX implied volatility data (ATM, 25Δ BF, 25Δ RR) from a Bloomberg
    Excel export.

    Returns
    -------
    pd.DataFrame
        DatetimeIndex='date', columns such as:
            USDBRL_ATM_1M, USDBRL_BF25_1M, USDBRL_RR25_1M
            USDBRL_ATM_3M, USDBRL_BF25_3M, USDBRL_RR25_3M
            USDBRL_ATM_1Y, USDBRL_BF25_1Y, USDBRL_RR25_1Y
        Values are decimal implied vols.
    """
    df = _read_bbg_excel(Path(path), sheet_name=sheet_name)
    log.info("FX vol columns: %s", df.columns.tolist())
    return df


def load_fx_spot(path: str | Path, sheet_name: str | int = 0) -> pd.DataFrame:
    """
    Load FX spot rate data from a Bloomberg Excel export.

    Returns
    -------
    pd.DataFrame
        DatetimeIndex='date', columns like ['USDBRL_SPOT'].
        Values are spot price levels.
    """
    df = _read_bbg_excel(Path(path), sheet_name=sheet_name)
    log.info("FX spot columns: %s", df.columns.tolist())
    return df


def load_all(
    adr_vol_path: str | Path,
    local_vol_path: str | Path,
    fx_vol_path: str | Path,
    fx_spot_path: str | Path,
    adr_sheet: str | int = 0,
    local_sheet: str | int = 0,
    fx_vol_sheet: str | int = 0,
    fx_spot_sheet: str | int = 0,
) -> dict[str, pd.DataFrame]:
    """
    Convenience wrapper: load all four datasets and return as a labelled dict.

    Returns
    -------
    dict with keys: 'adr_vols', 'local_vols', 'fx_vols', 'fx_spot'
    """
    return {
        "adr_vols":   load_adr_vols(adr_vol_path, sheet_name=adr_sheet),
        "local_vols": load_local_vols(local_vol_path, sheet_name=local_sheet),
        "fx_vols":    load_fx_vols(fx_vol_path, sheet_name=fx_vol_sheet),
        "fx_spot":    load_fx_spot(fx_spot_path, sheet_name=fx_spot_sheet),
    }
