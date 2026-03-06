"""
src/data_ingestion/loader.py
-----------------------------
Load Bloomberg-exported data files for:
  - ADR implied volatilities
  - Local equity implied volatilities
  - FX implied volatilities (ATM, 25-delta BF/RR, and individual 25Δ wings)
  - FX spot rates

Supports two source formats
---------------------------
  Excel (.xlsx)  — Bloomberg Excel Add-In exports with one metadata header row.
  CSV   (.csv)   — Bloomberg CSV pastes that contain trailing "Unnamed:" columns
                   carrying embedded BDH formula strings; these are stripped
                   automatically by whitelisting the valid column names.

CSV column whitelists (columns beyond these are silently dropped)
-----------------------------------------------------------------
  ADR  (asml_adr_vols.csv):
      Date, ATM_1M, P25_1M, C25_1M, ATM_3M, ATM_1Y, ADR_SPOT

  Local equity  (asml_loc_vols.csv):
      Date, ATM_1M, P25_1M, C25_1M, ATM_3M, ATM_1Y, LOC_SPOT

  FX  (eurusd_fx_vols.csv):
      Date, ATM_1M, RR_1M, BF_1M, ATM_3M, RR_3M, BF_3M,
      ATM_1Y, RR_1Y, BF_1Y, FX_SPOT

Output column conventions
--------------------------
After loading, all data columns receive a source prefix so that the three
datasets can be merged without name collisions:

  ADR data   →  adr_<col>   e.g.  adr_ATM_1M, adr_P25_1M, adr_ADR_SPOT
  Local data →  loc_<col>   e.g.  loc_ATM_1M, loc_P25_1M, loc_LOC_SPOT
  FX data    →  fx_<col>    e.g.  fx_ATM_1M,  fx_RR_1M,   fx_FX_SPOT

Vol columns are stored in the original Bloomberg units (percentage points,
e.g. 10.5 means 10.5%).  Conversion to decimal is done downstream in the
analysis layer if required.

Spot columns are raw price levels (e.g. EUR/USD = 1.08).
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional

import pandas as pd

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# CSV column whitelists — only these columns are kept after loading.
# All trailing Bloomberg formula/metadata columns ("Unnamed: N") are dropped.
# ---------------------------------------------------------------------------

_ADR_COLS: list[str] = [
    "Date", "ATM_1M", "P25_1M", "C25_1M", "ATM_3M", "ATM_1Y", "ADR_SPOT",
]

_LOC_COLS: list[str] = [
    "Date", "ATM_1M", "P25_1M", "C25_1M", "ATM_3M", "ATM_1Y", "LOC_SPOT",
]

_FX_COLS: list[str] = [
    "Date",
    "ATM_1M", "RR_1M", "BF_1M",
    "ATM_3M", "RR_3M", "BF_3M",
    "ATM_1Y", "RR_1Y", "BF_1Y",
    "FX_SPOT",
]

# Source prefixes applied to all non-Date columns after loading.
_PREFIX_ADR = "adr_"
_PREFIX_LOC = "loc_"
_PREFIX_FX  = "fx_"

# ---------------------------------------------------------------------------
# Internal helpers — Excel  (kept for reference; not used by the CSV pipeline)
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
        header=skip_rows,
        index_col=_DATE_COL,
        parse_dates=True,
        na_values=["#N/A N/A", "N/A", "#N/A", ""],
    )

    df.index.name = "date"
    df.index = pd.to_datetime(df.index, errors="coerce")
    df = df[df.index.notna()]
    df = df.apply(pd.to_numeric, errors="coerce")
    df = df.dropna(how="all")
    df.columns = [str(c).strip().upper() for c in df.columns]

    log.info("  → %d rows, %d columns loaded.", len(df), df.shape[1])
    return df


# ---------------------------------------------------------------------------
# Internal helpers — CSV
# ---------------------------------------------------------------------------

def _read_bbg_csv(
    path: Path,
    valid_cols: list[str],
    prefix: str,
) -> pd.DataFrame:
    """
    Read a Bloomberg-exported CSV, keep only `valid_cols`, parse the Date
    column into a DatetimeIndex, coerce all data columns to float64, and
    apply `prefix` to every non-date column.

    Parameters
    ----------
    path : Path
        Path to the CSV file.
    valid_cols : list[str]
        Ordered list of column names to keep (must include 'Date').
        Any column not in this list — including Bloomberg formula strings
        exported as trailing 'Unnamed: N' columns — is silently dropped.
    prefix : str
        String prepended to every non-date column name, e.g. 'adr_'.

    Returns
    -------
    pd.DataFrame
        DatetimeIndex named 'date'; columns prefixed; values float64.
    """
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"CSV file not found: {path}")

    log.info("Loading %s …", path.name)

    # Read all columns first so we can audit what was dropped.
    raw = pd.read_csv(path, dtype=str)   # read as str; convert below

    all_cols   = raw.columns.tolist()
    keep_cols  = [c for c in valid_cols if c in all_cols]
    drop_cols  = [c for c in all_cols   if c not in valid_cols]

    if drop_cols:
        log.info(
            "  Dropping %d trailing/metadata column(s): %s",
            len(drop_cols),
            drop_cols,
        )

    missing = [c for c in valid_cols if c not in all_cols]
    if missing:
        raise ValueError(
            f"{path.name}: expected columns not found: {missing}\n"
            f"  Available: {all_cols}"
        )

    df = raw[keep_cols].copy()

    # Parse date column → DatetimeIndex.
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
    df = df[df["Date"].notna()].copy()
    df = df.set_index("Date")
    df.index.name = "date"

    # Coerce all remaining columns to float64.
    df = df.apply(pd.to_numeric, errors="coerce")

    # Drop rows that are entirely NaN after coercion (catches formula rows
    # that slipped through as non-date rows).
    df = df.dropna(how="all")

    # Apply source prefix to every column.
    data_cols = [c for c in df.columns]
    df.columns = [f"{prefix}{c}" for c in data_cols]

    log.info(
        "  → %d rows, %d columns  (prefix='%s', date range: %s → %s)",
        len(df), df.shape[1], prefix,
        df.index.min().date(), df.index.max().date(),
    )
    return df


# ---------------------------------------------------------------------------
# Public loaders — Excel  (kept for reference; not used by the CSV pipeline)
# ---------------------------------------------------------------------------

def load_adr_vols(path: str | Path, sheet_name: str | int = 0) -> pd.DataFrame:
    """Load ADR implied vol data from a Bloomberg Excel export."""
    df = _read_bbg_excel(Path(path), sheet_name=sheet_name)
    log.info("ADR vol columns: %s", df.columns.tolist())
    return df


def load_local_vols(path: str | Path, sheet_name: str | int = 0) -> pd.DataFrame:
    """Load local equity implied vol data from a Bloomberg Excel export."""
    df = _read_bbg_excel(Path(path), sheet_name=sheet_name)
    log.info("Local equity vol columns: %s", df.columns.tolist())
    return df


def load_fx_vols(path: str | Path, sheet_name: str | int = 0) -> pd.DataFrame:
    """Load FX implied vol data (ATM, 25Δ BF, 25Δ RR) from a Bloomberg Excel export."""
    df = _read_bbg_excel(Path(path), sheet_name=sheet_name)
    log.info("FX vol columns: %s", df.columns.tolist())
    return df


def load_fx_spot(path: str | Path, sheet_name: str | int = 0) -> pd.DataFrame:
    """Load FX spot rate data from a Bloomberg Excel export."""
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
    Convenience wrapper: load all four datasets from Excel and return as a
    labelled dict with keys: 'adr_vols', 'local_vols', 'fx_vols', 'fx_spot'.
    """
    return {
        "adr_vols":   load_adr_vols(adr_vol_path, sheet_name=adr_sheet),
        "local_vols": load_local_vols(local_vol_path, sheet_name=local_sheet),
        "fx_vols":    load_fx_vols(fx_vol_path, sheet_name=fx_vol_sheet),
        "fx_spot":    load_fx_spot(fx_spot_path, sheet_name=fx_spot_sheet),
    }


# ---------------------------------------------------------------------------
# Public loaders — CSV
# ---------------------------------------------------------------------------

def load_adr_vols_csv(path: str | Path) -> pd.DataFrame:
    """
    Load ADR implied vol data from a Bloomberg CSV export.

    Keeps only: Date, ATM_1M, P25_1M, C25_1M, ATM_3M, ATM_1Y, ADR_SPOT.
    All other columns (Bloomberg formula strings, Unnamed trailing cols) are
    dropped.  Data columns are prefixed with 'adr_'.

    Returns
    -------
    pd.DataFrame
        DatetimeIndex='date', columns: adr_ATM_1M, adr_P25_1M, adr_C25_1M,
        adr_ATM_3M, adr_ATM_1Y, adr_ADR_SPOT.
        Vol values in percentage points as delivered by Bloomberg.
    """
    return _read_bbg_csv(Path(path), valid_cols=_ADR_COLS, prefix=_PREFIX_ADR)


def load_local_vols_csv(path: str | Path) -> pd.DataFrame:
    """
    Load local equity implied vol data from a Bloomberg CSV export.

    Keeps only: Date, ATM_1M, P25_1M, C25_1M, ATM_3M, ATM_1Y, LOC_SPOT.
    Data columns are prefixed with 'loc_'.

    Returns
    -------
    pd.DataFrame
        DatetimeIndex='date', columns: loc_ATM_1M, loc_P25_1M, loc_C25_1M,
        loc_ATM_3M, loc_ATM_1Y, loc_LOC_SPOT.
    """
    return _read_bbg_csv(Path(path), valid_cols=_LOC_COLS, prefix=_PREFIX_LOC)


def load_fx_vols_csv(path: str | Path) -> pd.DataFrame:
    """
    Load FX implied vol and spot data from a Bloomberg CSV export.

    Keeps only: Date, ATM_1M, RR_1M, BF_1M, ATM_3M, RR_3M, BF_3M,
                ATM_1Y, RR_1Y, BF_1Y, FX_SPOT.
    Data columns are prefixed with 'fx_'.

    Returns
    -------
    pd.DataFrame
        DatetimeIndex='date', columns: fx_ATM_1M, fx_RR_1M, fx_BF_1M,
        fx_ATM_3M, fx_RR_3M, fx_BF_3M, fx_ATM_1Y, fx_RR_1Y, fx_BF_1Y,
        fx_FX_SPOT.
    """
    return _read_bbg_csv(Path(path), valid_cols=_FX_COLS, prefix=_PREFIX_FX)


def load_all_csv(
    adr_path: str | Path,
    local_path: str | Path,
    fx_path: str | Path,
) -> dict[str, pd.DataFrame]:
    """
    Convenience wrapper: load all three CSV datasets.

    Returns
    -------
    dict with keys: 'adr', 'local', 'fx'
        Each value is a prefixed, cleaned DataFrame indexed by date.
    """
    return {
        "adr":   load_adr_vols_csv(adr_path),
        "local": load_local_vols_csv(local_path),
        "fx":    load_fx_vols_csv(fx_path),
    }
