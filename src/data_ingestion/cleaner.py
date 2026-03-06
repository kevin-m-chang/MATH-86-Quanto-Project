"""
src/data_ingestion/cleaner.py
------------------------------
Align and clean the four raw DataFrames produced by loader.py so that they
share a common set of business-day dates and have no structural gaps.

Steps
-----
1. Reindex each DataFrame to a common business-day date range (inner join).
2. Forward-fill up to `ffill_limit` consecutive missing observations (handles
   market holidays where Bloomberg already forward-fills, ensuring consistency).
3. Optionally drop dates where ANY required series is still NaN after filling.
4. Return a single wide DataFrame and the four individual cleaned frames.
"""

from __future__ import annotations

import logging
from typing import Optional

import pandas as pd

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _to_business_day_index(df: pd.DataFrame) -> pd.DataFrame:
    """Ensure the DatetimeIndex contains only business days and is sorted."""
    df = df.copy()
    df.index = pd.to_datetime(df.index)
    df = df[df.index.dayofweek < 5]   # 0=Mon … 4=Fri
    df = df.sort_index()
    return df


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def align_datasets(
    adr_vols: pd.DataFrame,
    local_vols: pd.DataFrame,
    fx_vols: pd.DataFrame,
    fx_spot: pd.DataFrame,
    ffill_limit: int = 3,
    drop_incomplete: bool = True,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Align all four DataFrames to a common business-day date index.

    Parameters
    ----------
    adr_vols, local_vols, fx_vols, fx_spot : pd.DataFrame
        Raw DataFrames from loader.py, each with a DatetimeIndex.
    ffill_limit : int
        Maximum number of consecutive NaN days to forward-fill per column.
    drop_incomplete : bool
        If True, drop any date where at least one column is still NaN after
        forward-filling.

    Returns
    -------
    merged : pd.DataFrame
        Single wide DataFrame with all columns, common date index.
    adr_vols_clean, local_vols_clean, fx_vols_clean, fx_spot_clean : pd.DataFrame
        Individual cleaned DataFrames on the same date index.
    """
    frames = {
        "adr_vols":   _to_business_day_index(adr_vols),
        "local_vols": _to_business_day_index(local_vols),
        "fx_vols":    _to_business_day_index(fx_vols),
        "fx_spot":    _to_business_day_index(fx_spot),
    }

    # Build intersection of all date indices.
    common_index: pd.DatetimeIndex = frames["adr_vols"].index
    for key, df in frames.items():
        common_index = common_index.intersection(df.index)

    if len(common_index) == 0:
        raise ValueError(
            "No overlapping dates across the four datasets. "
            "Check that the Excel files cover the same time period."
        )

    log.info(
        "Common date range: %s → %s  (%d business days)",
        common_index.min().date(),
        common_index.max().date(),
        len(common_index),
    )

    # Reindex, forward-fill, then clip to common index.
    cleaned: dict[str, pd.DataFrame] = {}
    for key, df in frames.items():
        df = df.reindex(common_index)
        df = df.ffill(limit=ffill_limit)
        cleaned[key] = df

    # Merge into one wide DataFrame.
    merged = pd.concat(
        [df.add_prefix(f"{key}__") for key, df in cleaned.items()],
        axis=1,
    )
    # Use un-prefixed columns in the individual frames; keep merged prefixed.
    # Actually, for the merged frame, drop the prefix so column names match
    # the original sheet columns — callers use the individual frames anyway.
    merged = pd.concat(list(cleaned.values()), axis=1)

    if drop_incomplete:
        n_before = len(merged)
        merged = merged.dropna()
        n_dropped = n_before - len(merged)
        if n_dropped:
            log.warning(
                "Dropped %d dates with remaining NaN values after ffill.", n_dropped
            )

    # Re-slice individual frames to the surviving dates.
    surviving = merged.index
    for key in cleaned:
        cleaned[key] = cleaned[key].reindex(surviving)

    log.info(
        "Final aligned dataset: %d dates, %d total columns.",
        len(merged),
        merged.shape[1],
    )

    return (
        merged,
        cleaned["adr_vols"],
        cleaned["local_vols"],
        cleaned["fx_vols"],
        cleaned["fx_spot"],
    )
