"""
src/features/skew.py
---------------------
Compute equity skew variables from the ADR and local-equity vol datasets.

Definition
----------
For each instrument and tenor:

    equity_skew = σ_25p − σ_25c
               = (put_vol at 25-delta) − (call_vol at 25-delta)

where σ_25p and σ_25c are the 25-delta put and call implied vols respectively.

Bloomberg option vol columns are typically provided as:
    <TICKER>_25P_<TENOR>   e.g.  VALE_25P_1M
    <TICKER>_25C_<TENOR>   e.g.  VALE_25C_1M

If only ATM and RR25 are provided without individual wings, equity skew can
be approximated as:
    equity_skew ≈ −RR25   (since RR = σ_25c − σ_25p → skew = −RR)

This module supports both conventions; pass `use_rr_approximation=True` if only
risk-reversal data is available.

Column naming convention expected
----------------------------------
Wing-vol convention (preferred):
    <TICKER>_25P_<TENOR>
    <TICKER>_25C_<TENOR>

Risk-reversal approximation:
    <TICKER>_RR25_<TENOR>

Output columns
--------------
    <TICKER>_SKEW_<TENOR>
"""

from __future__ import annotations

import logging
import re

import pandas as pd

log = logging.getLogger(__name__)

TENORS = ("1M", "3M", "1Y")


def compute_equity_skew(
    vol_df: pd.DataFrame,
    tickers: list[str] | None = None,
    tenors: tuple[str, ...] = TENORS,
    use_rr_approximation: bool = False,
) -> pd.DataFrame:
    """
    Compute equity skew for each ticker / tenor combination.

    Parameters
    ----------
    vol_df : pd.DataFrame
        DataFrame containing implied vol columns (ADR or local equity).
        DatetimeIndex expected.
    tickers : list[str] | None
        Ticker prefixes, e.g. ['VALE', 'PBR'].  Auto-detected if None.
    tenors : tuple[str, ...]
        Tenors to process.
    use_rr_approximation : bool
        If True, use  skew = -RR25  instead of p25 - c25.

    Returns
    -------
    pd.DataFrame
        New DataFrame (same DatetimeIndex) containing only the skew columns.
    """
    df = vol_df.copy()

    if tickers is None:
        if use_rr_approximation:
            pattern = re.compile(r"^(.+)_RR25_(.+)$")
        else:
            pattern = re.compile(r"^(.+)_25P_(.+)$")

        tickers = list(
            {m.group(1) for col in df.columns if (m := pattern.match(col))}
        )
        log.info("Auto-detected equity tickers: %s", tickers)

    skew_frames: list[pd.Series] = []

    for ticker in tickers:
        for tenor in tenors:
            if use_rr_approximation:
                rr_col = f"{ticker}_RR25_{tenor}"
                if rr_col not in df.columns:
                    log.warning("Missing column %s — skipping.", rr_col)
                    continue
                skew_series = -df[rr_col]
            else:
                p_col = f"{ticker}_25P_{tenor}"
                c_col = f"{ticker}_25C_{tenor}"
                missing = [c for c in (p_col, c_col) if c not in df.columns]
                if missing:
                    log.warning("Missing columns %s — skipping.", missing)
                    continue
                skew_series = df[p_col] - df[c_col]

            skew_series.name = f"{ticker}_SKEW_{tenor}"
            skew_frames.append(skew_series)

    if not skew_frames:
        log.warning(
            "No skew columns could be computed. "
            "Check column names and the use_rr_approximation flag."
        )
        return pd.DataFrame(index=df.index)

    return pd.concat(skew_frames, axis=1)
