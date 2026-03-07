"""
src/features/fx_vol_surface.py
--------------------------------
Reconstruct FX 25-delta call and put implied vols from the ATM, butterfly,
and risk-reversal quotes that Bloomberg provides.

Conventions (Garman-Kohlhagen / market standard)
-------------------------------------------------
Given for a single tenor:
    ATM  : at-the-money (delta-neutral straddle) implied vol
    BF25 : 25-delta butterfly spread  = (σ_25c + σ_25p)/2 − ATM
    RR25 : 25-delta risk reversal     = σ_25c − σ_25p

Solving:
    σ_25c = ATM + BF25 + RR25/2
    σ_25p = ATM + BF25 − RR25/2

All vols are in decimal (e.g. 0.12 = 12%).

Column naming convention expected in `fx_vols` DataFrame
---------------------------------------------------------
    <PAIR>_ATM_<TENOR>   e.g. USDBRL_ATM_1M
    <PAIR>_BF25_<TENOR>  e.g. USDBRL_BF25_1M
    <PAIR>_RR25_<TENOR>  e.g. USDBRL_RR25_1M

Output columns added to the returned DataFrame
-----------------------------------------------
    <PAIR>_25C_<TENOR>   e.g. USDBRL_25C_1M
    <PAIR>_25P_<TENOR>   e.g. USDBRL_25P_1M
"""

from __future__ import annotations

import logging
import re

import pandas as pd

log = logging.getLogger(__name__)

# Tenors the pipeline handles (must match suffixes in the input columns).
TENORS = ("1M", "3M", "1Y")


def reconstruct_wing_vols(
    fx_vols: pd.DataFrame,
    pairs: list[str] | None = None,
    tenors: tuple[str, ...] = TENORS,
) -> pd.DataFrame:
    """
    Add 25-delta call (25C) and put (25P) vol columns to `fx_vols`.

    Parameters
    ----------
    fx_vols : pd.DataFrame
        DataFrame with columns following the naming convention described above.
        DatetimeIndex expected.
    pairs : list[str] | None
        List of currency pair prefixes, e.g. ['USDBRL'].  If None, pairs are
        inferred automatically from columns matching *_ATM_* pattern.
    tenors : tuple[str, ...]
        Tenors to process (default: 1M, 3M, 1Y).

    Returns
    -------
    pd.DataFrame
        Original DataFrame with additional <PAIR>_25C_<TENOR> and
        <PAIR>_25P_<TENOR> columns appended.
    """
    df = fx_vols.copy()

    if pairs is None:
        # Auto-detect pairs from column names matching <PAIR>_ATM_<TENOR>.
        atm_pattern = re.compile(r"^(.+)_ATM_(.+)$")
        pairs = list(
            {m.group(1) for col in df.columns if (m := atm_pattern.match(col))}
        )
        log.info("Auto-detected FX pairs: %s", pairs)

    added: list[str] = []
    for pair in pairs:
        for tenor in tenors:
            atm_col  = f"{pair}_ATM_{tenor}"
            bf_col   = f"{pair}_BF25_{tenor}"
            rr_col   = f"{pair}_RR25_{tenor}"

            missing = [c for c in (atm_col, bf_col, rr_col) if c not in df.columns]
            if missing:
                log.warning(
                    "Skipping %s %s — missing columns: %s", pair, tenor, missing
                )
                continue

            atm = df[atm_col]
            bf  = df[bf_col]
            rr  = df[rr_col]

            df[f"{pair}_25C_{tenor}"] = atm + bf + rr / 2.0
            df[f"{pair}_25P_{tenor}"] = atm + bf - rr / 2.0
            added.extend([f"{pair}_25C_{tenor}", f"{pair}_25P_{tenor}"])

    log.info("Wing vol columns added: %s", added)
    return df


def reconstruct_wing_vols_from_columns(
    df: pd.DataFrame,
    atm_col: str,
    bf_col: str,
    rr_col: str,
    call_col: str = "call_vol",
    put_col: str = "put_vol",
) -> pd.DataFrame:
    """
    Reconstruct 25-delta call and put vols from explicit column names.

    This avoids the naming-convention coupling of ``reconstruct_wing_vols``
    and works directly with any ATM / BF / RR column names.

    Parameters
    ----------
    df : pd.DataFrame
        Must contain *atm_col*, *bf_col*, and *rr_col*.
    atm_col, bf_col, rr_col : str
        Column names for ATM vol, butterfly spread, and risk reversal.
    call_col, put_col : str
        Names for the two new output columns.

    Returns
    -------
    pd.DataFrame
        Copy of *df* with *call_col* and *put_col* appended.
    """
    out = df.copy()
    atm = df[atm_col]
    bf = df[bf_col]
    rr = df[rr_col]
    out[call_col] = atm + bf + rr / 2.0
    out[put_col] = atm + bf - rr / 2.0
    return out
