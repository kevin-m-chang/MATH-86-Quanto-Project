"""
src/analysis/implied_correlation.py
-------------------------------------
Compute implied equity-FX correlation from the quanto relationship.

Formula
-------
For each (ADR ticker, local equity ticker, FX pair) triplet and each tenor:

    ρ = (σ_ADR² − σ_local² − σ_FX²) / (2 · σ_local · σ_FX)

where all σ values are implied volatilities in decimal form (e.g. 0.25 = 25%).

This formula is the standard no-arbitrage result linking the volatility of an
ADR (priced in foreign currency) to the volatility of the underlying local
equity and the exchange rate, under the assumption that both marginal processes
are log-normal.

Inputs
------
The function expects three DataFrames on an aligned DatetimeIndex:

  adr_vols   : columns  <ADR_TICKER>_<TENOR>   e.g. VALE_1M
  local_vols : columns  <LOC_TICKER>_<TENOR>   e.g. VALE3_1M
  fx_vols    : columns  <PAIR>_ATM_<TENOR>     e.g. USDBRL_ATM_1M

  (For FX, ATM vol is used in the baseline calculation; wing vols feed the
  skew analysis in a separate module.)

Output
------
A DataFrame with columns:

    <ADR_TICKER>_<LOC_TICKER>_<PAIR>_RHO_<TENOR>

e.g.   VALE_VALE3_USDBRL_RHO_1M

Values are clipped to [-1, 1] after calculation; any NaN inputs propagate
through to NaN outputs.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

import numpy as np
import pandas as pd

log = logging.getLogger(__name__)

TENORS = ("1M", "3M", "1Y")


# ---------------------------------------------------------------------------
# Dataclass to define a (ADR, local, FX) triplet
# ---------------------------------------------------------------------------

@dataclass
class CorrelationSpec:
    """
    Specifies one ADR / local equity / FX pair combination to analyse.

    Attributes
    ----------
    adr_ticker   : Column prefix in `adr_vols`,   e.g. "VALE"
    local_ticker : Column prefix in `local_vols`, e.g. "VALE3"
    fx_pair      : Column prefix in `fx_vols`,    e.g. "USDBRL"
    label        : Optional human-readable label for plots/output.
    """
    adr_ticker:   str
    local_ticker: str
    fx_pair:      str
    label:        str = ""

    def __post_init__(self) -> None:
        if not self.label:
            self.label = f"{self.adr_ticker}/{self.local_ticker}/{self.fx_pair}"


# ---------------------------------------------------------------------------
# Core computation
# ---------------------------------------------------------------------------

def compute_implied_correlation(
    adr_vols: pd.DataFrame,
    local_vols: pd.DataFrame,
    fx_vols: pd.DataFrame,
    specs: list[CorrelationSpec],
    tenors: tuple[str, ...] = TENORS,
    fx_col_template: str = "{pair}_ATM_{tenor}",
    adr_col_template: str = "{ticker}_{tenor}",
    local_col_template: str = "{ticker}_{tenor}",
) -> pd.DataFrame:
    """
    Compute implied correlation ρ for each spec and tenor.

    Parameters
    ----------
    adr_vols, local_vols, fx_vols : pd.DataFrame
        Aligned DataFrames from the cleaning step.
    specs : list[CorrelationSpec]
        List of (ADR, local equity, FX pair) combinations.
    tenors : tuple[str, ...]
        Tenors to compute.
    fx_col_template : str
        Python format string for looking up FX ATM vol columns.
        Available variables: {pair}, {tenor}.
    adr_col_template : str
        Python format string for ADR vol columns.
        Available variables: {ticker}, {tenor}.
    local_col_template : str
        Python format string for local equity vol columns.
        Available variables: {ticker}, {tenor}.

    Returns
    -------
    pd.DataFrame
        DatetimeIndex = shared date range; one column per (spec, tenor).
    """
    results: dict[str, pd.Series] = {}

    for spec in specs:
        for tenor in tenors:
            adr_col   = adr_col_template.format(ticker=spec.adr_ticker,   tenor=tenor)
            local_col = local_col_template.format(ticker=spec.local_ticker, tenor=tenor)
            fx_col    = fx_col_template.format(pair=spec.fx_pair,         tenor=tenor)

            missing: list[str] = []
            if adr_col   not in adr_vols.columns:   missing.append(f"adr_vols[{adr_col}]")
            if local_col not in local_vols.columns: missing.append(f"local_vols[{local_col}]")
            if fx_col    not in fx_vols.columns:    missing.append(f"fx_vols[{fx_col}]")

            if missing:
                log.warning(
                    "Skipping %s %s — missing: %s", spec.label, tenor, missing
                )
                continue

            sigma_adr   = adr_vols[adr_col]
            sigma_local = local_vols[local_col]
            sigma_fx    = fx_vols[fx_col]

            # Guard against zero denominators → NaN.
            denom = 2.0 * sigma_local * sigma_fx
            with np.errstate(divide="ignore", invalid="ignore"):
                rho = (sigma_adr**2 - sigma_local**2 - sigma_fx**2) / denom

            # Clip to valid correlation range and assign NaN where denom ≈ 0.
            rho = rho.where(denom.abs() > 1e-10, other=np.nan)
            rho = rho.clip(-1.0, 1.0)

            col_name = (
                f"{spec.adr_ticker}_{spec.local_ticker}_{spec.fx_pair}"
                f"_RHO_{tenor}"
            )
            rho.name = col_name
            results[col_name] = rho

            log.info(
                "ρ(%s, %s): %d obs, mean=%.4f, std=%.4f",
                spec.label, tenor,
                rho.notna().sum(),
                rho.mean(),
                rho.std(),
            )

    if not results:
        raise ValueError(
            "No correlation series could be computed. "
            "Check CorrelationSpec definitions and column names."
        )

    return pd.DataFrame(results)


# ---------------------------------------------------------------------------
# Diagnostic summary
# ---------------------------------------------------------------------------

def correlation_summary(rho_df: pd.DataFrame) -> pd.DataFrame:
    """
    Return descriptive statistics for all ρ columns.

    Returns
    -------
    pd.DataFrame
        Rows = rho columns, columns = [count, mean, std, min, 25%, 50%, 75%, max].
    """
    stats = rho_df.describe().T
    stats.index.name = "series"
    return stats
