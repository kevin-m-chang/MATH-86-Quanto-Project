"""
src/analysis/build_dataset.py
-------------------------------
Assemble the final derived dataset by combining:

  1. Aligned raw vol series (ADR, local equity, FX ATM)
  2. Reconstructed FX wing vols (25C, 25P) from ATM + BF + RR
  3. Computed implied correlations ρ by tenor
  4. Equity skew variables by ticker and tenor

Saves the result to  data/processed/derived_dataset.csv

Usage
-----
    from src.analysis.build_dataset import build_and_save_dataset

    build_and_save_dataset(
        adr_vol_path   = "data/raw/adr_vols.xlsx",
        local_vol_path = "data/raw/local_vols.xlsx",
        fx_vol_path    = "data/raw/fx_vols.xlsx",
        fx_spot_path   = "data/raw/fx_spot.xlsx",
        specs          = [
            CorrelationSpec(adr_ticker="VALE", local_ticker="VALE3", fx_pair="USDBRL"),
        ],
    )
"""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

from src.data_ingestion.loader  import load_all
from src.data_ingestion.cleaner import align_datasets
from src.features.fx_vol_surface import reconstruct_wing_vols
from src.features.skew           import compute_equity_skew
from src.analysis.implied_correlation import (
    CorrelationSpec,
    compute_implied_correlation,
    correlation_summary,
)

log = logging.getLogger(__name__)

_PROJECT_ROOT   = Path(__file__).resolve().parent.parent.parent
_OUTPUT_PATH    = _PROJECT_ROOT / "data" / "processed" / "derived_dataset.csv"

TENORS = ("1M", "3M", "1Y")


def build_and_save_dataset(
    adr_vol_path:   str | Path,
    local_vol_path: str | Path,
    fx_vol_path:    str | Path,
    fx_spot_path:   str | Path,
    specs:          list[CorrelationSpec],
    output_path:    str | Path | None = None,
    ffill_limit:    int = 3,
    tenors:         tuple[str, ...] = TENORS,
    adr_use_rr_skew:   bool = False,
    local_use_rr_skew: bool = False,
) -> pd.DataFrame:
    """
    Full pipeline: load → clean → features → analysis → save.

    Parameters
    ----------
    adr_vol_path, local_vol_path, fx_vol_path, fx_spot_path : path-like
        Paths to Bloomberg-exported Excel files.
    specs : list[CorrelationSpec]
        ADR / local equity / FX pair triplets to analyse.
    output_path : path-like | None
        Destination for the derived CSV.  Defaults to
        data/processed/derived_dataset.csv.
    ffill_limit : int
        Forward-fill limit passed to align_datasets().
    tenors : tuple[str, ...]
        Tenors to process (default 1M, 3M, 1Y).
    adr_use_rr_skew, local_use_rr_skew : bool
        Whether to use -RR25 as the skew approximation for ADR / local
        equity data (use when individual wing vols are unavailable).

    Returns
    -------
    pd.DataFrame
        The assembled derived dataset (also written to CSV).
    """
    if output_path is None:
        output_path = _OUTPUT_PATH

    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    # 1. Load raw Excel exports
    # ------------------------------------------------------------------
    log.info("=== Step 1: Loading raw data ===")
    raw = load_all(
        adr_vol_path   = adr_vol_path,
        local_vol_path = local_vol_path,
        fx_vol_path    = fx_vol_path,
        fx_spot_path   = fx_spot_path,
    )

    # ------------------------------------------------------------------
    # 2. Align and clean
    # ------------------------------------------------------------------
    log.info("=== Step 2: Aligning datasets ===")
    _, adr_vols, local_vols, fx_vols, fx_spot = align_datasets(
        adr_vols   = raw["adr_vols"],
        local_vols = raw["local_vols"],
        fx_vols    = raw["fx_vols"],
        fx_spot    = raw["fx_spot"],
        ffill_limit = ffill_limit,
        drop_incomplete = False,   # keep partial rows; NaNs propagate to outputs
    )

    # ------------------------------------------------------------------
    # 3. Reconstruct FX wing vols
    # ------------------------------------------------------------------
    log.info("=== Step 3: Reconstructing FX wing vols ===")
    fx_vols_with_wings = reconstruct_wing_vols(fx_vols)

    # ------------------------------------------------------------------
    # 4. Compute implied correlations
    # ------------------------------------------------------------------
    log.info("=== Step 4: Computing implied correlations ===")
    rho_df = compute_implied_correlation(
        adr_vols   = adr_vols,
        local_vols = local_vols,
        fx_vols    = fx_vols,
        specs      = specs,
        tenors     = tenors,
    )

    log.info("\n%s", correlation_summary(rho_df).to_string())

    # ------------------------------------------------------------------
    # 5. Compute skew
    # ------------------------------------------------------------------
    log.info("=== Step 5: Computing equity skew ===")
    adr_skew   = compute_equity_skew(
        adr_vols, use_rr_approximation=adr_use_rr_skew
    )
    local_skew = compute_equity_skew(
        local_vols, use_rr_approximation=local_use_rr_skew
    )

    # ------------------------------------------------------------------
    # 6. Assemble derived dataset
    # ------------------------------------------------------------------
    log.info("=== Step 6: Assembling derived dataset ===")
    frames = [adr_vols, local_vols, fx_vols_with_wings, fx_spot, rho_df]
    if not adr_skew.empty:
        frames.append(adr_skew)
    if not local_skew.empty:
        frames.append(local_skew)

    derived = pd.concat(frames, axis=1)
    derived.index.name = "date"

    # ------------------------------------------------------------------
    # 7. Save
    # ------------------------------------------------------------------
    derived.to_csv(output_path)
    log.info("Derived dataset saved → %s  (%d rows, %d cols)", output_path, *derived.shape)

    return derived
