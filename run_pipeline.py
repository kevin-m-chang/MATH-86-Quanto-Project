"""
run_pipeline.py
----------------
Top-level entry point for the implied equity-FX correlation research pipeline.

Usage
-----
Edit the CONFIGURATION section below to match your Bloomberg Excel exports,
then run from the project root:

    python run_pipeline.py

This will:
  1. Load Excel exports from data/raw/
  2. Clean and align by date
  3. Reconstruct FX wing vols
  4. Compute implied correlations and equity skew
  5. Save data/processed/derived_dataset.csv
  6. Generate time-series plots in outputs/figures/
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(__name__)

# Ensure project root is on the path when run directly.
_ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(_ROOT))

from src.analysis.build_dataset import build_and_save_dataset
from src.analysis.implied_correlation import CorrelationSpec
from src.data_ingestion.loader import load_adr_vols, load_local_vols, load_fx_vols
from src.features.skew import compute_equity_skew
from src.visualization.plots import (
    plot_implied_correlation,
    plot_skew,
    plot_rho_and_skew,
)

# ============================================================
#  CONFIGURATION — edit to match your data files and tickers
# ============================================================

RAW_DIR = _ROOT / "data" / "raw"

# Bloomberg Excel export paths
ADR_VOL_PATH   = RAW_DIR / "adr_vols.xlsx"
LOCAL_VOL_PATH = RAW_DIR / "local_vols.xlsx"
FX_VOL_PATH    = RAW_DIR / "fx_vols.xlsx"
FX_SPOT_PATH   = RAW_DIR / "fx_spot.xlsx"

# Define the ADR / local equity / FX pair triplets to analyse.
# Add as many CorrelationSpec entries as needed.
SPECS = [
    CorrelationSpec(
        adr_ticker   = "VALE",    # ADR ticker prefix in adr_vols.xlsx columns
        local_ticker = "VALE3",   # Local equity ticker prefix in local_vols.xlsx
        fx_pair      = "USDBRL",  # FX pair prefix in fx_vols.xlsx
        label        = "Vale ADR / Vale ON / USD-BRL",
    ),
    # CorrelationSpec(
    #     adr_ticker   = "PBR",
    #     local_ticker = "PETR4",
    #     fx_pair      = "USDBRL",
    #     label        = "Petrobras ADR / PETR4 / USD-BRL",
    # ),
]

TENORS = ("1M", "3M", "1Y")

# Set True if only ATM + RR are available (no individual 25P/25C columns).
ADR_USE_RR_SKEW   = False
LOCAL_USE_RR_SKEW = False

# ============================================================
#  MAIN
# ============================================================

def main() -> None:
    log.info("========================================")
    log.info("  Implied Equity-FX Correlation Pipeline")
    log.info("========================================")

    # Validate that input files exist before proceeding.
    for label, path in [
        ("ADR vols",         ADR_VOL_PATH),
        ("Local equity vols", LOCAL_VOL_PATH),
        ("FX vols",          FX_VOL_PATH),
        ("FX spot",          FX_SPOT_PATH),
    ]:
        if not path.exists():
            log.error(
                "Missing input file: %s\n"
                "  Expected at: %s\n"
                "  Export this dataset from Bloomberg and place it in data/raw/.",
                label, path,
            )
            sys.exit(1)

    # ------------------------------------------------------------------
    # Run the full build pipeline (loads, cleans, computes, saves CSV).
    # ------------------------------------------------------------------
    derived = build_and_save_dataset(
        adr_vol_path   = ADR_VOL_PATH,
        local_vol_path = LOCAL_VOL_PATH,
        fx_vol_path    = FX_VOL_PATH,
        fx_spot_path   = FX_SPOT_PATH,
        specs          = SPECS,
        tenors         = TENORS,
        adr_use_rr_skew   = ADR_USE_RR_SKEW,
        local_use_rr_skew = LOCAL_USE_RR_SKEW,
    )

    # ------------------------------------------------------------------
    # Extract sub-frames for plotting.
    # ------------------------------------------------------------------
    rho_cols  = [c for c in derived.columns if "_RHO_"  in c]
    skew_cols = [c for c in derived.columns if "_SKEW_" in c]

    rho_df  = derived[rho_cols]
    skew_df = derived[skew_cols]

    spec_tuples = [
        (s.adr_ticker, s.local_ticker, s.fx_pair) for s in SPECS
    ]

    # ------------------------------------------------------------------
    # Generate plots.
    # ------------------------------------------------------------------
    log.info("=== Generating plots ===")

    if not rho_df.empty:
        plot_implied_correlation(
            rho_df   = rho_df,
            specs    = spec_tuples,
            tenors   = TENORS,
            filename = "implied_correlation.png",
        )

    if not skew_df.empty:
        plot_skew(
            skew_df  = skew_df,
            label    = "Equity Skew (25P − 25C)",
            tenors   = TENORS,
            filename = "equity_skew.png",
        )

    # Per-spec combo chart (ρ + skew in one figure).
    for spec in SPECS:
        if not rho_df.empty and not skew_df.empty:
            for tenor in TENORS:
                plot_rho_and_skew(
                    rho_df       = rho_df,
                    skew_df      = skew_df,
                    adr_ticker   = spec.adr_ticker,
                    local_ticker = spec.local_ticker,
                    fx_pair      = spec.fx_pair,
                    tenor        = tenor,
                )

    log.info("Pipeline complete.")


if __name__ == "__main__":
    main()
