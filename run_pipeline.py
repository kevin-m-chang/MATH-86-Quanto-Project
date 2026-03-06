"""
run_pipeline.py
----------------
Top-level entry point for the MATH-86 implied equity-FX correlation pipeline.

Run order
---------
    Step 1 — Ingest CSVs → data/processed/cleaned_dataset.csv
    Step 2 — Compute implied correlations → data/processed/derived_dataset.csv
                                          → outputs/figures/rho_*.png

Usage (from the project root)
------------------------------
    python run_pipeline.py

Both steps are idempotent: re-running overwrites the CSV and PNG outputs with
fresh results.  Individual steps can also be run standalone:

    python src/data_ingestion/ingest_csv_pipeline.py
    python src/analysis/compute_derived.py
"""

from __future__ import annotations

import logging
import runpy
import sys
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(__name__)

_ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(_ROOT))


def main() -> None:
    log.info("=" * 60)
    log.info("  MATH-86 Implied Equity-FX Correlation Pipeline")
    log.info("=" * 60)

    # ------------------------------------------------------------------
    # Step 1 — Ingest raw Bloomberg CSVs, align, clean, save.
    # ------------------------------------------------------------------
    log.info("")
    log.info("--- STEP 1: CSV Ingestion & Cleaning ---")
    runpy.run_path(
        str(_ROOT / "src" / "data_ingestion" / "ingest_csv_pipeline.py"),
        run_name="__main__",
    )

    # ------------------------------------------------------------------
    # Step 2 — Compute implied correlations (rho) and generate plots.
    # ------------------------------------------------------------------
    log.info("")
    log.info("--- STEP 2: Compute Derived Dataset & Plots ---")
    runpy.run_path(
        str(_ROOT / "src" / "analysis" / "compute_derived.py"),
        run_name="__main__",
    )

    log.info("")
    log.info("=" * 60)
    log.info("  Pipeline complete.")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
