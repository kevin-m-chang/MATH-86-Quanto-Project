"""
ingest_csv_pipeline.py
-----------------------
Data ingestion and cleaning pipeline for the Bloomberg CSV exports.

Steps
-----
1. Load the three CSV files from data/raw/ using the CSV loaders in
   src/data_ingestion/loader.py.  Trailing Bloomberg formula columns are
   stripped; valid columns are prefixed (adr_, loc_, fx_).
2. Strip weekends and forward-fill holiday gaps (≤3 days) using the existing
   cleaner.  The FX series covers calendar days (incl. Jan 1); the equity
   series are business-day only.  An inner join on business days is used so
   only dates where ALL three series have data are kept.
3. Merge on the date index (inner join) and sort by date.
4. Print a full pipeline status report.
5. Save to data/processed/cleaned_dataset.csv.

No derived features (implied correlation, skew) are computed here.

Usage
-----
    python ingest_csv_pipeline.py
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

_ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(_ROOT))

from src.data_ingestion.loader  import load_all_csv
from src.data_ingestion.cleaner import _to_business_day_index

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

RAW_DIR   = _ROOT / "data" / "raw"
OUT_DIR   = _ROOT / "data" / "processed"
OUT_PATH  = OUT_DIR / "cleaned_dataset.csv"

ADR_PATH   = RAW_DIR / "asml_adr_vols.csv"
LOCAL_PATH = RAW_DIR / "asml_loc_vols.csv"
FX_PATH    = RAW_DIR / "eurusd_fx_vols.csv"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _align_and_merge(
    adr: "pd.DataFrame",
    local: "pd.DataFrame",
    fx: "pd.DataFrame",
    ffill_limit: int = 3,
) -> "pd.DataFrame":
    """
    Align three DataFrames to a common business-day index and merge.

    Steps
    -----
    1. Strip weekends from each frame.
    2. Forward-fill up to `ffill_limit` consecutive NaN days (covers public
       holidays where one market is closed but others are open).
    3. Inner-join all three on the date index.
    4. Sort by date.
    5. Drop any remaining rows with NaN in any column.
    """
    import pandas as pd

    frames = {"adr": adr, "local": local, "fx": fx}

    # 1. Strip weekends.
    cleaned = {k: _to_business_day_index(v) for k, v in frames.items()}

    # 2. Build common business-day intersection, then reindex + ffill.
    common_index = cleaned["adr"].index
    for df in cleaned.values():
        common_index = common_index.intersection(df.index)

    if len(common_index) == 0:
        raise ValueError(
            "No overlapping business-day dates across the three datasets. "
            "Check that the CSV files cover overlapping time periods."
        )

    log.info(
        "Inner-join date range: %s → %s  (%d business days)",
        common_index.min().date(),
        common_index.max().date(),
        len(common_index),
    )

    for key in cleaned:
        cleaned[key] = cleaned[key].reindex(common_index).ffill(limit=ffill_limit)

    # 3. Merge (all frames now share common_index, so pd.concat is equivalent
    #    to an inner join at this point).
    merged = pd.concat(list(cleaned.values()), axis=1)

    # 4. Sort by date (already sorted, but make it explicit).
    merged = merged.sort_index()

    # 5. Drop any date still containing NaN in any column.
    n_before = len(merged)
    merged = merged.dropna()
    n_dropped = n_before - len(merged)
    if n_dropped:
        log.warning(
            "Dropped %d date(s) with remaining NaN values after ffill.", n_dropped
        )

    return merged


def _print_status_report(
    raw: dict[str, "pd.DataFrame"],
    merged: "pd.DataFrame",
) -> None:
    """Print a structured pipeline status report to stdout."""
    sep = "=" * 62

    print(f"\n{sep}")
    print("  PIPELINE STATUS REPORT")
    print(sep)

    # — Raw row counts —
    print("\n[1] Raw rows loaded from each CSV")
    print(f"    {'File':<30} {'Rows':>6}  {'Cols':>5}")
    print(f"    {'-'*30}  {'-'*6}  {'-'*5}")
    labels = {"adr": "asml_adr_vols.csv", "local": "asml_loc_vols.csv", "fx": "eurusd_fx_vols.csv"}
    for key, label in labels.items():
        df = raw[key]
        print(f"    {label:<30} {len(df):>6}  {df.shape[1]:>5}")

    # — Merge info —
    print(f"\n[2] After inner-join merge")
    print(f"    Rows : {len(merged)}")
    print(f"    Cols : {merged.shape[1]}")
    print(f"    Date : {merged.index.min().date()}  →  {merged.index.max().date()}")

    # — Column names —
    print(f"\n[3] Final column names")
    for i, col in enumerate(merged.columns, 1):
        print(f"    {i:>2}. {col}")

    # — dtypes —
    print(f"\n[4] Column dtypes")
    print(f"    {'Column':<25} {'dtype'}")
    print(f"    {'-'*25}  {'-'*10}")
    for col, dtype in merged.dtypes.items():
        print(f"    {str(col):<25} {dtype}")

    # — First 5 rows —
    print(f"\n[5] First 5 rows")
    print(merged.head().to_string())

    print(f"\n{sep}\n")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    import pandas as pd   # local import so the module is importable without pandas

    log.info("=== Step 1: Loading CSV files ===")
    for label, path in [("ADR", ADR_PATH), ("Local", LOCAL_PATH), ("FX", FX_PATH)]:
        if not path.exists():
            log.error("Missing file: %s", path)
            sys.exit(1)

    raw = load_all_csv(
        adr_path   = ADR_PATH,
        local_path = LOCAL_PATH,
        fx_path    = FX_PATH,
    )

    log.info(
        "  adr   : %d rows, columns=%s", len(raw["adr"]),   raw["adr"].columns.tolist()
    )
    log.info(
        "  local : %d rows, columns=%s", len(raw["local"]), raw["local"].columns.tolist()
    )
    log.info(
        "  fx    : %d rows, columns=%s", len(raw["fx"]),    raw["fx"].columns.tolist()
    )

    log.info("=== Step 2 & 3: Aligning and merging ===")
    merged = _align_and_merge(raw["adr"], raw["local"], raw["fx"])

    log.info("=== Step 4: Status report ===")
    _print_status_report(raw, merged)

    log.info("=== Step 5: Saving cleaned dataset ===")
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    merged.to_csv(OUT_PATH)
    log.info("Saved → %s  (%d rows × %d cols)", OUT_PATH, *merged.shape)


if __name__ == "__main__":
    main()
