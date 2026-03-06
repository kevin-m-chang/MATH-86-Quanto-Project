"""
tests/test_cleaner.py
----------------------
Unit tests for src/data_ingestion/cleaner.py

Run with:
    pytest tests/test_cleaner.py -v
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.data_ingestion.cleaner import align_datasets


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _make_df(cols: list[str], start: str, periods: int, seed: int = 0) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    idx = pd.bdate_range(start, periods=periods)
    return pd.DataFrame(
        rng.uniform(0.10, 0.40, (periods, len(cols))),
        index=idx,
        columns=cols,
    )


@pytest.fixture
def overlapping_frames():
    """Four DataFrames that share 40 business-day dates in common."""
    adr   = _make_df(["VALE_1M", "VALE_3M"],           "2022-01-03", 60, seed=1)
    local = _make_df(["VALE3_1M", "VALE3_3M"],          "2022-01-03", 50, seed=2)
    fx    = _make_df(["USDBRL_ATM_1M", "USDBRL_ATM_3M"], "2022-02-01", 80, seed=3)
    spot  = _make_df(["USDBRL_SPOT"],                   "2022-01-03", 80, seed=4)
    return adr, local, fx, spot


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestAlignDatasets:

    def test_output_has_common_dates(self, overlapping_frames):
        adr, local, fx, spot = overlapping_frames
        merged, *_ = align_datasets(
            adr, local, fx, spot, drop_incomplete=False
        )
        # Merged index must be a subset of each individual frame's index.
        assert merged.index.isin(adr.index).all()
        assert merged.index.isin(local.index).all()
        assert merged.index.isin(fx.index).all()
        assert merged.index.isin(spot.index).all()

    def test_all_five_outputs_returned(self, overlapping_frames):
        result = align_datasets(*overlapping_frames, drop_incomplete=False)
        assert len(result) == 5, "Expected (merged, adr, local, fx, spot) tuple."

    def test_individual_frames_same_index(self, overlapping_frames):
        merged, adr_c, local_c, fx_c, spot_c = align_datasets(
            *overlapping_frames, drop_incomplete=False
        )
        for frame in (adr_c, local_c, fx_c, spot_c):
            assert frame.index.equals(merged.index)

    def test_merged_contains_all_columns(self, overlapping_frames):
        adr, local, fx, spot = overlapping_frames
        merged, *_ = align_datasets(adr, local, fx, spot, drop_incomplete=False)
        for col in list(adr.columns) + list(local.columns) + list(fx.columns) + list(spot.columns):
            assert col in merged.columns, f"Column '{col}' missing from merged."

    def test_no_common_dates_raises(self):
        """Frames with disjoint date ranges should raise ValueError."""
        adr   = _make_df(["VALE_1M"],           "2020-01-01", 10)
        local = _make_df(["VALE3_1M"],           "2020-01-01", 10)
        fx    = _make_df(["USDBRL_ATM_1M"],      "2021-06-01", 10)   # no overlap
        spot  = _make_df(["USDBRL_SPOT"],        "2020-01-01", 10)

        with pytest.raises(ValueError, match="No overlapping dates"):
            align_datasets(adr, local, fx, spot)

    def test_weekend_dates_removed(self):
        """Any Saturday/Sunday dates in input should be stripped."""
        # Build a DataFrame that includes weekends.
        idx    = pd.date_range("2023-01-02", periods=10)   # includes weekends
        make   = lambda cols: pd.DataFrame(np.ones((10, len(cols))), index=idx, columns=cols)

        adr   = make(["VALE_1M"])
        local = make(["VALE3_1M"])
        fx    = make(["USDBRL_ATM_1M"])
        spot  = make(["USDBRL_SPOT"])

        merged, *_ = align_datasets(adr, local, fx, spot, drop_incomplete=False)
        assert (merged.index.dayofweek < 5).all(), "Weekend dates found in merged index."

    def test_ffill_fills_gaps(self):
        """A single-row gap should be forward-filled (within ffill_limit)."""
        idx  = pd.bdate_range("2023-01-02", periods=10)
        vals = np.ones(10) * 0.25
        vals[3] = np.nan   # introduce a gap

        make_df = lambda col: pd.DataFrame({col: vals.copy()}, index=idx)

        adr   = make_df("VALE_1M")
        local = make_df("VALE3_1M")
        fx    = make_df("USDBRL_ATM_1M")
        spot  = make_df("USDBRL_SPOT")

        _, adr_c, *_ = align_datasets(
            adr, local, fx, spot, ffill_limit=3, drop_incomplete=False
        )
        assert not adr_c["VALE_1M"].isna().any(), "ffill should have filled the NaN gap."
