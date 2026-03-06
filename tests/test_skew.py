"""
tests/test_skew.py
-------------------
Unit tests for src/features/skew.py

Run with:
    pytest tests/test_skew.py -v
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.features.skew import compute_equity_skew


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def wing_vol_df() -> pd.DataFrame:
    idx = pd.bdate_range("2023-01-02", periods=10)
    return pd.DataFrame(
        {
            "VALE_25P_1M":  np.linspace(0.30, 0.35, 10),
            "VALE_25C_1M":  np.linspace(0.25, 0.28, 10),
            "VALE_25P_3M":  np.linspace(0.32, 0.37, 10),
            "VALE_25C_3M":  np.linspace(0.27, 0.30, 10),
        },
        index=idx,
    )


@pytest.fixture
def rr_vol_df() -> pd.DataFrame:
    idx = pd.bdate_range("2023-01-02", periods=10)
    return pd.DataFrame(
        {
            "VALE_RR25_1M": np.linspace(-0.05, -0.07, 10),
            "VALE_RR25_3M": np.linspace(-0.04, -0.06, 10),
        },
        index=idx,
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestComputeEquitySkewWingVols:

    def test_skew_columns_created(self, wing_vol_df):
        result = compute_equity_skew(wing_vol_df, tenors=("1M", "3M"))
        assert "VALE_SKEW_1M" in result.columns
        assert "VALE_SKEW_3M" in result.columns

    def test_skew_formula(self, wing_vol_df):
        """skew = 25P - 25C"""
        result = compute_equity_skew(wing_vol_df, tenors=("1M",))
        expected = wing_vol_df["VALE_25P_1M"] - wing_vol_df["VALE_25C_1M"]
        pd.testing.assert_series_equal(
            result["VALE_SKEW_1M"].reset_index(drop=True),
            expected.reset_index(drop=True),
            check_names=False,
        )

    def test_skew_is_positive_when_put_vol_gt_call_vol(self, wing_vol_df):
        """With put vol > call vol, skew should be positive (standard equity skew)."""
        result = compute_equity_skew(wing_vol_df, tenors=("1M", "3M"))
        assert (result["VALE_SKEW_1M"] > 0).all()

    def test_auto_ticker_detection(self, wing_vol_df):
        result = compute_equity_skew(wing_vol_df, tickers=None, tenors=("1M",))
        assert "VALE_SKEW_1M" in result.columns

    def test_missing_put_column_skipped(self, wing_vol_df):
        df = wing_vol_df.drop(columns=["VALE_25P_1M"])
        result = compute_equity_skew(df, tenors=("1M", "3M"))
        # 1M should be absent, 3M should still be computed.
        assert "VALE_SKEW_1M" not in result.columns
        assert "VALE_SKEW_3M" in result.columns

    def test_empty_result_when_no_matching_columns(self):
        idx = pd.bdate_range("2023-01-02", periods=5)
        df  = pd.DataFrame({"JUNK_ATM_1M": np.ones(5)}, index=idx)
        result = compute_equity_skew(df, tenors=("1M",))
        assert result.empty


class TestComputeEquitySkewRRApproximation:

    def test_rr_approximation_skew_equals_neg_rr(self, rr_vol_df):
        """With use_rr_approximation=True, skew = -RR25."""
        result = compute_equity_skew(
            rr_vol_df, use_rr_approximation=True, tenors=("1M",)
        )
        expected = -rr_vol_df["VALE_RR25_1M"]
        pd.testing.assert_series_equal(
            result["VALE_SKEW_1M"].reset_index(drop=True),
            expected.reset_index(drop=True),
            check_names=False,
        )

    def test_rr_approx_produces_positive_skew_for_negative_rr(self, rr_vol_df):
        """Negative RR (put > call) → positive skew after negation."""
        result = compute_equity_skew(
            rr_vol_df, use_rr_approximation=True, tenors=("1M",)
        )
        assert (result["VALE_SKEW_1M"] > 0).all()
