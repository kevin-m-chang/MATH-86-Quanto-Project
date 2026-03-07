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
from src.features.skew import compute_skew_from_columns


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


class TestComputeSkewFromColumns:
    """Tests for the explicit-column variant."""

    @pytest.fixture
    def df(self) -> pd.DataFrame:
        idx = pd.bdate_range("2023-01-02", periods=5)
        return pd.DataFrame(
            {
                "adr_P25_1M": [35.0, 36.0, 37.0, 38.0, 39.0],
                "adr_C25_1M": [30.0, 31.0, 32.0, 33.0, 34.0],
                "loc_P25_1M": [38.0, 37.0, 36.0, 35.0, 34.0],
                "loc_C25_1M": [33.0, 32.0, 31.0, 30.0, 29.0],
            },
            index=idx,
        )

    def test_output_columns(self, df):
        result = compute_skew_from_columns(df, [
            ("adr_P25_1M", "adr_C25_1M", "adr_skew"),
            ("loc_P25_1M", "loc_C25_1M", "loc_skew"),
        ])
        assert "adr_skew" in result.columns
        assert "loc_skew" in result.columns

    def test_formula(self, df):
        result = compute_skew_from_columns(df, [
            ("adr_P25_1M", "adr_C25_1M", "adr_skew"),
        ])
        expected = df["adr_P25_1M"] - df["adr_C25_1M"]
        pd.testing.assert_series_equal(
            result["adr_skew"].reset_index(drop=True),
            expected.reset_index(drop=True),
            check_names=False,
        )

    def test_positive_when_put_gt_call(self, df):
        result = compute_skew_from_columns(df, [
            ("adr_P25_1M", "adr_C25_1M", "s"),
        ])
        assert (result["s"] > 0).all()

    def test_missing_column_skipped(self, df):
        result = compute_skew_from_columns(df, [
            ("adr_P25_1M", "adr_C25_1M", "adr_skew"),
            ("MISSING_P", "MISSING_C", "bad_skew"),
        ])
        assert "adr_skew" in result.columns
        assert "bad_skew" not in result.columns

    def test_empty_result(self):
        idx = pd.bdate_range("2023-01-02", periods=3)
        df = pd.DataFrame({"x": [1, 2, 3]}, index=idx)
        result = compute_skew_from_columns(df, [
            ("MISSING_P", "MISSING_C", "skew"),
        ])
        assert result.empty