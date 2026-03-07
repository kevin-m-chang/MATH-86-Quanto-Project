"""
tests/test_fx_vol_surface.py
------------------------------
Unit tests for src/features/fx_vol_surface.py

Run with:
    pytest tests/test_fx_vol_surface.py -v
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.features.fx_vol_surface import reconstruct_wing_vols
from src.features.fx_vol_surface import reconstruct_wing_vols_from_columns


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def fx_vol_df() -> pd.DataFrame:
    """Simple 5-row FX vol DataFrame with known ATM, BF25, RR25 values."""
    idx = pd.bdate_range("2023-01-02", periods=5)
    return pd.DataFrame(
        {
            "USDBRL_ATM_1M":  [0.20, 0.21, 0.19, 0.22, 0.18],
            "USDBRL_BF25_1M": [0.01, 0.01, 0.01, 0.01, 0.01],
            "USDBRL_RR25_1M": [0.02, 0.02, 0.02, 0.02, 0.02],
            "USDBRL_ATM_3M":  [0.22, 0.23, 0.21, 0.24, 0.20],
            "USDBRL_BF25_3M": [0.015, 0.015, 0.015, 0.015, 0.015],
            "USDBRL_RR25_3M": [0.025, 0.025, 0.025, 0.025, 0.025],
        },
        index=idx,
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestReconstructWingVols:

    def test_columns_added(self, fx_vol_df):
        result = reconstruct_wing_vols(fx_vol_df, pairs=["USDBRL"], tenors=("1M", "3M"))
        for tenor in ("1M", "3M"):
            assert f"USDBRL_25C_{tenor}" in result.columns
            assert f"USDBRL_25P_{tenor}" in result.columns

    def test_call_formula(self, fx_vol_df):
        """σ_25c = ATM + BF + RR/2"""
        result = reconstruct_wing_vols(fx_vol_df, pairs=["USDBRL"], tenors=("1M",))
        expected = fx_vol_df["USDBRL_ATM_1M"] + fx_vol_df["USDBRL_BF25_1M"] + fx_vol_df["USDBRL_RR25_1M"] / 2
        pd.testing.assert_series_equal(
            result["USDBRL_25C_1M"].reset_index(drop=True),
            expected.reset_index(drop=True),
            check_names=False,
        )

    def test_put_formula(self, fx_vol_df):
        """σ_25p = ATM + BF - RR/2"""
        result = reconstruct_wing_vols(fx_vol_df, pairs=["USDBRL"], tenors=("1M",))
        expected = fx_vol_df["USDBRL_ATM_1M"] + fx_vol_df["USDBRL_BF25_1M"] - fx_vol_df["USDBRL_RR25_1M"] / 2
        pd.testing.assert_series_equal(
            result["USDBRL_25P_1M"].reset_index(drop=True),
            expected.reset_index(drop=True),
            check_names=False,
        )

    def test_call_gt_put_when_rr_positive(self, fx_vol_df):
        """With positive RR25, 25C vol should always exceed 25P vol."""
        result = reconstruct_wing_vols(fx_vol_df, pairs=["USDBRL"], tenors=("1M", "3M"))
        for tenor in ("1M", "3M"):
            assert (result[f"USDBRL_25C_{tenor}"] > result[f"USDBRL_25P_{tenor}"]).all()

    def test_auto_pair_detection(self, fx_vol_df):
        """pairs=None should auto-detect USDBRL from column names."""
        result = reconstruct_wing_vols(fx_vol_df, pairs=None, tenors=("1M", "3M"))
        assert "USDBRL_25C_1M" in result.columns

    def test_missing_bf_column_skipped(self, fx_vol_df):
        """A tenor with a missing BF column should be skipped without error."""
        df = fx_vol_df.drop(columns=["USDBRL_BF25_1M"])
        result = reconstruct_wing_vols(df, pairs=["USDBRL"], tenors=("1M", "3M"))
        # 1M should be absent, 3M should be present.
        assert "USDBRL_25C_1M" not in result.columns
        assert "USDBRL_25C_3M" in result.columns

    def test_original_columns_preserved(self, fx_vol_df):
        """Reconstruction should NOT remove the original ATM/BF/RR columns."""
        result = reconstruct_wing_vols(fx_vol_df)
        for col in fx_vol_df.columns:
            assert col in result.columns


class TestReconstructWingVolsFromColumns:
    """Tests for the explicit-column variant."""

    @pytest.fixture
    def simple_df(self) -> pd.DataFrame:
        idx = pd.bdate_range("2023-01-02", periods=5)
        return pd.DataFrame(
            {
                "my_atm": [10.0, 11.0, 12.0, 13.0, 14.0],
                "my_bf":  [0.5,  0.5,  0.5,  0.5,  0.5],
                "my_rr":  [1.0,  1.0,  1.0,  1.0,  1.0],
            },
            index=idx,
        )

    def test_call_formula(self, simple_df):
        result = reconstruct_wing_vols_from_columns(
            simple_df, "my_atm", "my_bf", "my_rr",
            call_col="c25", put_col="p25",
        )
        expected = simple_df["my_atm"] + simple_df["my_bf"] + simple_df["my_rr"] / 2
        pd.testing.assert_series_equal(result["c25"], expected, check_names=False)

    def test_put_formula(self, simple_df):
        result = reconstruct_wing_vols_from_columns(
            simple_df, "my_atm", "my_bf", "my_rr",
            call_col="c25", put_col="p25",
        )
        expected = simple_df["my_atm"] + simple_df["my_bf"] - simple_df["my_rr"] / 2
        pd.testing.assert_series_equal(result["p25"], expected, check_names=False)

    def test_original_columns_preserved(self, simple_df):
        result = reconstruct_wing_vols_from_columns(
            simple_df, "my_atm", "my_bf", "my_rr",
        )
        for col in simple_df.columns:
            assert col in result.columns

    def test_call_gt_put_when_rr_positive(self, simple_df):
        result = reconstruct_wing_vols_from_columns(
            simple_df, "my_atm", "my_bf", "my_rr",
            call_col="c", put_col="p",
        )
        assert (result["c"] > result["p"]).all()
