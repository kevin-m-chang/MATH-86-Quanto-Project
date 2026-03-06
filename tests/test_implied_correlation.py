"""
tests/test_implied_correlation.py
-----------------------------------
Unit tests for src/analysis/implied_correlation.py

Tests are deliberately Bloomberg-free — all inputs are synthetic DataFrames.
Run with:
    pytest tests/test_implied_correlation.py -v
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.analysis.implied_correlation import (
    CorrelationSpec,
    compute_implied_correlation,
    correlation_summary,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def date_index() -> pd.DatetimeIndex:
    return pd.bdate_range("2020-01-01", periods=252)


@pytest.fixture
def synthetic_vols(date_index: pd.DatetimeIndex):
    """
    Synthesise consistent vol series where ρ ≡ 0.5 by construction.

    Given σ_local = 0.20, σ_fx = 0.15, ρ = 0.5:
        σ_adr² = σ_local² + σ_fx² + 2·ρ·σ_local·σ_fx
               = 0.04 + 0.0225 + 2·0.5·0.20·0.15
               = 0.04 + 0.0225 + 0.03 = 0.0925
        σ_adr  = sqrt(0.0925) ≈ 0.30414
    """
    n = len(date_index)
    sigma_local = 0.20
    sigma_fx    = 0.15
    rho_true    = 0.50
    sigma_adr   = np.sqrt(
        sigma_local**2 + sigma_fx**2 + 2 * rho_true * sigma_local * sigma_fx
    )

    adr_vols   = pd.DataFrame({"VALE_1M":  sigma_adr},   index=date_index)
    local_vols = pd.DataFrame({"VALE3_1M": sigma_local}, index=date_index)
    fx_vols    = pd.DataFrame({"USDBRL_ATM_1M": sigma_fx}, index=date_index)

    return adr_vols, local_vols, fx_vols, rho_true


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestComputeImpliedCorrelation:

    def test_exact_rho_recovery(self, synthetic_vols):
        """ρ computed from consistent vols should equal the true ρ exactly."""
        adr_vols, local_vols, fx_vols, rho_true = synthetic_vols

        specs = [CorrelationSpec("VALE", "VALE3", "USDBRL")]
        result = compute_implied_correlation(
            adr_vols, local_vols, fx_vols, specs, tenors=("1M",)
        )

        col = "VALE_VALE3_USDBRL_RHO_1M"
        assert col in result.columns, f"Expected column '{col}' not found."
        computed = result[col].dropna()
        assert len(computed) > 0, "No non-NaN ρ values computed."
        np.testing.assert_allclose(
            computed.values, rho_true,
            atol=1e-9,
            err_msg="Computed ρ does not match true ρ.",
        )

    def test_output_clipped_to_minus_one_one(self, date_index):
        """ρ must always be in [-1, 1] even with extreme inputs."""
        # Construct vols that would naively give |ρ| > 1.
        adr_vols   = pd.DataFrame({"X_1M": 0.99}, index=date_index)
        local_vols = pd.DataFrame({"Y_1M": 0.01}, index=date_index)
        fx_vols    = pd.DataFrame({"PAIR_ATM_1M": 0.01}, index=date_index)

        specs  = [CorrelationSpec("X", "Y", "PAIR")]
        result = compute_implied_correlation(
            adr_vols, local_vols, fx_vols, specs, tenors=("1M",)
        )
        col = "X_Y_PAIR_RHO_1M"
        assert result[col].between(-1.0, 1.0).all(), "ρ out of [-1, 1] range."

    def test_missing_columns_skipped_gracefully(self, date_index):
        """Specs referencing non-existent columns should be skipped, not raise."""
        adr_vols   = pd.DataFrame({"WRONG_1M": 0.25}, index=date_index)
        local_vols = pd.DataFrame({"VALE3_1M": 0.20}, index=date_index)
        fx_vols    = pd.DataFrame({"USDBRL_ATM_1M": 0.15}, index=date_index)

        specs = [CorrelationSpec("VALE", "VALE3", "USDBRL")]

        # Should raise because no series can be computed at all.
        with pytest.raises(ValueError, match="No correlation series"):
            compute_implied_correlation(
                adr_vols, local_vols, fx_vols, specs, tenors=("1M",)
            )

    def test_nan_inputs_propagate(self, date_index):
        """NaN in any input vol on a given date → NaN in ρ on that date."""
        sigma_local = 0.20
        sigma_fx    = 0.15
        sigma_adr   = np.sqrt(0.20**2 + 0.15**2 + 2 * 0.5 * 0.20 * 0.15)

        adr_vals   = np.full(len(date_index), sigma_adr)
        local_vals = np.full(len(date_index), sigma_local)
        fx_vals    = np.full(len(date_index), sigma_fx)

        # Introduce NaN at position 5.
        adr_vals[5] = np.nan

        adr_vols   = pd.DataFrame({"VALE_1M":  adr_vals},   index=date_index)
        local_vols = pd.DataFrame({"VALE3_1M": local_vals}, index=date_index)
        fx_vols    = pd.DataFrame({"USDBRL_ATM_1M": fx_vals}, index=date_index)

        specs  = [CorrelationSpec("VALE", "VALE3", "USDBRL")]
        result = compute_implied_correlation(
            adr_vols, local_vols, fx_vols, specs, tenors=("1M",)
        )
        col = "VALE_VALE3_USDBRL_RHO_1M"
        assert np.isnan(result[col].iloc[5]), "Expected NaN to propagate at index 5."
        assert result[col].iloc[6:].notna().all(), "Unexpected NaN after index 5."

    def test_multiple_tenors(self, date_index):
        """All requested tenors should produce output columns."""
        # Build consistent vols for 1M, 3M, 1Y with different ρ values.
        rows = {}
        expected = {"1M": 0.30, "3M": 0.50, "1Y": 0.70}
        for tenor, rho in expected.items():
            sl, sfx = 0.20, 0.15
            sa = np.sqrt(sl**2 + sfx**2 + 2 * rho * sl * sfx)
            rows[f"VALE_{tenor}"]        = sa
            rows[f"VALE3_{tenor}"]       = sl
            rows[f"USDBRL_ATM_{tenor}"]  = sfx

        n = len(date_index)
        adr_vols   = pd.DataFrame({k: np.full(n, v) for k, v in rows.items()
                                    if k.startswith("VALE_")}, index=date_index)
        local_vols = pd.DataFrame({k: np.full(n, v) for k, v in rows.items()
                                    if k.startswith("VALE3_")}, index=date_index)
        fx_vols    = pd.DataFrame({k: np.full(n, v) for k, v in rows.items()
                                    if k.startswith("USDBRL_")}, index=date_index)

        specs  = [CorrelationSpec("VALE", "VALE3", "USDBRL")]
        result = compute_implied_correlation(
            adr_vols, local_vols, fx_vols, specs, tenors=("1M", "3M", "1Y")
        )

        for tenor, rho_true in expected.items():
            col = f"VALE_VALE3_USDBRL_RHO_{tenor}"
            assert col in result.columns, f"Missing column {col}"
            np.testing.assert_allclose(
                result[col].dropna().values, rho_true, atol=1e-9
            )

    def test_correlation_summary_shape(self, synthetic_vols):
        """correlation_summary should return one row per ρ column."""
        adr_vols, local_vols, fx_vols, _ = synthetic_vols
        specs  = [CorrelationSpec("VALE", "VALE3", "USDBRL")]
        result = compute_implied_correlation(
            adr_vols, local_vols, fx_vols, specs, tenors=("1M",)
        )
        summary = correlation_summary(result)
        assert len(summary) == len(result.columns)
        assert "mean" in summary.columns


class TestCorrelationSpec:

    def test_auto_label(self):
        spec = CorrelationSpec("VALE", "VALE3", "USDBRL")
        assert spec.label == "VALE/VALE3/USDBRL"

    def test_explicit_label(self):
        spec = CorrelationSpec("VALE", "VALE3", "USDBRL", label="My Label")
        assert spec.label == "My Label"
