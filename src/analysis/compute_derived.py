"""
compute_derived.py
------------------
Load cleaned_dataset.csv, compute implied equity-FX correlations and raw
variance contributions, save derived_dataset.csv, and generate plots.

Layer 1 — ATM rho (existing)
-----------------------------
  rho_1m / rho_3m / rho_1y  — ATM implied correlation at 1M, 3M, 1Y
  delta_var_1m / 3m / 1y    — raw numerator (σ_adr² − σ_loc² − σ_fx²)

Layer 2 — Wing rho, correlation skew, drift, cross-term
--------------------------------------------------------
  fx_P25_1M, fx_C25_1M      — FX 25-delta put/call vols (from ATM+BF+RR)
  rho_put_1m, rho_call_1m   — implied corr using 25-delta wing vols
  corr_skew_1m               — rho_put_1m − rho_call_1m
  rho_drift_1d / 5d / 20d   — future change in ATM rho_1m
  cross_term_1m              — 2 * rho * σ_loc * σ_fx
  cross_term_drift_1d / 5d / 20d

Formula
-------
    rho = (sigma_adr^2 - sigma_local^2 - sigma_fx^2) / (2 * sigma_local * sigma_fx)

All vol columns are in percentage points (e.g. 32.14 = 32.14%).
The formula is scale-invariant so rho is identical in % or decimal.

Outputs
-------
    data/processed/derived_dataset.csv
    outputs/figures/rho_timeseries.png
    outputs/figures/rho_histograms.png
    outputs/figures/rho_rolling60.png
    outputs/figures/rho_atm_vs_wings.png
    outputs/figures/corr_skew_timeseries.png
    outputs/figures/corr_skew_vs_drift_1d.png
    outputs/figures/corr_skew_vs_drift_5d.png
    outputs/figures/corr_skew_vs_drift_20d.png
"""

from __future__ import annotations

from pathlib import Path

import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import numpy as np
import pandas as pd

# Use the canonical rho implementation rather than duplicating the formula.
import sys as _sys
_sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from src.analysis.implied_correlation import compute_implied_correlation, CorrelationSpec
from src.features.fx_vol_surface import reconstruct_wing_vols_from_columns
from src.features.skew import compute_skew_from_columns

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

ROOT          = Path(__file__).resolve().parent.parent.parent   # project root
INPUT_PATH    = ROOT / "data" / "processed" / "cleaned_dataset.csv"
OUTPUT_PATH   = ROOT / "data" / "processed" / "derived_dataset.csv"
FIGURES_DIR   = ROOT / "outputs" / "figures"

FIGURES_DIR.mkdir(parents=True, exist_ok=True)

# ---------------------------------------------------------------------------
# Plot styling (consistent with existing plots.py)
# ---------------------------------------------------------------------------

TENOR_COLORS = {"1M": "#1f77b4", "3M": "#ff7f0e", "1Y": "#2ca02c"}
TENOR_LABELS = {"1M": "1-Month", "3M": "3-Month", "1Y": "1-Year"}

plt.rcParams.update({
    "font.size":       11,
    "axes.titlesize":  12,
    "axes.labelsize":  11,
    "legend.fontsize": 10,
    "xtick.labelsize": 10,
    "ytick.labelsize": 10,
    "figure.dpi":      120,
    "axes.grid":       True,
    "grid.alpha":      0.4,
    "grid.linestyle":  "--",
})

TENORS = ["1M", "3M", "1Y"]


# ---------------------------------------------------------------------------
# Load & compute
# ---------------------------------------------------------------------------

print("=" * 65)
print("COMPUTE DERIVED DATASET")
print("=" * 65)

df = pd.read_csv(INPUT_PATH, index_col=0, parse_dates=True)
df.index.name = "date"

print(f"\nLoaded:  {INPUT_PATH.name}  |  {df.shape[0]} rows × {df.shape[1]} cols")
print(f"Date range: {df.index.min().date()} → {df.index.max().date()}")

# ---------------------------------------------------------------------------
# Compute rho via the canonical library function in implied_correlation.py.
# The merged DataFrame holds all three sources in a single frame, so we pass
# it as all three inputs; the column templates pick the right prefix columns.
# ---------------------------------------------------------------------------
SPEC = CorrelationSpec(
    adr_ticker   = "adr_ATM",   # → column template "adr_ATM_{tenor}"
    local_ticker = "loc_ATM",   # → column template "loc_ATM_{tenor}"
    fx_pair      = "fx_ATM",    # → column template "fx_ATM_{tenor}"
    label        = "ASML ADR / ASML Local / EUR-USD",
)

rho_df = compute_implied_correlation(
    adr_vols        = df,
    local_vols      = df,
    fx_vols         = df,
    specs           = [SPEC],
    tenors          = tuple(TENORS),
    adr_col_template   = "{ticker}_{tenor}",   # → adr_ATM_1M / 3M / 1Y
    local_col_template = "{ticker}_{tenor}",   # → loc_ATM_1M / 3M / 1Y
    fx_col_template    = "{pair}_{tenor}",     # → fx_ATM_1M  / 3M / 1Y
)

# Rename to the short rho_1m / rho_3m / rho_1y convention.
RENAME = {
    "adr_ATM_loc_ATM_fx_ATM_RHO_1M": "rho_1m",
    "adr_ATM_loc_ATM_fx_ATM_RHO_3M": "rho_3m",
    "adr_ATM_loc_ATM_fx_ATM_RHO_1Y": "rho_1y",
}
rho_df = rho_df.rename(columns=RENAME)

# delta_var = sigma_adr^2 - sigma_loc^2 - sigma_fx^2  (raw numerator, %^2 units)
TENOR_COLS = {
    "1M": ("adr_ATM_1M", "loc_ATM_1M", "fx_ATM_1M"),
    "3M": ("adr_ATM_3M", "loc_ATM_3M", "fx_ATM_3M"),
    "1Y": ("adr_ATM_1Y", "loc_ATM_1Y", "fx_ATM_1Y"),
}
dvar_cols: dict[str, pd.Series] = {}
for tenor, (adr_col, loc_col, fx_col) in TENOR_COLS.items():
    s_adr = df[adr_col].astype(float)
    s_loc = df[loc_col].astype(float)
    s_fx  = df[fx_col].astype(float)
    dvar_cols[f"delta_var_{tenor.lower()}"] = s_adr**2 - s_loc**2 - s_fx**2

for col in rho_df.columns:
    print(f"\nComputed {col}  |  {rho_df[col].notna().sum()} valid obs")

derived = pd.concat(
    [df, rho_df, pd.DataFrame(dvar_cols, index=df.index)],
    axis=1,
)
derived.to_csv(OUTPUT_PATH)
print(f"\nSaved → {OUTPUT_PATH}  ({derived.shape[0]} rows × {derived.shape[1]} cols)")


# ---------------------------------------------------------------------------
# Summary statistics
# ---------------------------------------------------------------------------

print("\n" + "=" * 65)
print("SUMMARY STATISTICS — Implied Correlation (rho)")
print("=" * 65)

rho_cols = [f"rho_{t.lower()}" for t in TENORS]
stats = derived[rho_cols].describe(percentiles=[0.05, 0.25, 0.5, 0.75, 0.95])

STAT_LABELS = {
    "count": "n",
    "mean":  "mean",
    "std":   "std",
    "min":   "min",
    "5%":    "5th pct",
    "25%":   "25th pct",
    "50%":   "median",
    "75%":   "75th pct",
    "95%":   "95th pct",
    "max":   "max",
}

# Pretty-print with tenor labels as headers
print(f"\n{'Statistic':<15}", end="")
for t in TENORS:
    print(f"  {TENOR_LABELS[t]:>16}", end="")
print()
print("-" * (15 + 3 * 18))

for raw_stat in stats.index:
    label = STAT_LABELS.get(raw_stat, raw_stat)
    print(f"{label:<15}", end="")
    for t in TENORS:
        col = f"rho_{t.lower()}"
        val = float(stats.loc[raw_stat, col])
        if raw_stat == "count":
            print(f"  {int(val):>16}", end="")
        else:
            print(f"  {val:>16.4f}", end="")
    print()


# ---------------------------------------------------------------------------
# Plot 1 — rho time series
# ---------------------------------------------------------------------------

fig1, ax1 = plt.subplots(figsize=(13, 4.5))

for tenor in TENORS:
    col = f"rho_{tenor.lower()}"
    ax1.plot(
        derived.index,
        derived[col],
        color=TENOR_COLORS[tenor],
        linewidth=1.1,
        alpha=0.85,
        label=TENOR_LABELS[tenor],
    )

ax1.axhline(0, color="black", linewidth=0.8, linestyle="--", alpha=0.55)
ax1.set_ylim(-1.05, 1.05)
ax1.set_ylabel("Implied ρ")
ax1.set_xlabel("Date")
ax1.set_title("Implied Equity–FX Correlation: ASML ADR / ASML Local / EUR-USD")
ax1.legend(loc="upper right", framealpha=0.8)
ax1.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))
ax1.xaxis.set_major_locator(mdates.MonthLocator(interval=12))
plt.setp(ax1.xaxis.get_majorticklabels(), rotation=30, ha="right")
fig1.tight_layout()
fig1.savefig(FIGURES_DIR / "rho_timeseries.png", bbox_inches="tight")
print(f"\nPlot 1 saved → {FIGURES_DIR / 'rho_timeseries.png'}")
plt.close(fig1)


# ---------------------------------------------------------------------------
# Plot 2 — rho histograms
# ---------------------------------------------------------------------------

fig2, axes2 = plt.subplots(1, 3, figsize=(14, 4.2), sharey=False)

for ax, tenor in zip(axes2, TENORS):
    col  = f"rho_{tenor.lower()}"
    data = derived[col].dropna()
    ax.hist(
        data,
        bins=60,
        color=TENOR_COLORS[tenor],
        edgecolor="white",
        linewidth=0.4,
        alpha=0.85,
    )
    ax.axvline(data.mean(), color="black",  linewidth=1.4, linestyle="--",
               label=f"Mean={data.mean():.3f}")
    ax.axvline(data.median(), color="grey", linewidth=1.2, linestyle=":",
               label=f"Median={data.median():.3f}")
    ax.set_xlabel("Implied ρ")
    ax.set_ylabel("Frequency" if tenor == "1M" else "")
    ax.set_title(f"{TENOR_LABELS[tenor]}")
    ax.legend(fontsize=9, framealpha=0.8)

fig2.suptitle(
    "Distribution of Implied Equity–FX Correlation (ASML / EUR-USD)",
    fontsize=12, y=1.01,
)
fig2.tight_layout()
fig2.savefig(FIGURES_DIR / "rho_histograms.png", bbox_inches="tight")
print(f"Plot 2 saved → {FIGURES_DIR / 'rho_histograms.png'}")
plt.close(fig2)


# ---------------------------------------------------------------------------
# Plot 3 — rolling 60-day mean
# ---------------------------------------------------------------------------

WINDOW = 60

fig3, ax3 = plt.subplots(figsize=(13, 4.5))

for tenor in TENORS:
    col     = f"rho_{tenor.lower()}"
    rolling = derived[col].rolling(WINDOW, min_periods=WINDOW // 2).mean()
    ax3.plot(
        rolling.index,
        rolling,
        color=TENOR_COLORS[tenor],
        linewidth=1.5,
        label=TENOR_LABELS[tenor],
    )

ax3.axhline(0, color="black", linewidth=0.8, linestyle="--", alpha=0.55)
ax3.set_ylim(-1.05, 1.05)
ax3.set_ylabel("Rolling 60-day Mean Implied ρ")
ax3.set_xlabel("Date")
ax3.set_title(
    f"Rolling {WINDOW}-Day Mean — Implied Equity–FX Correlation: ASML / EUR-USD"
)
ax3.legend(loc="upper right", framealpha=0.8)
ax3.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))
ax3.xaxis.set_major_locator(mdates.MonthLocator(interval=12))
plt.setp(ax3.xaxis.get_majorticklabels(), rotation=30, ha="right")
fig3.tight_layout()
fig3.savefig(FIGURES_DIR / "rho_rolling60.png", bbox_inches="tight")
print(f"Plot 3 saved → {FIGURES_DIR / 'rho_rolling60.png'}")
plt.close(fig3)


# ===================================================================
# LAYER 2 — Wing rho, correlation skew, drift, cross-term
# ===================================================================

print("\n" + "=" * 65)
print("LAYER 2 — Wing Rho / Correlation Skew / Drift / Cross-Term")
print("=" * 65)

# ------------------------------------------------------------------
# 2a. Reconstruct FX 25-delta put and call vols from ATM + BF + RR
# ------------------------------------------------------------------
derived = reconstruct_wing_vols_from_columns(
    derived,
    atm_col="fx_ATM_1M", bf_col="fx_BF_1M", rr_col="fx_RR_1M",
    call_col="fx_C25_1M", put_col="fx_P25_1M",
)
print(f"\nReconstructed FX wing vols: fx_C25_1M, fx_P25_1M")

# ------------------------------------------------------------------
# 2b. Compute put- and call-implied rho at 1M
# ------------------------------------------------------------------
# Put rho: use 25-delta put vols for all three legs.
SPEC_PUT = CorrelationSpec(
    adr_ticker   = "adr_P25",
    local_ticker = "loc_P25",
    fx_pair      = "fx_P25",
    label        = "ASML put-wing rho",
)

rho_put_df = compute_implied_correlation(
    adr_vols=derived, local_vols=derived, fx_vols=derived,
    specs=[SPEC_PUT], tenors=("1M",),
    adr_col_template="{ticker}_{tenor}",
    local_col_template="{ticker}_{tenor}",
    fx_col_template="{pair}_{tenor}",
)
rho_put_df = rho_put_df.rename(columns={
    "adr_P25_loc_P25_fx_P25_RHO_1M": "rho_put_1m",
})

# Call rho: use 25-delta call vols for all three legs.
SPEC_CALL = CorrelationSpec(
    adr_ticker   = "adr_C25",
    local_ticker = "loc_C25",
    fx_pair      = "fx_C25",
    label        = "ASML call-wing rho",
)

rho_call_df = compute_implied_correlation(
    adr_vols=derived, local_vols=derived, fx_vols=derived,
    specs=[SPEC_CALL], tenors=("1M",),
    adr_col_template="{ticker}_{tenor}",
    local_col_template="{ticker}_{tenor}",
    fx_col_template="{pair}_{tenor}",
)
rho_call_df = rho_call_df.rename(columns={
    "adr_C25_loc_C25_fx_C25_RHO_1M": "rho_call_1m",
})

derived = pd.concat([derived, rho_put_df, rho_call_df], axis=1)

print(f"rho_put_1m:  {derived['rho_put_1m'].notna().sum()} obs, "
      f"mean={derived['rho_put_1m'].mean():.4f}")
print(f"rho_call_1m: {derived['rho_call_1m'].notna().sum()} obs, "
      f"mean={derived['rho_call_1m'].mean():.4f}")

# ------------------------------------------------------------------
# 2c. Correlation skew
# ------------------------------------------------------------------
derived["corr_skew_1m"] = derived["rho_put_1m"] - derived["rho_call_1m"]
print(f"corr_skew_1m: mean={derived['corr_skew_1m'].mean():.4f}, "
      f"std={derived['corr_skew_1m'].std():.4f}")

# ------------------------------------------------------------------
# 2d. Future ATM rho drift
# ------------------------------------------------------------------
for horizon, label in [(1, "1d"), (5, "5d"), (20, "20d")]:
    col = f"rho_drift_{label}"
    derived[col] = derived["rho_1m"].shift(-horizon) - derived["rho_1m"]
    valid = derived[col].notna().sum()
    print(f"{col}: {valid} obs, mean={derived[col].mean():.4f}")

# ------------------------------------------------------------------
# 2e. Cross-term and cross-term drift
# ------------------------------------------------------------------
derived["cross_term_1m"] = (
    2.0 * derived["rho_1m"] * derived["loc_ATM_1M"] * derived["fx_ATM_1M"]
)
print(f"cross_term_1m: mean={derived['cross_term_1m'].mean():.4f}")

for horizon, label in [(1, "1d"), (5, "5d"), (20, "20d")]:
    col = f"cross_term_drift_{label}"
    derived[col] = (
        derived["cross_term_1m"].shift(-horizon) - derived["cross_term_1m"]
    )
    print(f"{col}: {derived[col].notna().sum()} obs")

# ------------------------------------------------------------------
# 2f. Equity skew (using explicit columns)
# ------------------------------------------------------------------
eq_skew = compute_skew_from_columns(derived, [
    ("adr_P25_1M", "adr_C25_1M", "adr_skew_1m"),
    ("loc_P25_1M", "loc_C25_1M", "loc_skew_1m"),
])
derived = pd.concat([derived, eq_skew], axis=1)
for c in eq_skew.columns:
    print(f"{c}: mean={derived[c].mean():.4f}")

# ------------------------------------------------------------------
# Save updated derived dataset
# ------------------------------------------------------------------
derived.to_csv(OUTPUT_PATH)
print(f"\nSaved → {OUTPUT_PATH}  ({derived.shape[0]} rows × {derived.shape[1]} cols)")

# ------------------------------------------------------------------
# Layer 2 summary statistics
# ------------------------------------------------------------------
print("\n" + "=" * 65)
print("SUMMARY — Layer 2 Columns")
print("=" * 65)

layer2_cols = [
    "rho_put_1m", "rho_call_1m", "corr_skew_1m",
    "rho_drift_1d", "rho_drift_5d", "rho_drift_20d",
    "cross_term_1m",
    "cross_term_drift_1d", "cross_term_drift_5d", "cross_term_drift_20d",
    "adr_skew_1m", "loc_skew_1m",
]
l2_stats = derived[layer2_cols].describe(percentiles=[0.05, 0.5, 0.95])
print(l2_stats.round(4).to_string())


# ===================================================================
# LAYER 2 PLOTS
# ===================================================================

# ------------------------------------------------------------------
# Plot 4 — ATM rho vs put rho vs call rho
# ------------------------------------------------------------------
fig4, ax4 = plt.subplots(figsize=(13, 4.5))
ax4.plot(derived.index, derived["rho_1m"],      color="#1f77b4", lw=1.1, alpha=0.8, label="ρ ATM 1M")
ax4.plot(derived.index, derived["rho_put_1m"],  color="#d62728", lw=1.0, alpha=0.7, label="ρ put-wing 1M")
ax4.plot(derived.index, derived["rho_call_1m"], color="#2ca02c", lw=1.0, alpha=0.7, label="ρ call-wing 1M")
ax4.axhline(0, color="black", lw=0.8, ls="--", alpha=0.5)
ax4.set_ylim(-1.05, 1.05)
ax4.set_ylabel("Implied ρ")
ax4.set_xlabel("Date")
ax4.set_title("ATM vs 25Δ Put vs 25Δ Call Implied Correlation (1M)")
ax4.legend(loc="upper right", framealpha=0.8)
ax4.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))
ax4.xaxis.set_major_locator(mdates.MonthLocator(interval=12))
plt.setp(ax4.xaxis.get_majorticklabels(), rotation=30, ha="right")
fig4.tight_layout()
fig4.savefig(FIGURES_DIR / "rho_atm_vs_wings.png", bbox_inches="tight")
print(f"\nPlot 4 saved → {FIGURES_DIR / 'rho_atm_vs_wings.png'}")
plt.close(fig4)

# ------------------------------------------------------------------
# Plot 5 — Correlation skew time series
# ------------------------------------------------------------------
fig5, ax5 = plt.subplots(figsize=(13, 4.0))
ax5.plot(derived.index, derived["corr_skew_1m"], color="#9467bd", lw=1.1, alpha=0.85)
ax5.axhline(0, color="black", lw=0.8, ls="--", alpha=0.5)
rolling_cs = derived["corr_skew_1m"].rolling(60, min_periods=30).mean()
ax5.plot(derived.index, rolling_cs, color="#e377c2", lw=1.8, label="60-day rolling mean")
ax5.set_ylabel("Correlation Skew (ρ_put − ρ_call)")
ax5.set_xlabel("Date")
ax5.set_title("Correlation Skew: 1M Put-Wing vs Call-Wing Implied ρ (ASML / EUR-USD)")
ax5.legend(loc="upper right", framealpha=0.8)
ax5.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))
ax5.xaxis.set_major_locator(mdates.MonthLocator(interval=12))
plt.setp(ax5.xaxis.get_majorticklabels(), rotation=30, ha="right")
fig5.tight_layout()
fig5.savefig(FIGURES_DIR / "corr_skew_timeseries.png", bbox_inches="tight")
print(f"Plot 5 saved → {FIGURES_DIR / 'corr_skew_timeseries.png'}")
plt.close(fig5)

# ------------------------------------------------------------------
# Plots 6-8 — Scatter: corr_skew vs rho drift at 1d / 5d / 20d
# ------------------------------------------------------------------
for horizon_label, color in [("1d", "#1f77b4"), ("5d", "#ff7f0e"), ("20d", "#2ca02c")]:
    drift_col = f"rho_drift_{horizon_label}"
    mask = derived["corr_skew_1m"].notna() & derived[drift_col].notna()
    x = derived.loc[mask, "corr_skew_1m"]
    y = derived.loc[mask, drift_col]

    fig_s, ax_s = plt.subplots(figsize=(6.5, 5.5))
    ax_s.scatter(x, y, s=6, alpha=0.35, color=color, edgecolors="none")

    # OLS fit line
    if len(x) > 10:
        coeffs = np.polyfit(x, y, 1)
        x_fit = np.linspace(x.min(), x.max(), 200)
        ax_s.plot(x_fit, np.polyval(coeffs, x_fit), color="black", lw=1.6, ls="--",
                  label=f"OLS: β={coeffs[0]:.3f}")
        corr = x.corr(y)
        ax_s.set_title(
            f"Correlation Skew vs {horizon_label} ATM ρ Drift  "
            f"(r = {corr:.3f}, β = {coeffs[0]:.3f})",
            fontsize=11,
        )
        ax_s.legend(loc="upper right", framealpha=0.8)
    else:
        ax_s.set_title(f"Correlation Skew vs {horizon_label} ATM ρ Drift")

    ax_s.axhline(0, color="grey", lw=0.6, ls="--", alpha=0.5)
    ax_s.axvline(0, color="grey", lw=0.6, ls="--", alpha=0.5)
    ax_s.set_xlabel("Correlation Skew (ρ_put − ρ_call)")
    ax_s.set_ylabel(f"Δρ ATM 1M ({horizon_label} forward)")
    fig_s.tight_layout()
    fname = f"corr_skew_vs_drift_{horizon_label}.png"
    fig_s.savefig(FIGURES_DIR / fname, bbox_inches="tight")
    print(f"Plot saved → {FIGURES_DIR / fname}")
    plt.close(fig_s)

print("\n" + "=" * 65)
print("DONE")
print("=" * 65)
