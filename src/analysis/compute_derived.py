"""
compute_derived.py
------------------
Load cleaned_dataset.csv, compute implied equity-FX correlations and raw
variance contributions, save derived_dataset.csv, and generate three plots:

  1. rho time series (all three tenors)
  2. histograms of rho values (all three tenors)
  3. rolling 60-day mean of rho (all three tenors)

Formula
-------
    rho = (sigma_adr^2 - sigma_local^2 - sigma_fx^2) / (2 * sigma_local * sigma_fx)

    delta_var = sigma_adr^2 - sigma_local^2 - sigma_fx^2   (raw numerator)

All vol columns in cleaned_dataset.csv are in percentage points
(e.g. 32.14 = 32.14%).  The formula is scale-invariant, so rho is identical
whether inputs are in % points or decimal; only delta_var units differ
(here: %^2 units, consistent throughout).

Column mapping from cleaned_dataset.csv
----------------------------------------
    sigma_adr  : adr_ATM_1M / adr_ATM_3M / adr_ATM_1Y
    sigma_local: loc_ATM_1M / loc_ATM_3M / loc_ATM_1Y
    sigma_fx   : fx_ATM_1M  / fx_ATM_3M  / fx_ATM_1Y

Outputs
-------
    data/processed/derived_dataset.csv   — original 22 cols + 6 new cols
    outputs/figures/rho_timeseries.png
    outputs/figures/rho_histograms.png
    outputs/figures/rho_rolling60.png
"""

from __future__ import annotations

from pathlib import Path

import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import pandas as pd

# Use the canonical rho implementation rather than duplicating the formula.
import sys as _sys
_sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from src.analysis.implied_correlation import compute_implied_correlation, CorrelationSpec

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

print("\n" + "=" * 65)
print("DONE")
print("=" * 65)
