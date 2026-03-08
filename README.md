# MATH 86 Quanto Project

A reproducible research pipeline for extracting implied equity–FX correlation from option markets and studying its role in quanto hedge stability, using ADR, local equity, and FX volatility surfaces from Bloomberg.

**Research question:** Does correlation skew in vanilla option markets identify periods where ATM-based quanto hedges become more fragile?

**Case study:** ASML US ADR (ASML US Equity) vs ASML Amsterdam shares (ASML NA Equity) with EUR/USD.  
**Data:** Bloomberg CSV exports, 2016–2025 (~2500 trading days), committed to this repository.

---

## Table of Contents

1. [Theory](#theory)
2. [Empirical Findings](#empirical-findings)
3. [Repository Structure](#repository-structure)
4. [Datasets](#datasets)
5. [Quick Start](#quick-start)
6. [Source Module Guide](#source-module-guide)
7. [Caveats and Known Behavior](#caveats-and-known-behavior)
8. [Bloomberg Notes](#bloomberg-notes)

---

## Theory

### Implied Equity–FX Correlation

Under log-normal dynamics, an ADR (priced in USD) is a composite of the local
equity (priced in EUR) and the EUR/USD exchange rate. Their implied volatilities
satisfy the **ADR variance identity**:

$$\sigma_{\text{ADR}}^2 = \sigma_{\text{local}}^2 + \sigma_{\text{FX}}^2 + 2\,\rho\,\sigma_{\text{local}}\,\sigma_{\text{FX}}$$

Rearranging gives the **implied correlation** extracted directly from option markets:

$$\rho = \frac{\sigma_{\text{ADR}}^2 - \sigma_{\text{local}}^2 - \sigma_{\text{FX}}^2}{2\,\sigma_{\text{local}}\,\sigma_{\text{FX}}}$$

Computed at three tenors: **1M**, **3M**, and **1Y**, and at three strike levels: **ATM**, **25Δ-put**, and **25Δ-call**.

### Correlation Skew

Applying the identity at put-strike and call-strike implied vols yields strike-specific implied correlations $\hat\rho_\text{put}$ and $\hat\rho_\text{call}$. Their difference is the **correlation skew**:

$$\text{corr\_skew} = \hat\rho_\text{put} - \hat\rho_\text{call}$$

This measures *state-dependent* stock–FX dependence: how differently the market prices co-movement in downside vs upside scenarios. When skew is large, the single ATM correlation is a poor summary of the full dependence structure across strikes.

A useful way to see the fragility implication: the ATM rho can be interpreted as a probability-weighted average of the down-state and up-state correlations,

$$\hat\rho_\text{ATM} \approx p \cdot \hat\rho_\text{down} + (1-p) \cdot \hat\rho_\text{up}$$

so that day-to-day drift in ATM rho is amplified by the put/call gap:

$$\Delta\hat\rho_\text{ATM} \approx (\hat\rho_\text{down} - \hat\rho_\text{up})\,\Delta p$$

Large correlation skew therefore makes the ATM hedge more sensitive to regime-probability shifts, even when the underlying volatilities are stable.

### Hedge Fragility

A standard vanilla quanto hedge fixes $\hat\rho$ at the ATM level. The relevant covariance term is:

$$C_t = 2\,\hat\rho_t\,\sigma_\text{loc}\,\sigma_\text{FX}$$

We measure forward-looking hedge instability using:

$$|\Delta\hat\rho_h| = |\hat\rho_{t+h} - \hat\rho_t|, \qquad |\Delta C_h| = |C_{t+h} - C_t|$$

at horizons $h \in \{1\text{d},\, 5\text{d},\, 20\text{d}\}$.

### FX Wing Vol Reconstruction

Bloomberg delivers FX implied vols as ATM straddle + 25-delta butterfly spread +
25-delta risk reversal. The individual wing vols follow directly:

$$\sigma_{25c} = \text{ATM} + \text{BF} + \frac{\text{RR}}{2}$$

$$\sigma_{25p} = \text{ATM} + \text{BF} - \frac{\text{RR}}{2}$$

### Equity Skew

$$\text{skew} = \sigma_{25p} - \sigma_{25c}$$

Positive skew means the put wing is wider than the call wing, as is typical for equities.

---

## Empirical Findings

The full analysis is in `notebooks/analysis_corr_skew.ipynb`.

### Main result: bat-shaped fragility

Binning observations by signed `corr_skew_1m` into ten equal-count deciles (D01 = most negative, D10 = most positive) reveals a **U-shaped ("bat-shaped") profile** in future hedge fragility:

- Both tail deciles (D01 and D10) show **30–53% larger future |ΔC|** than the neutral middle (D05) at the 20-day horizon.
- The positive skew tail (D10) is slightly more destabilising than the negative tail (D01) at longer horizons (D10: 1.53× D05 at 20d vs D01: 1.31×).
- Linear OLS regressions of skew on signed future rho drift show β ≈ 0 — the U-shape cancels symmetrically and is invisible to linear models.

### Magnitude confirms the signal

Binning by absolute |corr_skew| into magnitude deciles (M01–M10) yields a near-monotone increase in fragility: the highest-skew decile (M10) shows **1.2–1.5× more |ΔC|** than the lowest (M01) across all horizons.

### Transition-state test

An unexpected fragility spike in D06 (middle-positive decile) was investigated:

- D06 has the **highest sign-switch rate (59%)** — nearly every other day the skew crosses zero — while having the lowest within-decile skew standard deviation. This is the "zero-crossing zone" where correlation is actively being repriced.
- Days where skew changes sign ("sign-switch days") show **~15% more |ΔC|** than non-switch days across all horizons (~41% of the sample).

### Key figures

| Figure | Description |
|--------|-------------|
| ATM / put / call ρ time series | Shows the spread between strike-specific implied correlations over 2016–2025 |
| Correlation skew time series | `corr_skew_1m` with 60-day rolling mean |
| Bat-shape bar chart | Mean |ΔC| per signed decile, normalised to D05 = 1.00 |
| Magnitude decile chart | Mean |ΔC| per |skew| magnitude decile, normalised to M01 = 1.00 |
| Transition-state chart | Skew velocity, within-decile std, sign-switch vs non-switch fragility |

### Practical interpretation

> Correlation skew appears to be a **regime indicator** for periods when ATM-based vanilla quanto hedges are less reliable. Extreme skew regimes are associated with larger future covariance drift, suggesting practitioners should widen rehedging bands or increase monitoring frequency when |corr_skew| is elevated.
>
> This is not a directional return predictor and does not imply tradable alpha. The relationship reflects the sensitivity of ATM-implied covariance to regime shifts when put/call rhos diverge.

---

## Repository Structure

```
MATH-86-Quanto-Project/
│
├── run_pipeline.py                        ← Single entry point: python run_pipeline.py
├── requirements.txt
├── README.md
├── .gitignore
│
├── data/
│   ├── raw/                               ← Bloomberg CSV exports (committed to git)
│   │   ├── asml_adr_vols.csv              — ASML US ADR implied vols + spot
│   │   ├── asml_loc_vols.csv              — ASML Amsterdam local implied vols + spot
│   │   ├── eurusd_fx_vols.csv             — EUR/USD ATM, RR, BF vols + spot
│   │   └── README.md                      — Column schemas and Bloomberg field map
│   └── processed/                         ← Generated datasets (committed to git)
│       ├── cleaned_dataset.csv            — Inner-joined, business-day aligned, 2491 × 22, zero NaNs
│       └── derived_dataset.csv            — cleaned + rho_1m/3m/1y + delta_var, 2491 × 28
│
├── outputs/
│   └── figures/                           ← Generated plots (committed to git)
│       ├── rho_timeseries.png             — All three rho series, 2016–2025
│       ├── rho_histograms.png             — Per-tenor distribution with mean and median
│       └── rho_rolling60.png              — 60-day rolling mean, all tenors
│
├── src/
│   ├── data_ingestion/
│   │   ├── loader.py                      ← Read Bloomberg CSVs into prefixed DataFrames
│   │   ├── cleaner.py                     ← Align DataFrames to a shared business-day index
│   │   └── ingest_csv_pipeline.py         ← Step 1: raw CSVs → cleaned_dataset.csv
│   ├── analysis/
│   │   ├── implied_correlation.py         ← Core rho formula + CorrelationSpec dataclass
│   │   ├── compute_derived.py             ← Step 2: cleaned → derived_dataset.csv + plots
│   │   └── timing_stability_check.py      ← Sanity check: Amsterdam/NYSE close-time sensitivity
│   ├── features/
│   │   ├── fx_vol_surface.py              ← Reconstruct 25-delta wing vols from ATM + BF + RR
│   │   └── skew.py                        ← Compute equity skew = sigma_25p − sigma_25c
│   └── visualization/
│       └── plots.py                       ← Reusable matplotlib plot functions
│
├── scripts/                               ← Bloomberg data re-pull scripts (terminal required)
│   ├── bdh_generic.py                     — Reusable BDH helper function
│   └── bdh_pull_fx_spot.py                — One-shot EUR/USD spot pull
│
└── tests/                                 ← 30 unit tests, all Bloomberg-free
    ├── test_implied_correlation.py        — 8 tests for the rho formula
    ├── test_fx_vol_surface.py             — 7 tests for wing vol reconstruction
    ├── test_skew.py                       — 8 tests for equity skew
    ├── test_cleaner.py                    — 7 tests for date alignment
    └── test_bbg_connection.py             — Live Bloomberg session smoke-test (terminal only)
```

---

## Datasets

### `data/raw/` — Bloomberg CSV exports

All vol columns are in **percentage points** (e.g. `32.14` means 32.14%).
Spot is a price level (e.g. EUR/USD = `1.08`).
Bloomberg appends trailing formula columns to CSV exports; `loader.py` strips
these automatically on load using explicit column whitelists.

| File | Raw rows | Columns after loading |
|---|---|---|
| `asml_adr_vols.csv` | 2514 | `adr_ATM_1M`, `adr_P25_1M`, `adr_C25_1M`, `adr_ATM_3M`, `adr_ATM_1Y`, `adr_ADR_SPOT` |
| `asml_loc_vols.csv` | 2560 | `loc_ATM_1M`, `loc_P25_1M`, `loc_C25_1M`, `loc_ATM_3M`, `loc_ATM_1Y`, `loc_LOC_SPOT` |
| `eurusd_fx_vols.csv` | 2610 | `fx_ATM_1M/3M/1Y`, `fx_RR_1M/3M/1Y`, `fx_BF_1M/3M/1Y`, `fx_FX_SPOT` |

Column naming follows `<source>_<bloomberg_field>`. The spot columns retain their
Bloomberg field names after prefixing: `adr_ADR_SPOT` (ASML US price in USD),
`loc_LOC_SPOT` (ASML Amsterdam price in EUR), `fx_FX_SPOT` (EUR/USD rate).

See `data/raw/README.md` for the full column schemas and Bloomberg field mappings.

### `data/processed/cleaned_dataset.csv`

Produced by **Step 1** (`ingest_csv_pipeline.py`).

- Inner join of all three CSVs on shared business days
- Forward-fill across market holidays (at most 3 consecutive days)
- Remaining NaN rows dropped; zero NaNs in output
- **2491 rows × 22 columns**, 2016-01-04 → 2025-12-31
- All values `float64`, all columns prefixed (`adr_`, `loc_`, `fx_`)

### `data/processed/derived_dataset.csv`

Produced by **Step 2** (`compute_derived.py`). Contains everything in
`cleaned_dataset.csv` plus derived columns including:

| Column group | Description |
|---|---|
| `rho_1m/3m/1y` | ATM implied equity–FX correlation, three tenors |
| `rho_put_1m`, `rho_call_1m` | 25Δ-put and 25Δ-call implied correlation (1M) |
| `corr_skew_1m` | Correlation skew = `rho_put_1m − rho_call_1m` |
| `cross_term_1m` | Covariance term $2\hat\rho\,\sigma_\text{loc}\,\sigma_\text{FX}$ (pp²) |
| `rho_drift_{1d/5d/20d}` | Forward signed change in ATM rho at each horizon |
| `cross_term_drift_{1d/5d/20d}` | Forward signed change in cross-term at each horizon |
| `adr_skew_1m`, `loc_skew_1m` | Equity skew for ADR and local shares |
| `fx_RR_1M` | FX 25Δ risk reversal (1M) |
| `delta_var_1m/3m/1y` | Raw variance-identity numerator (diagnostic) |

**2491 rows × 43 columns**, 2016-01-04 → 2025-12-31.

**ATM rho summary (1M):**

| | Mean | Std | Median | 5th pct | 95th pct |
|---|---:|---:|---:|---:|---:|
| `rho_1m` | 0.104 | 0.314 | 0.105 | −0.414 | 0.632 |

---

## Quick Start

### 1. Clone and set up

```bash
git clone https://github.com/kevin-m-chang/MATH-86-Quanto-Project.git
cd MATH-86-Quanto-Project
python -m venv .venv
source .venv/bin/activate        # macOS / Linux
# .venv\Scripts\activate       # Windows
pip install -r requirements.txt
```

### 2. Run the full pipeline

The raw CSVs and processed datasets are already committed, so outputs are
immediately available. To regenerate everything from scratch:

```bash
python run_pipeline.py
```

This runs two steps in sequence:

| Step | Script | Input | Output |
|---|---|---|---|
| 1 | `src/data_ingestion/ingest_csv_pipeline.py` | `data/raw/*.csv` | `data/processed/cleaned_dataset.csv` |
| 2 | `src/analysis/compute_derived.py` | `cleaned_dataset.csv` | `derived_dataset.csv` + `outputs/figures/*.png` |

Each step is idempotent and can also be run standalone:

```bash
python src/data_ingestion/ingest_csv_pipeline.py
python src/analysis/compute_derived.py
```

### 3. Run the unit tests

```bash
pytest tests/ -v --ignore=tests/test_bbg_connection.py
```

All 30 tests are Bloomberg-free and run entirely on synthetic data.
`test_bbg_connection.py` requires a live Bloomberg terminal and is excluded
from the standard run.

---

## Source Module Guide

### `src/data_ingestion/loader.py`
Reads Bloomberg CSV exports. Maintains explicit column whitelists for each source
(stripping Bloomberg's trailing formula/metadata columns), parses dates into a
`DatetimeIndex`, coerces all data to `float64`, and applies source prefixes
(`adr_`, `loc_`, `fx_`). Also contains Excel loaders preserved for reference.

### `src/data_ingestion/cleaner.py`
Aligns a set of DataFrames to a shared business-day index: inner join across all
sources → forward-fill holiday gaps (at most 3 consecutive days) → drop any
remaining NaN rows.

### `src/data_ingestion/ingest_csv_pipeline.py`
**Step 1 script.** Orchestrates `loader.py` and `cleaner.py` for the three
ASML US / ASML NA / EURUSD CSVs, prints a structured status report (row counts,
column names, dtypes, first 5 rows), and saves `cleaned_dataset.csv`.

### `src/analysis/implied_correlation.py`
Core **library module** — imported by `compute_derived.py` and the unit tests.
Contains:
- `CorrelationSpec` dataclass — binds together an ADR ticker, local ticker, and FX pair
- `compute_implied_correlation()` — general rho computation with configurable column templates
- `correlation_summary()` — descriptive statistics for any rho DataFrame

### `src/analysis/compute_derived.py`
**Step 2 script.** Calls `compute_implied_correlation()` from the library above,
computes `delta_var` (raw numerator), saves `derived_dataset.csv`, prints summary
statistics, and generates all three plots.

### `src/analysis/timing_stability_check.py`
Standalone sanity check. Simulates a 1-day lag on the Amsterdam or FX close and
measures the resulting shift in rho. Key finding: FX timing is benign (p95
deviation ≤ 0.04 rho-points); the Amsterdam/NYSE 4.5-hour closing-time gap
introduces moderate noise at the 1M tenor (p95 deviation ≈ 0.57 rho-points).
Supports the recommendation to use 3M and 1Y as primary series.

### `src/features/fx_vol_surface.py`
Given ATM + 25-delta butterfly + 25-delta risk reversal, reconstructs the
25-delta call and put vols: `sigma_25c = ATM + BF + RR/2`,
`sigma_25p = ATM + BF - RR/2`. Auto-detects pair prefixes from column names.

### `src/features/skew.py`
Computes equity skew = `sigma_25p − sigma_25c` per ticker/tenor. Supports both
direct wing vol inputs and a `−RR25` approximation when individual wings are
unavailable.

### `src/visualization/plots.py`
Reusable matplotlib functions: `plot_implied_correlation`, `plot_skew`,
`plot_rho_and_skew`. Saves PNGs to `outputs/figures/`.

---

## Caveats and Known Behavior

### Rho clipping
The variance identity is only an approximation in markets with skew and
convexity. Raw rho estimates can therefore exceed [−1, 1]. The implementation
clips rho to [−1, 1] for stability; the raw numerator (`delta_var`) is also
stored in `derived_dataset.csv` for diagnostic use.

### 1M rho is noisier than 3M and 1Y
Short-tenor implied vols respond faster to intraday moves, amplifying day-to-day
variation in the rho estimate. Additionally, ASML Amsterdam shares close at
approximately 11:30 ET while the NYSE closes at 16:00 ET — a 4.5-hour gap that
introduces asynchronous pricing into daily vol observations. The sensitivity
analysis in `timing_stability_check.py` shows a p95 rho deviation of ≈ 0.57
rho-points under a simulated 1-day lag on the Amsterdam close.

**Use 3M and 1Y rho as the primary series. Treat 1M rho as indicative.**

### Common setup issues

| Symptom | Cause | Fix |
|---|---|---|
| `ModuleNotFoundError` | Dependency not installed | `pip install -r requirements.txt` |
| `FileNotFoundError: cleaned_dataset.csv` | Step 1 has not been run | `python src/data_ingestion/ingest_csv_pipeline.py` |
| Bloomberg `ImportError` | `blpapi` not installed | Only needed for `scripts/` — not required for the analysis pipeline |

---

## Bloomberg Notes

The raw CSV files in `data/raw/` were exported from a Bloomberg terminal and are
committed directly to this repository. **No Bloomberg access is required to run
the analysis pipeline.**

If you need to extend the date range or re-pull the data, use the scripts in
`scripts/`. These require a live Bloomberg terminal session:

```bash
# On a Bloomberg terminal (Windows):
C:\path\to\python.exe scripts\bdh_pull_fx_spot.py
```

After re-exporting CSVs, re-run `python run_pipeline.py` to regenerate
`cleaned_dataset.csv`, `derived_dataset.csv`, and all plots.
