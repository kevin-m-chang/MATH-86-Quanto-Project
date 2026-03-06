# MATH 86 Quanto Project

Research pipeline for computing **implied equity-FX correlation** from ADR, local equity,
and FX implied volatilities — a core input to quanto derivative pricing.

**Subject**: ASML Holding N.V. — US ADR (`ASML US Equity`) vs Amsterdam local shares
(`ASML NA Equity`) with EUR/USD as the FX leg.

**Data**: Bloomberg CSV exports, 2016-01-04 → 2025-12-31, 2491 business days.

---

## Theory

### Implied Equity-FX Correlation

Under log-normal dynamics, the ADR (priced in USD) is a composite of the local equity
(priced in EUR) and the EUR/USD exchange rate. Their implied vols satisfy:

$$\sigma_{\text{ADR}}^2 = \sigma_{\text{local}}^2 + \sigma_{\text{FX}}^2
  + 2\,\rho\,\sigma_{\text{local}}\,\sigma_{\text{FX}}$$

Rearranging gives the **implied correlation** extracted directly from option markets:

$$\rho = \frac{\sigma_{\text{ADR}}^2 - \sigma_{\text{local}}^2 - \sigma_{\text{FX}}^2}
             {2\,\sigma_{\text{local}}\,\sigma_{\text{FX}}}$$

Computed at three tenors: **1M, 3M, 1Y**.

### FX Wing Vol Reconstruction

Bloomberg provides FX vols as ATM straddle + 25Δ butterfly + 25Δ risk reversal:

$$\sigma_{25c} = \text{ATM} + \text{BF} + \tfrac{\text{RR}}{2}, \qquad
  \sigma_{25p} = \text{ATM} + \text{BF} - \tfrac{\text{RR}}{2}$$

### Equity Skew

$$\text{skew} = \sigma_{25p} - \sigma_{25c}$$

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
│   ├── raw/                               ← Bloomberg CSV exports (tracked in git)
│   │   ├── asml_adr_vols.csv              — ASML US ADR implied vols
│   │   ├── asml_loc_vols.csv              — ASML Amsterdam local implied vols
│   │   ├── eurusd_fx_vols.csv             — EUR/USD ATM + RR + BF vols + spot
│   │   └── README.md                      — Column schemas and Bloomberg field map
│   └── processed/                         ← Generated datasets (tracked in git)
│       ├── cleaned_dataset.csv            — Inner-joined, business-day aligned, 2491×22
│       └── derived_dataset.csv            — cleaned + rho_1m/3m/1y + delta_var, 2491×28
│
├── outputs/
│   └── figures/                           ← Generated plots (tracked in git)
│       ├── rho_timeseries.png             — All three rho series, 2016–2025
│       ├── rho_histograms.png             — Per-tenor distribution with mean/median
│       └── rho_rolling60.png              — 60-day rolling mean, all tenors
│
├── src/
│   ├── data_ingestion/
│   │   ├── loader.py                      ← Load Bloomberg CSVs into prefixed DataFrames
│   │   ├── cleaner.py                     ← Align DataFrames to shared business-day index
│   │   └── ingest_csv_pipeline.py         ← Step 1: raw CSVs → cleaned_dataset.csv
│   ├── analysis/
│   │   ├── implied_correlation.py         ← Core rho formula + CorrelationSpec dataclass
│   │   ├── compute_derived.py             ← Step 2: cleaned → derived_dataset.csv + plots
│   │   └── timing_stability_check.py      ← Sanity check: Amsterdam/NYSE close-time sensitivity
│   ├── features/
│   │   ├── fx_vol_surface.py              ← Reconstruct 25Δ call/put from ATM + BF + RR
│   │   └── skew.py                        ← Compute equity skew = σ_25p − σ_25c
│   └── visualization/
│       └── plots.py                       ← Reusable matplotlib plot functions
│
├── scripts/                               ← Bloomberg data re-pull scripts (terminal required)
│   ├── bdh_generic.py                     — Reusable BDH helper
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

All vol columns are in **percentage points** (e.g. `32.14` = 32.14%). Spot is in price
level (e.g. EUR/USD = `1.08`).

| File | Rows | Columns after load |
|---|---|---|
| `asml_adr_vols.csv` | 2514 | `adr_ATM_1M`, `adr_P25_1M`, `adr_C25_1M`, `adr_ATM_3M`, `adr_ATM_1Y`, `adr_ADR_SPOT` |
| `asml_loc_vols.csv` | 2560 | `loc_ATM_1M`, `loc_P25_1M`, `loc_C25_1M`, `loc_ATM_3M`, `loc_ATM_1Y`, `loc_LOC_SPOT` |
| `eurusd_fx_vols.csv` | 2610 | `fx_ATM_1M/3M/1Y`, `fx_RR_1M/3M/1Y`, `fx_BF_1M/3M/1Y`, `fx_FX_SPOT` |

See `data/raw/README.md` for full column schemas and Bloomberg field mappings.

### `data/processed/cleaned_dataset.csv`

Inner join of all three sources on shared business days, forward-filled across
holidays (≤3 days), zero NaNs.

- **2491 rows × 22 columns**, date range 2016-01-04 → 2025-12-31
- All float64, all columns prefixed (`adr_`, `loc_`, `fx_`)

### `data/processed/derived_dataset.csv`

Everything in `cleaned_dataset.csv` plus six derived columns:

| Column | Description |
|---|---|
| `rho_1m` | Implied equity-FX correlation at 1-month tenor |
| `rho_3m` | Implied equity-FX correlation at 3-month tenor |
| `rho_1y` | Implied equity-FX correlation at 1-year tenor |
| `delta_var_1m` | Raw numerator $\sigma^2_{adr} - \sigma^2_{loc} - \sigma^2_{fx}$ at 1M (%² units) |
| `delta_var_3m` | Same at 3M |
| `delta_var_1y` | Same at 1Y |

Summary statistics for the rho series:

| | 1-Month | 3-Month | 1-Year |
|---|---|---|---|
| Mean | 0.104 | 0.151 | 0.116 |
| Std | 0.314 | 0.261 | 0.227 |
| Median | 0.105 | 0.149 | 0.125 |
| 5th pct | −0.414 | −0.302 | −0.296 |
| 95th pct | 0.632 | 0.583 | 0.481 |

> **Note**: 1M rho has wider dispersion than 3M/1Y due to ASML's Amsterdam close
> (~11:30 ET) vs NYSE close (~16:00 ET). Use 3M and 1Y as primary series; treat
> 1M as indicative. See `timing_stability_check.py` for full sensitivity analysis.

---

## Quick Start

### 1. Clone and set up the environment

```bash
git clone https://github.com/kevin-m-chang/MATH-86-Quanto-Project.git
cd MATH-86-Quanto-Project
python -m venv .venv
source .venv/bin/activate        # macOS/Linux
# .venv\Scripts\activate         # Windows
pip install -r requirements.txt
```

### 2. Run the full pipeline

The raw CSVs and processed datasets are already committed — you can re-run from
scratch at any time:

```bash
python run_pipeline.py
```

This runs two steps in sequence:
1. **Step 1** (`ingest_csv_pipeline.py`) — loads the three Bloomberg CSVs, strips
   trailing formula columns, aligns on business days, saves `cleaned_dataset.csv`
2. **Step 2** (`compute_derived.py`) — computes rho and delta_var for all tenors,
   saves `derived_dataset.csv`, generates three plots in `outputs/figures/`

Each step can also be run standalone:

```bash
python src/data_ingestion/ingest_csv_pipeline.py
python src/analysis/compute_derived.py
```

### 3. Run the unit tests

```bash
pytest tests/ -v --ignore=tests/test_bbg_connection.py
```

All 30 tests are Bloomberg-free and run on synthetic data. `test_bbg_connection.py`
requires a live Bloomberg terminal and is excluded from the standard test run.

---

## Source Module Guide

### `src/data_ingestion/loader.py`
Reads Bloomberg CSV exports. Whitelists valid columns (stripping Bloomberg's
trailing formula/metadata columns), parses dates, coerces to float64, and applies
source prefixes (`adr_`, `loc_`, `fx_`). Also contains Excel loaders (preserved
for reference in case data is re-exported as `.xlsx`).

### `src/data_ingestion/cleaner.py`
Takes any number of DataFrames and aligns them to a shared business-day index:
inner join → forward-fill holidays (≤3 days) → drop residual NaNs.

### `src/data_ingestion/ingest_csv_pipeline.py`
**Step 1 script.** Orchestrates loader + cleaner for the three ASML/EURUSD CSVs,
prints a structured status report, saves `cleaned_dataset.csv`.

### `src/analysis/implied_correlation.py`
Core library module. Contains:
- `CorrelationSpec` dataclass — identifies an ADR/local/FX triplet
- `compute_implied_correlation()` — general function with configurable column
  templates; called by `compute_derived.py` and directly tested by the unit tests
- `correlation_summary()` — descriptive statistics for any rho DataFrame

### `src/analysis/compute_derived.py`
**Step 2 script.** Calls `compute_implied_correlation()` from the library above,
computes `delta_var`, saves `derived_dataset.csv`, prints summary statistics,
generates all three plots.

### `src/analysis/timing_stability_check.py`
Standalone sanity check. Simulates a 1-day lag on the Amsterdam or FX close and
measures how much rho shifts. Conclusion: FX timing is benign (p95 deviation ≤0.04
rho-points); Amsterdam equity lag introduces moderate noise at 1M tenor
(p95 deviation 0.57 rho-points). Supports using 3M/1Y as primary series.

### `src/features/fx_vol_surface.py`
Given ATM + butterfly + risk reversal, reconstructs 25Δ call vol and put vol.
Auto-detects pair prefixes from column names.

### `src/features/skew.py`
Computes equity skew = σ_25p − σ_25c per ticker/tenor. Can also approximate as
−RR25 when individual wing vols are unavailable.

### `src/visualization/plots.py`
Reusable matplotlib functions: `plot_implied_correlation`, `plot_skew`,
`plot_rho_and_skew`. Saves PNGs to `outputs/figures/`.

---

## Re-pulling Data from Bloomberg

If you need to update the raw CSVs (e.g. extend the date range), use the scripts
in `scripts/`. These require a live Bloomberg terminal session.

```bash
# On a Bloomberg terminal (Windows):
C:\Users\...\python.exe scripts\bdh_pull_fx_spot.py
```

After re-pulling, re-run `python run_pipeline.py` to regenerate the processed
datasets and plots.

---

## Common Issues

| Symptom | Cause | Fix |
|---|---|---|
| `ModuleNotFoundError` | Dependency not installed | `pip install -r requirements.txt` |
| `FileNotFoundError: cleaned_dataset.csv` | Step 1 hasn't run | `python src/data_ingestion/ingest_csv_pipeline.py` |
| rho values outside [−1, 1] | Should never happen — clipped by formula | Check for NaN inputs in raw CSVs |
| 1M rho very noisy | Expected — Amsterdam/NYSE close-time gap | Use 3M/1Y as primary series |
| Bloomberg `ImportError` | `blpapi` not installed | Only needed for `scripts/` — not required for analysis |


---

## Environment

- **Machine**: Windows Bloomberg Terminal
- **Python**: Portable Python 3.11 at `C:\Users\Feldberg.Dartmouth\python\python.exe`
- **blpapi**: 3.26.1.1 (pre-verified working)
- **Core deps**: `blpapi`, `pandas`, `pyarrow`

---

## Setup

### 1. Confirm portable Python path

All scripts use `Path(__file__).resolve()` for file paths and expect to be run with
the explicit Python interpreter:

```
C:\Users\Feldberg.Dartmouth\python\python.exe scripts\bdh_pull_fx_spot.py
```

Never rely on a system `python` alias — use the full path.

### 2. Ensure Bloomberg Terminal is open and logged in

The Bloomberg API connects to the local terminal process. The terminal must be:
- Open and fully loaded (not just the splash screen)
- Logged in to your Bloomberg account
- Running before any script is executed

### 3. Verify API entitlements

If you see errors like `WORKFLOW_REVIEW_NEEDED` or permission-denied messages in
the response, it means your Bloomberg account lacks the data entitlement for the
requested field or ticker. Steps to resolve:
- In the Bloomberg terminal, type `PAPI <GO>` to review API permissions.
- Contact your Bloomberg representative or librarian to request data access.
- For academic access, Dartmouth Bloomberg terminals may have restricted fields —
  stick to `PX_LAST`, `PX_OPEN`, `PX_HIGH`, `PX_LOW`, `PX_VOLUME`.

### 4. Install Python dependencies

```powershell
C:\Users\Feldberg.Dartmouth\python\python.exe -m pip install pandas pyarrow
```

`blpapi` is pre-installed with the Bloomberg API SDK and should already be importable.

### 5. Run the connection test

```powershell
C:\Users\Feldberg.Dartmouth\python\python.exe tests\test_bbg_connection.py
```

Expected output:
```
Bloomberg connection successful
```

If you see a connection error, the terminal is not running or not logged in.

### 6. Run the EURUSD spot pull

```powershell
C:\Users\Feldberg.Dartmouth\python\python.exe scripts\bdh_pull_fx_spot.py
```

This pulls daily `PX_LAST` for `EURUSD Curncy` from 2015-01-01 to 2025-01-01
and saves to `data/raw/eurusd_spot.parquet`.

---

## Project Structure

```
MATH-86-Quanto-Project/
├── run_pipeline.py               # ← top-level entry point
│
├── src/
│   ├── data_ingestion/
│   │   ├── loader.py             # Load Bloomberg Excel exports
│   │   └── cleaner.py            # Align datasets to common business-day index
│   ├── features/
│   │   ├── fx_vol_surface.py     # Reconstruct FX 25Δ wing vols (ATM+BF±RR/2)
│   │   └── skew.py               # Compute equity skew (25P − 25C)
│   ├── analysis/
│   │   ├── implied_correlation.py # Compute ρ = (σ²_ADR − σ²_loc − σ²_FX) / (2σ_loc σ_FX)
│   │   └── build_dataset.py      # Orchestrate pipeline → data/processed/derived_dataset.csv
│   └── visualization/
│       └── plots.py              # Time-series plots of ρ and skew
│
├── scripts/
│   ├── bdh_generic.py            # Reusable Bloomberg BDH helper (live terminal)
│   └── bdh_pull_fx_spot.py       # One-shot EURUSD spot pull
│
├── tests/
│   ├── test_bbg_connection.py    # Live Bloomberg session smoke-test
│   ├── test_implied_correlation.py
│   ├── test_fx_vol_surface.py
│   ├── test_skew.py
│   └── test_cleaner.py
│
├── data/
│   ├── raw/                      # Bloomberg Excel exports (git-ignored)
│   └── processed/
│       └── derived_dataset.csv   # Pipeline output (git-ignored)
│
├── outputs/
│   └── figures/                  # PNG charts (git-ignored)
│
├── requirements.txt
└── .gitignore
```

---

## Theory

### Implied Equity-FX Correlation

For a quanto product, the ADR (priced in USD) behaves as a composite of the
local equity (priced in BRL) and the USD/BRL exchange rate. Under log-normal
dynamics the three implied vols satisfy:

$$\sigma_{\text{ADR}}^2 = \sigma_{\text{local}}^2 + \sigma_{\text{FX}}^2
  + 2\,\rho\,\sigma_{\text{local}}\,\sigma_{\text{FX}}$$

Rearranging gives the **implied correlation**:

$$\rho = \frac{\sigma_{\text{ADR}}^2 - \sigma_{\text{local}}^2 - \sigma_{\text{FX}}^2}
             {2\,\sigma_{\text{local}}\,\sigma_{\text{FX}}}$$

computed for each of the 1M, 3M, and 1Y maturities.

### FX Wing Vol Reconstruction

Bloomberg provides FX vols as ATM straddle, 25Δ butterfly, and 25Δ risk reversal:

$$\sigma_{25c} = \text{ATM} + \text{BF} + \tfrac{\text{RR}}{2}$$
$$\sigma_{25p} = \text{ATM} + \text{BF} - \tfrac{\text{RR}}{2}$$

### Equity Skew

$$\text{skew} = \sigma_{25p} - \sigma_{25c}$$

(positive when the volatility smile is negatively skewed, as typical for equities).

---

## Setup

### 1. Install Python dependencies

```bash
pip install -r requirements.txt
```

> `blpapi` is pre-installed via the Bloomberg API SDK — do not install from pip.

### 2. Prepare Bloomberg Excel exports

Export the following datasets from Bloomberg Terminal to `data/raw/`:

| File | Bloomberg source | Required columns |
|------|-----------------|-----------------|
| `adr_vols.xlsx` | Option implied vols for ADR tickers | `<TICKER>_1M`, `<TICKER>_3M`, `<TICKER>_1Y` |
| `local_vols.xlsx` | Option implied vols for local equity | `<TICKER>_1M`, `<TICKER>_3M`, `<TICKER>_1Y` |
| `fx_vols.xlsx` | FX vol surface (ATM, BF25, RR25) | `<PAIR>_ATM_<T>`, `<PAIR>_BF25_<T>`, `<PAIR>_RR25_<T>` |
| `fx_spot.xlsx` | FX spot rates | `<PAIR>_SPOT` |

Bloomberg Excel format assumed: **one metadata header row (row 1), column names in row 2, data from row 3**.

### 3. Configure tickers

Edit the `SPECS` list in `run_pipeline.py`:

```python
SPECS = [
    CorrelationSpec(
        adr_ticker   = "VALE",    # column prefix in adr_vols.xlsx
        local_ticker = "VALE3",   # column prefix in local_vols.xlsx
        fx_pair      = "USDBRL",  # column prefix in fx_vols.xlsx
    ),
]
```

### 4. Run the pipeline

```bash
python run_pipeline.py
```

Outputs:
- `data/processed/derived_dataset.csv` — aligned vols, ρ, skew
- `outputs/figures/implied_correlation.png`
- `outputs/figures/equity_skew.png`
- `outputs/figures/<ADR>_<LOCAL>_<PAIR>_<TENOR>_combo.png` — per-tenor ρ + skew

### 5. Run tests (no Bloomberg required)

```bash
pytest tests/ -v --ignore=tests/test_bbg_connection.py
```

---

---

## Common Failure Modes

| Symptom | Cause | Fix |
|---------|-------|-----|
| `ConnectionError` / session won't start | Terminal not open/logged in | Open terminal, log in, retry |
| `responseError` in event | Bad ticker or field | Double-check ticker on terminal, e.g. `EURUSD Curncy <GO>` |
| `securityError` | Entitlement missing | See §3 above |
| `WORKFLOW_REVIEW_NEEDED` | Compliance review required | Contact Bloomberg support |
| `ImportError: No module named blpapi` | Wrong Python interpreter | Use full path to portable Python |
| Empty DataFrame | Date range outside data availability | Try narrower range; check `PX_LAST` availability |

---

## Future Pipeline Goals

- **FX Vol Surface**: Pull FX option vol grids using `OVDV` tickers (e.g.
  `EURUSD1M Curncy` for 1-month atm vol) and surface tenors / deltas.
- **FX Option Correlation**: Use `FXOPT_CORRELATION` data fields to construct
  correlation matrices between currency pairs for quanto adjustments.
- **ADR Options from WRDS**: Supplement Bloomberg data with WRDS OptionMetrics
  data for ADR-listed options where Bloomberg coverage is thin.
- **Quanto Pricing Model**: Combine domestic/foreign rate curves, FX vol surface,
  and correlation inputs into a closed-form or Monte Carlo quanto pricer.

---

## Notes

- All raw data files (`.parquet`) are git-ignored. Re-pull from Bloomberg as needed.
- Scripts are idempotent: re-running a pull overwrites the parquet file in place.
- `bdh_generic.py` returns a tidy long-format DataFrame. Pivot as needed downstream.
