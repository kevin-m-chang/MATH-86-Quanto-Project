# MATH 86 Quanto Project

Research pipeline for computing **implied equity-FX correlation** from ADR,
local equity, and FX implied volatilities — a core input to quanto derivative pricing.

The pipeline loads Bloomberg-exported Excel files, cleans and aligns the data,
computes implied correlations and equity skew across 1M/3M/1Y maturities, saves
a derived dataset, and produces publication-quality time-series plots.

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
