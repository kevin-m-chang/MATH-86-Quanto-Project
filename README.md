# MATH 86 Quanto Project

Bloomberg Desktop API (blpapi) data-pull pipeline for FX spot rates, vol surfaces,
and related instruments. Final target is a quanto pricing model fed by live market data.

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
math86-quanto-project/
├── scripts/
│   ├── bdh_pull_fx_spot.py     # One-shot EURUSD spot pull (HistoricalDataRequest)
│   └── bdh_generic.py          # Reusable bdh() function, returns tidy DataFrame
├── tests/
│   └── test_bbg_connection.py  # Verify Bloomberg session opens and refdata available
├── data/
│   ├── raw/                    # Parquet files from Bloomberg (git-ignored)
│   └── processed/              # Cleaned / transformed data (git-ignored)
├── README.md
├── requirements.txt
└── .gitignore
```

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
