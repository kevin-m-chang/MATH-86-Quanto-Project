"""
scripts/bdh_pull_fx_spot.py
----------------------------
Pull daily EURUSD spot rates from Bloomberg using a raw HistoricalDataRequest
and save the result to data/raw/eurusd_spot.parquet.

Run from the project root:
    C:\\Users\\Feldberg.Dartmouth\\python\\python.exe scripts\\bdh_pull_fx_spot.py

Requirements:
    - Bloomberg Terminal open and logged in
    - blpapi, pandas, pyarrow installed for the portable Python interpreter
"""

import sys
import logging
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

try:
    import blpapi
except ImportError:
    log.error(
        "Could not import blpapi.  Use the portable Python at "
        "C:\\Users\\Feldberg.Dartmouth\\python\\python.exe"
    )
    sys.exit(1)

try:
    import pandas as pd
except ImportError:
    log.error("pandas not found.  Run: python -m pip install pandas pyarrow")
    sys.exit(1)

# ---------------------------------------------------------------------------
# Configuration — edit these constants to change the pull.
# ---------------------------------------------------------------------------
TICKER = "EURUSD Curncy"
FIELD = "PX_LAST"
START_DATE = "20150101"
END_DATE = "20250101"
PERIODICITY = "DAILY"

# Output path is always relative to this script's location, not CWD.
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
OUTPUT_PATH = _PROJECT_ROOT / "data" / "raw" / "eurusd_spot.parquet"

SERVICE_NAME = "//blp/refdata"


def _check_response_errors(event: blpapi.Event) -> None:
    """Raise RuntimeError if the event contains a responseError or securityError."""
    for msg in event:
        if msg.hasElement("responseError"):
            err = msg.getElement("responseError")
            raise RuntimeError(
                f"responseError from Bloomberg: {err.toString()}"
            )
        if msg.hasElement("securityData"):
            sec_data = msg.getElement("securityData")
            if sec_data.hasElement("securityError"):
                err = sec_data.getElement("securityError")
                raise RuntimeError(
                    f"securityError for '{TICKER}': {err.toString()}"
                )


def pull_eurusd_spot() -> pd.DataFrame:
    """
    Open a Bloomberg session, fire a HistoricalDataRequest for EURUSD Curncy
    PX_LAST, collect all response events, and return a DataFrame with columns
    [date, value].
    """
    options = blpapi.SessionOptions()
    options.setServerHost("localhost")
    options.setServerPort(8194)

    session = blpapi.Session(options)

    log.info("Starting Bloomberg session…")
    if not session.start():
        raise RuntimeError(
            "session.start() failed.  "
            "Is the Bloomberg Terminal open and logged in?"
        )

    log.info("Opening %s…", SERVICE_NAME)
    if not session.openService(SERVICE_NAME):
        session.stop()
        raise RuntimeError(
            f"session.openService('{SERVICE_NAME}') failed.  "
            "Check terminal login and refdata entitlement."
        )

    refdata_service = session.getService(SERVICE_NAME)
    request = refdata_service.createRequest("HistoricalDataRequest")

    request.getElement("securities").appendValue(TICKER)
    request.getElement("fields").appendValue(FIELD)
    request.set("startDate", START_DATE)
    request.set("endDate", END_DATE)
    request.set("periodicityAdjustment", "ACTUAL")
    request.set("periodicitySelection", PERIODICITY)
    request.set("nonTradingDayFillOption", "NON_TRADING_WEEKDAYS")
    request.set("nonTradingDayFillMethod", "PREVIOUS_VALUE")

    log.info(
        "Sending HistoricalDataRequest: %s  %s  %s → %s",
        TICKER, FIELD, START_DATE, END_DATE,
    )
    session.sendRequest(request)

    rows: list[dict] = []

    # Stream events until we receive the final RESPONSE event type.
    try:
        while True:
            event = session.nextEvent(timeout=30_000)  # 30-second timeout

            if event.eventType() in (
                blpapi.Event.RESPONSE,
                blpapi.Event.PARTIAL_RESPONSE,
            ):
                _check_response_errors(event)

                for msg in event:
                    security_data = msg.getElement("securityData")
                    field_data_array = security_data.getElement("fieldData")

                    for i in range(field_data_array.numValues()):
                        point = field_data_array.getValueAsElement(i)
                        date_val = point.getElementAsDatetime("date")
                        price = point.getElementAsFloat(FIELD)
                        rows.append(
                            {
                                "date": pd.Timestamp(
                                    date_val.year,
                                    date_val.month,
                                    date_val.day,
                                ),
                                "value": price,
                            }
                        )

            if event.eventType() == blpapi.Event.RESPONSE:
                break  # Final (non-partial) response — we are done.

            if event.eventType() == blpapi.Event.TIMEOUT:
                raise RuntimeError(
                    "Timed out waiting for Bloomberg response (30 s).  "
                    "Check terminal connection."
                )

    finally:
        session.stop()

    if not rows:
        raise RuntimeError(
            f"No data returned for {TICKER} {FIELD} {START_DATE}–{END_DATE}.  "
            "Check ticker spelling and entitlement."
        )

    df = pd.DataFrame(rows).sort_values("date").reset_index(drop=True)
    return df


def main() -> None:
    df = pull_eurusd_spot()

    log.info("Head of pulled data:\n%s", df.head().to_string(index=False))

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(OUTPUT_PATH, index=False)

    print(f"Saved {len(df):,} rows  →  {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
