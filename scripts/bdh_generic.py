"""
scripts/bdh_generic.py
-----------------------
Reusable Bloomberg HistoricalDataRequest helper.

Usage (import from another script):
    from scripts.bdh_generic import bdh

    df = bdh(
        tickers=["EURUSD Curncy", "GBPUSD Curncy"],
        fields=["PX_LAST", "PX_OPEN"],
        start="20200101",
        end="20250101",
        periodicity="DAILY",
    )

    # df columns: date (pd.Timestamp), ticker (str), field (str), value (float)
    print(df.head())

Or run directly to do a quick smoke-test against the live terminal:
    C:\\Users\\Feldberg.Dartmouth\\python\\python.exe scripts\\bdh_generic.py
"""

import sys
import logging
from pathlib import Path
from typing import Optional

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

SERVICE_NAME = "//blp/refdata"
_TIMEOUT_MS = 60_000  # 60-second per-event wait


def _open_session() -> blpapi.Session:
    """Create, start, and return a Bloomberg session with refdata open."""
    options = blpapi.SessionOptions()
    options.setServerHost("localhost")
    options.setServerPort(8194)

    session = blpapi.Session(options)

    if not session.start():
        raise RuntimeError(
            "session.start() failed.  "
            "Is the Bloomberg Terminal open and logged in?"
        )

    if not session.openService(SERVICE_NAME):
        session.stop()
        raise RuntimeError(
            f"session.openService('{SERVICE_NAME}') failed.  "
            "Check terminal login and refdata entitlement."
        )

    return session


def _check_message_errors(msg: blpapi.Message, ticker: str) -> None:
    """
    Inspect a single message for responseError or securityError and raise
    RuntimeError with the Bloomberg error string if found.
    """
    if msg.hasElement("responseError"):
        err = msg.getElement("responseError")
        raise RuntimeError(
            f"responseError from Bloomberg (ticker='{ticker}'): {err.toString()}"
        )

    if msg.hasElement("securityData"):
        sec_data = msg.getElement("securityData")
        if sec_data.hasElement("securityError"):
            err = sec_data.getElement("securityError")
            raise RuntimeError(
                f"securityError for '{ticker}': {err.toString()}"
            )


def bdh(
    tickers: list[str],
    fields: list[str],
    start: str,
    end: str,
    periodicity: str = "DAILY",
    fill_non_trading: bool = True,
    session: Optional[blpapi.Session] = None,
) -> pd.DataFrame:
    """
    Bloomberg Historical Data pull.

    Parameters
    ----------
    tickers : list[str]
        Bloomberg tickers, e.g. ["EURUSD Curncy", "GBPUSD Curncy"].
    fields : list[str]
        Bloomberg field mnemonics, e.g. ["PX_LAST", "PX_OPEN"].
    start : str
        Start date as "YYYYMMDD".
    end : str
        End date as "YYYYMMDD".
    periodicity : str
        "DAILY" (default), "WEEKLY", "MONTHLY", "QUARTERLY", "YEARLY".
    fill_non_trading : bool
        If True, forward-fill non-trading weekdays (Bloomberg default behaviour).
    session : blpapi.Session, optional
        Pass an already-open session to reuse.  If None, a new session is
        opened and closed within this call.

    Returns
    -------
    pd.DataFrame
        Tidy long-format DataFrame with columns:
            date    (pd.Timestamp)
            ticker  (str)
            field   (str)
            value   (float | None)
        Sorted by ticker, field, date.
    """
    if not tickers:
        raise ValueError("tickers list must not be empty.")
    if not fields:
        raise ValueError("fields list must not be empty.")

    _own_session = session is None
    if _own_session:
        session = _open_session()

    try:
        refdata_service = session.getService(SERVICE_NAME)
        request = refdata_service.createRequest("HistoricalDataRequest")

        for ticker in tickers:
            request.getElement("securities").appendValue(ticker)
        for field in fields:
            request.getElement("fields").appendValue(field)

        request.set("startDate", start)
        request.set("endDate", end)
        request.set("periodicityAdjustment", "ACTUAL")
        request.set("periodicitySelection", periodicity)

        if fill_non_trading:
            request.set("nonTradingDayFillOption", "NON_TRADING_WEEKDAYS")
            request.set("nonTradingDayFillMethod", "PREVIOUS_VALUE")
        else:
            request.set("nonTradingDayFillOption", "ALL_CALENDAR_DAYS")
            request.set("nonTradingDayFillMethod", "NIL_VALUE")

        log.info(
            "bdh: %d ticker(s), %d field(s), %s → %s, periodicity=%s",
            len(tickers), len(fields), start, end, periodicity,
        )
        session.sendRequest(request)

        rows: list[dict] = []

        while True:
            event = session.nextEvent(timeout=_TIMEOUT_MS)

            if event.eventType() in (
                blpapi.Event.RESPONSE,
                blpapi.Event.PARTIAL_RESPONSE,
            ):
                for msg in event:
                    # Each message contains data for one ticker.
                    if not msg.hasElement("securityData"):
                        continue

                    security_data = msg.getElement("securityData")
                    # Retrieve the ticker name as Bloomberg echoes it back.
                    ticker_name: str = security_data.getElementAsString(
                        "security"
                    )

                    _check_message_errors(msg, ticker_name)

                    field_data_array = security_data.getElement("fieldData")

                    for i in range(field_data_array.numValues()):
                        point = field_data_array.getValueAsElement(i)
                        date_val = point.getElementAsDatetime("date")
                        ts = pd.Timestamp(
                            date_val.year, date_val.month, date_val.day
                        )

                        for field_name in fields:
                            if not point.hasElement(field_name):
                                value = None
                            else:
                                try:
                                    value = point.getElementAsFloat(field_name)
                                except blpapi.exception.InvalidConversionException:
                                    value = None

                            rows.append(
                                {
                                    "date": ts,
                                    "ticker": ticker_name,
                                    "field": field_name,
                                    "value": value,
                                }
                            )

            if event.eventType() == blpapi.Event.RESPONSE:
                break

            if event.eventType() == blpapi.Event.TIMEOUT:
                raise RuntimeError(
                    f"Timed out waiting for Bloomberg response ({_TIMEOUT_MS // 1000} s).  "
                    "Check terminal connection."
                )

    finally:
        if _own_session:
            session.stop()

    if not rows:
        raise RuntimeError(
            f"No data returned for tickers={tickers} fields={fields} "
            f"{start}–{end}.  Check ticker spelling and entitlement."
        )

    df = (
        pd.DataFrame(rows)
        .sort_values(["ticker", "field", "date"])
        .reset_index(drop=True)
    )
    log.info("bdh: received %d rows.", len(df))
    return df


# ---------------------------------------------------------------------------
# Quick smoke-test when run directly
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    TEST_TICKERS = ["EURUSD Curncy"]
    TEST_FIELDS = ["PX_LAST"]
    TEST_START = "20240101"
    TEST_END = "20240110"

    log.info(
        "Running smoke-test: %s %s %s → %s",
        TEST_TICKERS, TEST_FIELDS, TEST_START, TEST_END,
    )

    result = bdh(
        tickers=TEST_TICKERS,
        fields=TEST_FIELDS,
        start=TEST_START,
        end=TEST_END,
    )

    print(result.to_string(index=False))
    print(f"\n{len(result)} rows returned.")
