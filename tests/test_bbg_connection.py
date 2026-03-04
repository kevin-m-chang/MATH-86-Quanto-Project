"""
tests/test_bbg_connection.py
----------------------------
Verify that a Bloomberg session can be established and the reference-data service
is reachable.  Run from the project root with the portable Python interpreter:

    C:\\Users\\Feldberg.Dartmouth\\python\\python.exe tests\\test_bbg_connection.py

Expected output (terminal open and logged in):
    Bloomberg connection successful

Exit codes:
    0  — connection OK
    1  — connection failed (see error message for details)
"""

import sys
import logging

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
        "Could not import blpapi.  "
        "Make sure you are using the Bloomberg portable Python at "
        "C:\\Users\\Feldberg.Dartmouth\\python\\python.exe "
        "and that the Bloomberg API SDK is installed."
    )
    sys.exit(1)

SERVICE_NAME = "//blp/refdata"


def test_connection() -> None:
    """Start a Bloomberg session and open the reference-data service."""
    options = blpapi.SessionOptions()
    # Default localhost:8194 — Bloomberg Desktop API standard.
    options.setServerHost("localhost")
    options.setServerPort(8194)

    session = blpapi.Session(options)

    log.info("Starting Bloomberg session (localhost:8194)…")
    if not session.start():
        log.error(
            "session.start() returned False.  "
            "Is the Bloomberg Terminal open and logged in?"
        )
        sys.exit(1)

    log.info("Opening service %s…", SERVICE_NAME)
    if not session.openService(SERVICE_NAME):
        log.error(
            "session.openService('%s') returned False.  "
            "Check that the terminal is logged in and you have refdata entitlement.",
            SERVICE_NAME,
        )
        session.stop()
        sys.exit(1)

    session.stop()
    print("Bloomberg connection successful")


if __name__ == "__main__":
    test_connection()
