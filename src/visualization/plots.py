"""
src/visualization/plots.py
----------------------------
Time-series plotting utilities for the implied equity-FX correlation pipeline.

Produces two classes of charts:

  1. Implied correlation (ρ) time series — one panel per ADR/local/FX triplet,
     overlaying the 1M, 3M, 1Y tenor series.

  2. Equity skew time series — one panel per instrument, overlaying tenors.

All functions return a matplotlib Figure so the caller can display or save them.
Figures are also optionally saved to outputs/figures/.

Styling
-------
- Consistent colour palette per tenor (1M = blue, 3M = orange, 1Y = green).
- Horizontal zero-line on skew charts.
- Recession shading can be optionally added (NBER dates).
- All fonts sized for inclusion in a research paper (11pt base).
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional

import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import pandas as pd

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Styling constants
# ---------------------------------------------------------------------------

TENOR_COLORS  = {"1M": "#1f77b4", "3M": "#ff7f0e", "1Y": "#2ca02c"}
TENOR_LABELS  = {"1M": "1-Month", "3M": "3-Month", "1Y": "1-Year"}
FIGURE_WIDTH  = 12
FIGURE_HEIGHT = 4   # per subplot row
FONT_SIZE     = 11

plt.rcParams.update({
    "font.size":        FONT_SIZE,
    "axes.titlesize":   FONT_SIZE + 1,
    "axes.labelsize":   FONT_SIZE,
    "legend.fontsize":  FONT_SIZE - 1,
    "xtick.labelsize":  FONT_SIZE - 1,
    "ytick.labelsize":  FONT_SIZE - 1,
    "figure.dpi":       120,
    "axes.grid":        True,
    "grid.alpha":       0.4,
    "grid.linestyle":   "--",
})

_PROJECT_ROOT   = Path(__file__).resolve().parent.parent.parent
_FIGURES_DIR    = _PROJECT_ROOT / "outputs" / "figures"

TENORS = ("1M", "3M", "1Y")


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _format_date_axis(ax: plt.Axes) -> None:
    """Apply a clean major/minor date axis format."""
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))
    ax.xaxis.set_major_locator(mdates.MonthLocator(interval=6))
    plt.setp(ax.xaxis.get_majorticklabels(), rotation=30, ha="right")


def _save_figure(
    fig: plt.Figure,
    filename: str,
    output_dir: Path | None = None,
) -> None:
    """Save figure to PNG in the outputs/figures directory."""
    out_dir = Path(output_dir) if output_dir else _FIGURES_DIR
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / filename
    fig.savefig(out_path, bbox_inches="tight")
    log.info("Figure saved → %s", out_path)


# ---------------------------------------------------------------------------
# Public plotting functions
# ---------------------------------------------------------------------------

def plot_implied_correlation(
    rho_df: pd.DataFrame,
    specs: list[tuple[str, str, str]],   # (adr_ticker, local_ticker, fx_pair)
    tenors: tuple[str, ...] = TENORS,
    save: bool = True,
    output_dir: str | Path | None = None,
    filename: str = "implied_correlation.png",
) -> plt.Figure:
    """
    Plot implied correlation ρ time series.

    One subplot per (ADR, local, FX) triplet; tenors overlaid as separate lines.

    Parameters
    ----------
    rho_df : pd.DataFrame
        Output of compute_implied_correlation(), columns named
        <ADR>_<LOC>_<PAIR>_RHO_<TENOR>.
    specs : list of (adr_ticker, local_ticker, fx_pair) tuples.
    tenors : tenors to plot.
    save : bool  — write PNG to outputs/figures/.
    output_dir : override default output directory.
    filename : output file name.

    Returns
    -------
    matplotlib.figure.Figure
    """
    n_specs = len(specs)
    fig, axes = plt.subplots(
        n_specs, 1,
        figsize=(FIGURE_WIDTH, FIGURE_HEIGHT * n_specs),
        sharex=True,
        squeeze=False,
    )

    for row_idx, (adr, loc, pair) in enumerate(specs):
        ax = axes[row_idx, 0]

        for tenor in tenors:
            col = f"{adr}_{loc}_{pair}_RHO_{tenor}"
            if col not in rho_df.columns:
                log.warning("Column %s not found in rho_df — skipping.", col)
                continue
            ax.plot(
                rho_df.index,
                rho_df[col],
                color=TENOR_COLORS.get(tenor, "black"),
                linewidth=1.2,
                label=TENOR_LABELS.get(tenor, tenor),
            )

        ax.axhline(0, color="black", linewidth=0.8, linestyle="--", alpha=0.5)
        ax.set_ylim(-1.05, 1.05)
        ax.set_ylabel("Implied ρ")
        ax.set_title(f"Implied Equity–FX Correlation: {adr} / {loc} / {pair}")
        ax.legend(loc="upper right", framealpha=0.8)
        _format_date_axis(ax)

    axes[-1, 0].set_xlabel("Date")
    fig.suptitle("Implied Equity–FX Correlation", fontsize=FONT_SIZE + 3, y=1.01)
    fig.tight_layout()

    if save:
        _save_figure(fig, filename, output_dir)

    return fig


def plot_skew(
    skew_df: pd.DataFrame,
    label: str = "Equity Skew",
    tenors: tuple[str, ...] = TENORS,
    save: bool = True,
    output_dir: str | Path | None = None,
    filename: str = "equity_skew.png",
) -> plt.Figure:
    """
    Plot equity skew time series.

    One subplot per ticker (all tenors overlaid).

    Parameters
    ----------
    skew_df : pd.DataFrame
        Output of compute_equity_skew(), columns named
        <TICKER>_SKEW_<TENOR>.
    label : overall chart title.
    tenors : tenors to plot.
    save : write to PNG.
    output_dir : override default output directory.
    filename : output file name.

    Returns
    -------
    matplotlib.figure.Figure
    """
    # Detect unique tickers from columns of the form <TICKER>_SKEW_<TENOR>.
    tickers = list(
        dict.fromkeys(
            col.rsplit("_SKEW_", 1)[0]
            for col in skew_df.columns
            if "_SKEW_" in col
        )
    )

    if not tickers:
        raise ValueError("No _SKEW_ columns found in skew_df.")

    n_tickers = len(tickers)
    fig, axes = plt.subplots(
        n_tickers, 1,
        figsize=(FIGURE_WIDTH, FIGURE_HEIGHT * n_tickers),
        sharex=True,
        squeeze=False,
    )

    for row_idx, ticker in enumerate(tickers):
        ax = axes[row_idx, 0]

        for tenor in tenors:
            col = f"{ticker}_SKEW_{tenor}"
            if col not in skew_df.columns:
                continue
            ax.plot(
                skew_df.index,
                skew_df[col],
                color=TENOR_COLORS.get(tenor, "black"),
                linewidth=1.2,
                label=TENOR_LABELS.get(tenor, tenor),
            )

        ax.axhline(0, color="black", linewidth=0.8, linestyle="--", alpha=0.5)
        ax.set_ylabel("Skew (25P − 25C)")
        ax.set_title(f"Equity Skew: {ticker}")
        ax.legend(loc="upper right", framealpha=0.8)
        _format_date_axis(ax)

    axes[-1, 0].set_xlabel("Date")
    fig.suptitle(label, fontsize=FONT_SIZE + 3, y=1.01)
    fig.tight_layout()

    if save:
        _save_figure(fig, filename, output_dir)

    return fig


def plot_rho_and_skew(
    rho_df: pd.DataFrame,
    skew_df: pd.DataFrame,
    adr_ticker: str,
    local_ticker: str,
    fx_pair: str,
    tenor: str = "1M",
    save: bool = True,
    output_dir: str | Path | None = None,
    filename: str | None = None,
) -> plt.Figure:
    """
    Plot implied ρ and equity skew for a single triplet on a shared time axis
    with two vertically stacked panels.

    Parameters
    ----------
    rho_df : pd.DataFrame   — implied correlation frame.
    skew_df : pd.DataFrame  — equity skew frame (ADR or local).
    adr_ticker, local_ticker, fx_pair : str — identify the series.
    tenor : str             — which tenor to overlay (e.g. "1M").
    save : bool
    output_dir : Path | None
    filename : str | None   — defaults to <adr>_<loc>_<pair>_<tenor>_combo.png

    Returns
    -------
    matplotlib.figure.Figure
    """
    rho_col   = f"{adr_ticker}_{local_ticker}_{fx_pair}_RHO_{tenor}"
    skew_col  = f"{adr_ticker}_SKEW_{tenor}"

    if filename is None:
        filename = f"{adr_ticker}_{local_ticker}_{fx_pair}_{tenor}_combo.png"

    fig, (ax1, ax2) = plt.subplots(
        2, 1,
        figsize=(FIGURE_WIDTH, FIGURE_HEIGHT * 2),
        sharex=True,
    )

    # — top panel: ρ —
    if rho_col in rho_df.columns:
        ax1.plot(
            rho_df.index, rho_df[rho_col],
            color=TENOR_COLORS.get(tenor, "steelblue"),
            linewidth=1.4,
            label=f"ρ ({TENOR_LABELS.get(tenor, tenor)})",
        )
    ax1.axhline(0, color="black", linewidth=0.8, linestyle="--", alpha=0.5)
    ax1.set_ylim(-1.05, 1.05)
    ax1.set_ylabel("Implied ρ")
    ax1.set_title(
        f"Implied Correlation & Skew — {adr_ticker}/{local_ticker}/{fx_pair} ({tenor})"
    )
    ax1.legend(loc="upper right", framealpha=0.8)

    # — bottom panel: skew —
    if skew_col in skew_df.columns:
        ax2.plot(
            skew_df.index, skew_df[skew_col],
            color=TENOR_COLORS.get(tenor, "darkorange"),
            linewidth=1.4,
            label=f"Skew ({TENOR_LABELS.get(tenor, tenor)})",
        )
    ax2.axhline(0, color="black", linewidth=0.8, linestyle="--", alpha=0.5)
    ax2.set_ylabel("Equity Skew")
    ax2.set_xlabel("Date")
    ax2.legend(loc="upper right", framealpha=0.8)
    _format_date_axis(ax2)

    fig.tight_layout()

    if save:
        _save_figure(fig, filename, output_dir)

    return fig
