#!/usr/bin/env python3
"""
Plot gas-metering benchmark results from a CSV produced by gas-metering-bench.

Usage:
    ./target/release/gas-metering-bench --csv-output > results.csv
    python3 runtime/near-vm-runner/benchmarks/plot_gas_metering.py results.csv [out_base]

Produces one PNG per filter set, named <out_base>_<set>.png.
Default out_base is derived from the CSV filename (strip .csv suffix).

Filter sets
-----------
all          All strategies.
gas-no-host  Gas-counting only, excluding host-function and no-gas strategies.
winch        Winch strategies only.
fw           Finite-wasm instrumented strategies (strategy name contains "-fw").
no-gas       Non-gas-counting baselines (ng variants + cranelift-fuel).

Layout: 3 rows (instrumentation / compilation / execution) × N columns (one per
contract). Each cell is a bar chart with strategies on the X-axis.
"""

import csv
import os
import sys
from collections import OrderedDict

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np

CURRENT_STRATEGY  = "winch-fw-inline"
PREVIOUS_STRATEGY = "near-vm"

# Display order within each subplot (strategies not listed appear at the end).
STRATEGY_ORDER = [
    "near-vm",
    "winch-fw-inline",
    "winch-fw",
    "winch-fw-subcheck",
    "winch-fw-local",
    "cranelift-fw-inline",
    "cranelift-fw",
    "cranelift-fw-subcheck",
    "cranelift-fw-local",
    "cranelift-fw-wt-inlined",
    "winch-fw-host",
    "cranelift-fw-host",
    "cranelift-fuel",
    "winch-ng",
    "near-vm-ng",
    "cranelift-ng",
]

# Consistent palette per strategy name.
STRATEGY_COLORS = {
    # Winch: blue family, darker = more optimized hot path
    "winch-fw-inline":         "#8ab4f0",  # light blue  — original inline (2 global.gets)
    "winch-fw-subcheck":       "#5a9de0",  # medium blue — subtract-first (1 global.get)
    "winch-fw-local":          "#1a5eb0",  # dark blue   — local counter (0 global.gets)
    "winch-fw":                "#4c9be8",  # production (module-defined gas_check call)
    "winch-fw-host":           "#007070",  # teal        — direct host call
    "winch-ng":                "#b0ccf8",  # very light blue — no gas (baseline)
    # Cranelift: red/orange family, same progression
    "cranelift-fw-inline":     "#f0a090",  # light red   — original inline
    "cranelift-fw-subcheck":   "#e07060",  # medium red  — subtract-first
    "cranelift-fw-local":      "#a02010",  # dark red    — local counter
    "cranelift-fw":            "#e86c4c",  # production
    "cranelift-fw-wt-inlined": "#c06000",  # brown-orange — wasmtime inlining
    "cranelift-fw-host":       "#20a0a0",  # teal        — direct host call
    "cranelift-ng":            "#f8c4b8",  # very light red — no gas (baseline)
    "cranelift-fuel":          "#e8c240",  # yellow      — wasmtime fuel
    # NearVM reference
    "near-vm":                 "#5cb85c",
    "near-vm-ng":              "#9dd69d",
}
DEFAULT_COLOR = "#999999"
NA_COLOR      = "#dddddd"

# (y-axis label, CSV field, multiplier → ms)
METRICS = [
    ("Instrumentation (ms)",    "instrument_ms", 1.0),
    ("Compilation (ms)",        "compile_ms",    1.0),
    ("Execution avg/call (ms)", "exec_ns",       1e-6),
]

# Each entry: (file_suffix, title_suffix, predicate(strategy_name) → bool)
FILTER_SETS = [
    (
        "all",
        "All strategies",
        lambda s: True,
    ),
    (
        "gas-no-host",
        "Gas-counting, no host-function approach",
        lambda s: not s.endswith("-ng")
                  and s != "cranelift-fuel"
                  and not s.endswith("-host"),
    ),
    (
        "winch",
        "Winch strategies only",
        lambda s: s.startswith("winch-") or s == "near-vm",
    ),
    (
        "fw",
        "Finite-wasm instrumented (fw) strategies",
        lambda s: "-fw" in s or s == "near-vm",
    ),
    (
        "no-gas",
        "Non-gas-counting baselines",
        lambda s: s.endswith("-ng") or s == "cranelift-fuel",
    ),
]


def load_csv(path: str) -> list[dict]:
    with open(path, newline="") as f:
        return list(csv.DictReader(f))


def parse_float(s: str) -> tuple[float, bool]:
    """Return (value, is_na). is_na=True when the field is empty."""
    s = s.strip()
    if not s:
        return 0.0, True
    try:
        return float(s), False
    except ValueError:
        return 0.0, True


def plot_subset(
    groups: "OrderedDict[tuple, list[dict]]",
    predicate,
    title: str,
    out_path: str,
) -> None:
    # Filter and sort rows within each group.
    def sort_key(r):
        s = r["strategy"]
        try:
            return STRATEGY_ORDER.index(s)
        except ValueError:
            return len(STRATEGY_ORDER)

    filtered: OrderedDict = OrderedDict()
    for key, rows in groups.items():
        subset = sorted([r for r in rows if predicate(r["strategy"])], key=sort_key)
        if subset:
            filtered[key] = subset

    if not filtered:
        print(f"  skip {out_path}: no rows matched the filter", file=sys.stderr)
        return

    n_contracts = len(filtered)
    n_metrics   = len(METRICS)

    max_strats  = max(len(g) for g in filtered.values())
    col_width   = max(3.5, 0.5 * max_strats + 1.5)
    fig, axes = plt.subplots(
        n_metrics, n_contracts,
        figsize=(col_width * n_contracts, 3.2 * n_metrics),
        squeeze=False,
    )

    for col, ((contract, method), contract_rows) in enumerate(filtered.items()):
        strategies        = [r["strategy"] for r in contract_rows]
        is_current_flags  = [s == CURRENT_STRATEGY  for s in strategies]
        is_previous_flags = [s == PREVIOUS_STRATEGY for s in strategies]
        x = np.arange(len(strategies))

        for row_idx, (ylabel, field, scale) in enumerate(METRICS):
            ax = axes[row_idx][col]

            values, na_flags = [], []
            for r in contract_rows:
                v, is_na = parse_float(r.get(field, ""))
                values.append(v * scale)
                na_flags.append(is_na)

            bar_colors  = [
                NA_COLOR if is_na else STRATEGY_COLORS.get(s, DEFAULT_COLOR)
                for s, is_na in zip(strategies, na_flags)
            ]
            edge_colors = [
                "black"  if cur else ("#9a4a00" if prev else "white")
                for cur, prev in zip(is_current_flags, is_previous_flags)
            ]
            edge_widths = [
                2.0 if cur else (2.0 if prev else 0.5)
                for cur, prev in zip(is_current_flags, is_previous_flags)
            ]

            ax.bar(x, values, color=bar_colors, edgecolor=edge_colors,
                   linewidth=edge_widths)

            for i, (is_na, is_cur, is_prev) in enumerate(
                zip(na_flags, is_current_flags, is_previous_flags)
            ):
                if is_na:
                    ax.text(i, 0.04, "N/A",
                            ha="center", va="bottom", fontsize=6, color="#888888",
                            transform=ax.get_xaxis_transform())
                if is_cur:
                    ax.text(i, 1.01, "current",
                            ha="center", va="bottom", fontsize=6, color="black",
                            transform=ax.get_xaxis_transform())
                if is_prev:
                    ax.text(i, 1.01, "previous",
                            ha="center", va="bottom", fontsize=6, color="#9a4a00",
                            transform=ax.get_xaxis_transform())

            ax.set_xticks(x)
            ax.set_xticklabels(strategies, rotation=40, ha="right", fontsize=7)
            ax.set_ylabel(ylabel, fontsize=8)
            ax.tick_params(axis="y", labelsize=7)
            ax.grid(axis="y", alpha=0.3, linewidth=0.5)
            ax.set_axisbelow(True)

            if row_idx == 0:
                col_title = contract if method == contract else f"{contract}\n{method}"
                ax.set_title(col_title, fontsize=9)

    fig.suptitle(f"Gas Metering Benchmark — {title}", fontsize=12)
    fig.tight_layout()
    fig.savefig(out_path, bbox_inches="tight", dpi=150)
    plt.close(fig)
    print(f"saved: {out_path}", file=sys.stderr)


def main() -> None:
    if len(sys.argv) < 2:
        sys.exit(f"Usage: {sys.argv[0]} <results.csv> [out_base]")

    csv_path = sys.argv[1]

    # Derive default base name from the CSV filename.
    default_base = os.path.splitext(os.path.basename(csv_path))[0]
    out_base     = sys.argv[2] if len(sys.argv) > 2 else default_base
    # Strip trailing .png if the user passed a full path.
    if out_base.endswith(".png"):
        out_base = out_base[:-4]

    rows = load_csv(csv_path)

    # Group by (contract, method) preserving CSV order.
    groups: OrderedDict = OrderedDict()
    for row in rows:
        key = (row["contract"], row["method"])
        groups.setdefault(key, []).append(row)

    for suffix, title, predicate in FILTER_SETS:
        out_path = f"{out_base}_{suffix}.png"
        plot_subset(groups, predicate, title, out_path)


if __name__ == "__main__":
    main()
