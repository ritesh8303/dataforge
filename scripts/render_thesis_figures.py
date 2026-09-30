"""Render thesis figures from eval JSON (requires matplotlib)."""

from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
FIG_DIR = ROOT / "docs" / "thesis" / "latex" / "figures"
EVAL_PATH = ROOT / "evals" / "results" / "matching_eval.json"


def render_rq3_bars(out_path: Path) -> None:
    try:
        import matplotlib.pyplot as plt
    except ImportError:
        print("matplotlib not installed; skip PNG render", file=sys.stderr)
        return

    data = json.loads(EVAL_PATH.read_text(encoding="utf-8"))
    av = data["averages"]
    methods = ["bm25", "dense", "hybrid", "heuristic"]
    labels = ["BM25", "Dense", "Hybrid", "Heuristic"]
    values = [av[m]["ndcg@10"] for m in methods]
    colors = ["#94a3b8", "#2563eb", "#7c3aed", "#059669"]

    fig, ax = plt.subplots(figsize=(6, 4))
    bars = ax.bar(labels, values, color=colors, edgecolor="#333", linewidth=0.5)
    ax.set_ylabel("nDCG@10")
    ax.set_ylim(0, max(values) * 1.25)
    ax.set_title(
        f"RQ3 retrieval comparison ({data['n_queries']} queries × {data['n_jobs']} jobs)"
    )
    for bar, val in zip(bars, values):
        ax.text(
            bar.get_x() + bar.get_width() / 2,
            bar.get_height() + 0.005,
            f"{val:.3f}",
            ha="center",
            va="bottom",
            fontsize=10,
        )
    fig.tight_layout()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_path, dpi=150)
    plt.close(fig)
    print(f"Wrote {out_path}")


def main() -> int:
    if not EVAL_PATH.exists():
        print(f"Missing {EVAL_PATH}; run evals/run_matching_eval.py first", file=sys.stderr)
        return 1
    render_rq3_bars(FIG_DIR / "rq3_bars.png")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
