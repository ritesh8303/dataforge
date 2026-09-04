"""Compare router cost vs always-best (local) provider."""

from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "src"))

from ai_gateway.router import ModelRouter


def main():
    router = ModelRouter()
    prompts = [
        "Summarize: Senior Python data engineer role in Berlin with AWS and Spark.",
        "Extract skills from: Machine learning engineer with PyTorch and LLM experience.",
    ]
    for p in prompts:
        router.complete("summarize", p)
        router.embed("embed", p)

    summary = router.cost_logger.summary()
    out_dir = ROOT / "evals" / "results"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "router_eval.json"
    out_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
