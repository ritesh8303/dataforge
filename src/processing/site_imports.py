"""Import PyPI packages when vendored copies under src/ shadow them (local Lambda bundle)."""

from __future__ import annotations

import importlib
import sys
from pathlib import Path

_SRC_DIR = Path(__file__).resolve().parents[1]


def import_site_package(name: str):
    """
    Import `name` from site-packages even if an incomplete vendored copy exists in src/.

    Lambda deployment vendors dependencies into src/ for ingest Lambdas. EURES runs on
    GitHub Actions / local dev with pip-installed packages and must not load those shadows.
    """
    shadow = _SRC_DIR / name
    src_str = str(_SRC_DIR)

    if shadow.is_dir():
        while src_str in sys.path:
            sys.path.remove(src_str)
        for key in list(sys.modules):
            if key == name or key.startswith(f"{name}."):
                del sys.modules[key]

    try:
        return importlib.import_module(name)
    finally:
        if src_str not in sys.path:
            sys.path.insert(0, src_str)
