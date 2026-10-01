"""Versioned prompt registry for LLMOps / thesis reproducibility.

Resolves prompt markdown from (in order):
1. This package directory (src/prompts — Lambda zip)
2. Repo-root prompts/ (local / CI when PYTHONPATH includes repo root)
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

_HERE = Path(__file__).resolve().parent
_CANDIDATE_DIRS = [
    _HERE,
    _HERE.parent.parent / "prompts",  # repo_root/prompts when this file is src/prompts/registry.py
]


@dataclass(frozen=True)
class PromptVersion:
    name: str
    version: str
    text: str

    @property
    def id(self) -> str:
        return f"{self.name}@{self.version}"


_CACHE: dict[str, PromptVersion] = {}


def _find_prompt_path(name: str, version: str) -> Path:
    candidates = [
        f"{name}_{version}.md",
        f"{name}.md",
    ]
    for directory in _CANDIDATE_DIRS:
        if not directory.is_dir():
            continue
        for fname in candidates:
            path = directory / fname
            if path.exists():
                return path
    raise FileNotFoundError(f"Prompt not found: {name}@{version} in {_CANDIDATE_DIRS}")


def load_prompt(name: str, version: str = "v1") -> PromptVersion:
    key = f"{name}@{version}"
    if key in _CACHE:
        return _CACHE[key]
    path = _find_prompt_path(name, version)
    text = path.read_text(encoding="utf-8").strip()
    pv = PromptVersion(name=name, version=version, text=text)
    _CACHE[key] = pv
    return pv


def list_prompts() -> list[str]:
    names: set[str] = set()
    for directory in _CANDIDATE_DIRS:
        if not directory.is_dir():
            continue
        for p in directory.glob("*.md"):
            names.add(p.stem)
    return sorted(names)
