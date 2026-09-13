"""Versioned prompt registry for LLMOps / thesis reproducibility."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PROMPTS_DIR = Path(__file__).resolve().parent


@dataclass(frozen=True)
class PromptVersion:
    name: str
    version: str
    text: str

    @property
    def id(self) -> str:
        return f"{self.name}@{self.version}"


_CACHE: dict[str, PromptVersion] = {}


def load_prompt(name: str, version: str = "v1") -> PromptVersion:
    key = f"{name}@{version}"
    if key in _CACHE:
        return _CACHE[key]
    path = PROMPTS_DIR / f"{name}_{version}.md"
    if not path.exists():
        # Allow name_v1.md or name.md
        alt = PROMPTS_DIR / f"{name}.md"
        if not alt.exists():
            raise FileNotFoundError(f"Prompt not found: {path}")
        path = alt
    text = path.read_text(encoding="utf-8").strip()
    pv = PromptVersion(name=name, version=version, text=text)
    _CACHE[key] = pv
    return pv


def list_prompts() -> list[str]:
    return sorted(p.stem for p in PROMPTS_DIR.glob("*.md"))
