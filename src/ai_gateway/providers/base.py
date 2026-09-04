from __future__ import annotations

import json
import time
from abc import ABC, abstractmethod
from typing import Any

from ai_gateway.types import EmbeddingResponse, ProviderResponse


class BaseProvider(ABC):
    name: str

    @abstractmethod
    def complete(self, prompt: str, system: str = "", model: str | None = None, **kwargs: Any) -> ProviderResponse:
        raise NotImplementedError

    @abstractmethod
    def embed(self, text: str, model: str | None = None, **kwargs: Any) -> EmbeddingResponse:
        raise NotImplementedError

    def available(self) -> bool:
        return True


def validate_json_response(text: str) -> tuple[bool, dict | None]:
    text = text.strip()
    if text.startswith("```"):
        lines = text.split("\n")
        text = "\n".join(lines[1:-1] if lines[-1].strip() == "```" else lines[1:])
    try:
        return True, json.loads(text)
    except json.JSONDecodeError:
        return False, None


def timed_call(fn, *args, **kwargs):
    start = time.perf_counter()
    result = fn(*args, **kwargs)
    latency_ms = (time.perf_counter() - start) * 1000
    return result, latency_ms
