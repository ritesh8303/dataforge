"""Anthropic provider adapter."""

from __future__ import annotations

import requests

from ai_gateway.config import get_env
from ai_gateway.providers.base import BaseProvider, timed_call
from ai_gateway.types import EmbeddingResponse, ProviderResponse
from ai_gateway.providers.local import LocalProvider


class AnthropicProvider(BaseProvider):
    name = "anthropic"

    def __init__(self, api_key: str | None = None):
        self.api_key = api_key or get_env("ANTHROPIC_API_KEY")
        self._local = LocalProvider()

    def available(self) -> bool:
        return bool(self.api_key)

    def complete(self, prompt: str, system: str = "", model: str | None = None, **kwargs) -> ProviderResponse:
        model = model or get_env("ANTHROPIC_MODEL", "claude-3-haiku-20240307")
        body = {
            "model": model,
            "max_tokens": kwargs.get("max_tokens", 1024),
            "temperature": kwargs.get("temperature", 0.1),
            "messages": [{"role": "user", "content": prompt}],
        }
        if system:
            body["system"] = system

        def _call():
            res = requests.post(
                "https://api.anthropic.com/v1/messages",
                headers={
                    "x-api-key": self.api_key,
                    "anthropic-version": "2023-06-01",
                    "Content-Type": "application/json",
                },
                json=body,
                timeout=kwargs.get("timeout", 30),
            )
            res.raise_for_status()
            data = res.json()
            text = data["content"][0]["text"]
            usage = data.get("usage", {})
            return text, usage.get("input_tokens", 0), usage.get("output_tokens", 0)

        (text, in_tok, out_tok), latency_ms = timed_call(_call)
        return ProviderResponse(
            text=text,
            model_id=model,
            provider=self.name,
            input_tokens=in_tok,
            output_tokens=out_tok,
            latency_ms=latency_ms,
        )

    def embed(self, text: str, model: str | None = None, **kwargs) -> EmbeddingResponse:
        # Anthropic has no public embeddings API — delegate to local for routing tests
        return self._local.embed(text, model=model or "local-tfidf")
