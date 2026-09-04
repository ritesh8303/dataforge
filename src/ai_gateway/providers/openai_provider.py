"""OpenAI provider adapter."""

from __future__ import annotations

import requests

from ai_gateway.config import get_env
from ai_gateway.providers.base import BaseProvider, timed_call
from ai_gateway.types import EmbeddingResponse, ProviderResponse


class OpenAIProvider(BaseProvider):
    name = "openai"

    def __init__(self, api_key: str | None = None, base_url: str | None = None):
        self.api_key = api_key or get_env("OPENAI_API_KEY")
        self.base_url = (base_url or get_env("OPENAI_BASE_URL", "https://api.openai.com/v1")).rstrip("/")

    def available(self) -> bool:
        return bool(self.api_key)

    def complete(self, prompt: str, system: str = "", model: str | None = None, **kwargs) -> ProviderResponse:
        model = model or get_env("OPENAI_COMPLETION_MODEL", "gpt-4o-mini")
        messages = []
        if system:
            messages.append({"role": "system", "content": system})
        messages.append({"role": "user", "content": prompt})
        body: dict = {"model": model, "messages": messages, "temperature": kwargs.get("temperature", 0.1)}
        if kwargs.get("json_mode"):
            body["response_format"] = {"type": "json_object"}

        def _call():
            res = requests.post(
                f"{self.base_url}/chat/completions",
                headers={"Authorization": f"Bearer {self.api_key}", "Content-Type": "application/json"},
                json=body,
                timeout=kwargs.get("timeout", 30),
            )
            res.raise_for_status()
            data = res.json()
            choice = data["choices"][0]["message"]["content"]
            usage = data.get("usage", {})
            return choice, usage.get("prompt_tokens", 0), usage.get("completion_tokens", 0)

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
        model = model or get_env("OPENAI_EMBEDDING_MODEL", "text-embedding-3-small")

        def _call():
            res = requests.post(
                f"{self.base_url}/embeddings",
                headers={"Authorization": f"Bearer {self.api_key}", "Content-Type": "application/json"},
                json={"model": model, "input": text[:8000]},
                timeout=kwargs.get("timeout", 20),
            )
            res.raise_for_status()
            data = res.json()
            vec = data["data"][0]["embedding"]
            usage = data.get("usage", {})
            return vec, usage.get("prompt_tokens", 0)

        (vector, in_tok), latency_ms = timed_call(_call)
        return EmbeddingResponse(
            vector=vector,
            model_id=model,
            provider=self.name,
            input_tokens=in_tok,
            latency_ms=latency_ms,
        )
