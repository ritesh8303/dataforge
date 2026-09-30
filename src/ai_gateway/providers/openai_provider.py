"""OpenAI provider adapter."""

from __future__ import annotations

import time

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

    def _post_with_retries(self, path: str, body: dict, *, timeout: int = 30, max_retries: int = 5) -> dict:
        url = f"{self.base_url}{path}"
        headers = {"Authorization": f"Bearer {self.api_key}", "Content-Type": "application/json"}
        last_exc: Exception | None = None
        for attempt in range(max_retries):
            try:
                res = requests.post(url, headers=headers, json=body, timeout=timeout)
                if res.status_code == 429:
                    err_body = (res.text or "")[:500]
                    # Daily RPD exhaustion — Retry-After is misleading; fail fast.
                    if "requests per day" in err_body.lower() or "RPD" in err_body:
                        reset = res.headers.get("x-ratelimit-reset-requests", "unknown")
                        raise requests.HTTPError(
                            f"429 daily request limit (RPD) exhausted; resets in {reset}. {err_body[:200]}",
                            response=res,
                        )
                    retry_after = res.headers.get("Retry-After")
                    if retry_after and str(retry_after).replace(".", "", 1).isdigit():
                        wait = min(float(retry_after), 60)
                    else:
                        wait = min(5 * (attempt + 1), 45)
                    print(f"OpenAI 429 — sleeping {wait:.0f}s (attempt {attempt + 1}/{max_retries})", flush=True)
                    time.sleep(wait)
                    last_exc = requests.HTTPError(
                        f"429 Too Many Requests (attempt {attempt + 1})",
                        response=res,
                    )
                    continue
                res.raise_for_status()
                return res.json()
            except requests.HTTPError as exc:
                # Re-raise hard daily-limit errors immediately
                if exc.response is not None and "requests per day" in (exc.response.text or "").lower():
                    raise
                last_exc = exc
                if exc.response is not None and exc.response.status_code == 429:
                    time.sleep(min(5 * (attempt + 1), 45))
                    continue
                raise
            except requests.RequestException as exc:
                last_exc = exc
                time.sleep(min(2**attempt, 15))
        if last_exc:
            raise last_exc
        raise RuntimeError("OpenAI request failed with no exception")

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
            data = self._post_with_retries("/chat/completions", body, timeout=kwargs.get("timeout", 30))
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
            data = self._post_with_retries(
                "/embeddings",
                {"model": model, "input": text[:8000]},
                timeout=kwargs.get("timeout", 20),
            )
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
