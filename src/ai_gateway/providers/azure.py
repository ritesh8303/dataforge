"""Optional Azure OpenAI adapter (off by default; literacy stub for DACH JDs)."""

from __future__ import annotations

from typing import Any

from ai_gateway.config import get_env
from ai_gateway.providers.base import BaseProvider
from ai_gateway.types import EmbeddingResponse, ProviderResponse


class AzureOpenAIProvider(BaseProvider):
    name = "azure"

    def __init__(self) -> None:
        self.endpoint = get_env("AZURE_OPENAI_ENDPOINT")
        self.api_key = get_env("AZURE_OPENAI_API_KEY")
        self.deployment = get_env("AZURE_OPENAI_DEPLOYMENT", "gpt-4o-mini")
        self.embed_deployment = get_env("AZURE_OPENAI_EMBED_DEPLOYMENT", "text-embedding-3-small")

    def available(self) -> bool:
        # Explicit opt-in — never spend unless both endpoint and key are set
        # AND AI_AZURE_ENABLED=true.
        return bool(
            self.endpoint
            and self.api_key
            and get_env("AI_AZURE_ENABLED", "false").lower() in ("1", "true", "yes")
        )

    def complete(self, prompt: str, system: str = "", **kwargs: Any) -> ProviderResponse:
        raise NotImplementedError(
            "Azure OpenAI adapter is stubbed. Set AI_AZURE_ENABLED=true and implement "
            "HTTP calls when free credits are available."
        )

    def embed(self, text: str, **kwargs: Any) -> EmbeddingResponse:
        raise NotImplementedError("Azure embeddings stub — enable only with free credits.")
