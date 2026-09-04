"""AWS Bedrock provider adapter (eu-central-1)."""

from __future__ import annotations

import json

import boto3

from ai_gateway.config import get_env
from ai_gateway.providers.base import BaseProvider, timed_call
from ai_gateway.types import EmbeddingResponse, ProviderResponse


class BedrockProvider(BaseProvider):
    name = "bedrock"

    def __init__(self, region: str | None = None):
        self.region = region or get_env("AWS_BEDROCK_REGION", "eu-central-1")
        self._client = None

    @property
    def client(self):
        if self._client is None:
            self._client = boto3.client("bedrock-runtime", region_name=self.region)
        return self._client

    def available(self) -> bool:
        try:
            return bool(self.region)
        except Exception:
            return False

    def complete(self, prompt: str, system: str = "", model: str | None = None, **kwargs) -> ProviderResponse:
        model = model or get_env("BEDROCK_COMPLETION_MODEL", "amazon.titan-text-express-v1")
        if model.startswith("anthropic."):
            body = {
                "anthropic_version": "bedrock-2023-05-31",
                "max_tokens": kwargs.get("max_tokens", 1024),
                "messages": [{"role": "user", "content": prompt}],
            }
            if system:
                body["system"] = system
        else:
            full = f"{system}\n\n{prompt}" if system else prompt
            body = {
                "inputText": full[:4000],
                "textGenerationConfig": {
                    "maxTokenCount": kwargs.get("max_tokens", 512),
                    "temperature": kwargs.get("temperature", 0.1),
                },
            }

        def _call():
            res = self.client.invoke_model(
                modelId=model,
                body=json.dumps(body),
                contentType="application/json",
                accept="application/json",
            )
            payload = json.loads(res["body"].read())
            if "results" in payload:
                text = payload["results"][0]["outputText"]
            elif "content" in payload:
                text = payload["content"][0]["text"]
            else:
                text = json.dumps(payload)
            est_in = len(prompt.split())
            est_out = len(text.split())
            return text, est_in, est_out

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
        model = model or get_env("BEDROCK_EMBEDDING_MODEL", "amazon.titan-embed-text-v2:0")

        def _call():
            res = self.client.invoke_model(
                modelId=model,
                body=json.dumps({"inputText": text[:8000]}),
                contentType="application/json",
                accept="application/json",
            )
            payload = json.loads(res["body"].read())
            vec = payload.get("embedding") or []
            return vec, len(text.split())

        (vector, in_tok), latency_ms = timed_call(_call)
        return EmbeddingResponse(
            vector=vector,
            model_id=model,
            provider=self.name,
            input_tokens=in_tok,
            latency_ms=latency_ms,
        )
