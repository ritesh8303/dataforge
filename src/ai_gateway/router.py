from __future__ import annotations

from typing import Any

from ai_gateway.config import TASK_PROFILES, ai_enabled, daily_budget_usd
from ai_gateway.cost_logger import CostLogger
from ai_gateway.providers.anthropic_provider import AnthropicProvider
from ai_gateway.providers.azure import AzureOpenAIProvider
from ai_gateway.providers.bedrock_provider import BedrockProvider
from ai_gateway.providers.local import LocalProvider
from ai_gateway.providers.mistral_provider import MistralProvider
from ai_gateway.providers.openai_provider import OpenAIProvider
from ai_gateway.providers.base import BaseProvider, validate_json_response
from ai_gateway.tracing import TraceLogger
from ai_gateway.types import EmbeddingResponse, ProviderResponse, TaskProfile

# Tasks that must not silently poison labels with LocalProvider heuristics.
STRICT_NO_LOCAL_FALLBACK = frozenset({"enrich", "rerank", "explain"})


class AIDisabledError(RuntimeError):
    """Raised when AI_ENABLED kill switch is off."""


class BudgetExceededError(RuntimeError):
    """Raised when daily AI spend budget is exhausted."""


class ProviderExhaustedError(RuntimeError):
    """Raised when all preferred providers fail and local fallback is disallowed."""


class ModelRouter:
    """Task-aware multi-provider router with fallback cascade and cost logging."""

    def __init__(
        self,
        cost_logger: CostLogger | None = None,
        trace_logger: TraceLogger | None = None,
    ):
        self.cost_logger = cost_logger or CostLogger()
        self.trace_logger = trace_logger if trace_logger is not None else TraceLogger()
        self._providers: dict[str, BaseProvider] = {
            "local": LocalProvider(),
            "openai": OpenAIProvider(),
            "mistral": MistralProvider(),
            "anthropic": AnthropicProvider(),
            "bedrock": BedrockProvider(),
            "azure": AzureOpenAIProvider(),
        }

    def get_profile(self, task: str) -> TaskProfile:
        cfg = TASK_PROFILES.get(task, TASK_PROFILES["summarize"])
        return TaskProfile(name=task, **cfg)

    def _guard(self) -> None:
        if not ai_enabled():
            raise AIDisabledError("AI_ENABLED=false — kill switch active")
        spent = float(self.cost_logger.summary().get("total_cost_usd", 0.0) or 0.0)
        budget = daily_budget_usd()
        if spent >= budget:
            raise BudgetExceededError(f"AI daily budget exceeded: ${spent:.4f} >= ${budget:.4f}")

    def _ordered_providers(self, profile: TaskProfile) -> list[BaseProvider]:
        ordered: list[BaseProvider] = []
        for name in profile.preferred_providers:
            p = self._providers.get(name)
            if p and p.available():
                ordered.append(p)
        if not ordered:
            ordered.append(self._providers["local"])
        return ordered

    def _allow_local_fallback(self, task: str, explicit: bool | None) -> bool:
        if explicit is not None:
            return explicit
        # Production classification / agent polish must not invent mid/empty labels.
        return task not in STRICT_NO_LOCAL_FALLBACK

    def complete(
        self,
        task: str,
        prompt: str,
        system: str = "",
        *,
        allow_local_fallback: bool | None = None,
        **kwargs: Any,
    ) -> ProviderResponse:
        self._guard()
        profile = self.get_profile(task)
        kwargs.setdefault("json_mode", profile.require_json)
        kwargs.setdefault("task", task)
        allow_local = self._allow_local_fallback(task, allow_local_fallback)
        last_error = ""
        with self.trace_logger.span(
            f"complete:{task}",
            task=task,
            allow_local_fallback=allow_local,
            prompt_version=kwargs.get("prompt_version", ""),
        ) as span:
            for provider in self._ordered_providers(profile):
                if provider.name == "local" and not allow_local:
                    continue
                try:
                    resp = provider.complete(prompt, system=system, **kwargs)
                    if profile.require_json:
                        ok, _ = validate_json_response(resp.text)
                        if not ok:
                            raise ValueError(f"Invalid JSON from {provider.name}")
                    if resp.latency_ms > profile.max_latency_ms:
                        print(f"Warning: {provider.name} exceeded latency budget ({resp.latency_ms}ms)")
                    record = self.cost_logger.log(
                        task=task,
                        provider=resp.provider,
                        model_id=resp.model_id,
                        input_tokens=resp.input_tokens,
                        output_tokens=resp.output_tokens,
                        latency_ms=resp.latency_ms,
                        success=True,
                    )
                    span.metadata["provider"] = resp.provider
                    span.metadata["model_id"] = resp.model_id
                    span.metadata["cost_usd"] = record.cost_usd
                    return resp
                except Exception as e:
                    last_error = str(e)
                    self.cost_logger.log(
                        task=task,
                        provider=provider.name,
                        model_id=kwargs.get("model", "unknown"),
                        input_tokens=0,
                        output_tokens=0,
                        latency_ms=0,
                        success=False,
                        error=last_error,
                    )
                    continue

            if not allow_local:
                span.error = last_error or "providers_exhausted"
                raise ProviderExhaustedError(
                    f"No provider succeeded for task={task} (local fallback disabled): {last_error}"
                )

            # Final fallback — local always works (offline / demos / summarize)
            resp = self._providers["local"].complete(prompt, system=system, **kwargs)
            self.cost_logger.log(
                task=task,
                provider=resp.provider,
                model_id=resp.model_id,
                input_tokens=resp.input_tokens,
                output_tokens=resp.output_tokens,
                latency_ms=resp.latency_ms,
                success=True,
                error=f"fallback after: {last_error}",
            )
            span.metadata["provider"] = resp.provider
            span.metadata["fallback"] = True
            return resp

    def embed(self, task: str, text: str, **kwargs: Any) -> EmbeddingResponse:
        self._guard()
        profile = self.get_profile(task if task in TASK_PROFILES else "embed")
        last_error = ""
        with self.trace_logger.span(f"embed:{task or 'embed'}", task=task or "embed") as span:
            for provider in self._ordered_providers(profile):
                try:
                    resp = provider.embed(text, **kwargs)
                    record = self.cost_logger.log(
                        task=task or "embed",
                        provider=resp.provider,
                        model_id=resp.model_id,
                        input_tokens=resp.input_tokens,
                        output_tokens=0,
                        latency_ms=resp.latency_ms,
                        success=True,
                    )
                    span.metadata["provider"] = resp.provider
                    span.metadata["cost_usd"] = record.cost_usd
                    return resp
                except Exception as e:
                    last_error = str(e)
                    self.cost_logger.log(
                        task=task or "embed",
                        provider=provider.name,
                        model_id="unknown",
                        input_tokens=0,
                        output_tokens=0,
                        latency_ms=0,
                        success=False,
                        error=last_error,
                    )
            resp = self._providers["local"].embed(text)
            self.cost_logger.log(
                task=task or "embed",
                provider=resp.provider,
                model_id=resp.model_id,
                input_tokens=resp.input_tokens,
                output_tokens=0,
                latency_ms=resp.latency_ms,
                success=True,
                error=f"fallback after: {last_error}",
            )
            span.metadata["provider"] = resp.provider
            span.metadata["fallback"] = True
            return resp
