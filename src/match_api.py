"""Match API Lambda entry — delegates to FastAPI + Mangum.

Kept for terraform handler compatibility (`match_api.lambda_handler`).
"""

from api.handler import lambda_handler

__all__ = ["lambda_handler"]
