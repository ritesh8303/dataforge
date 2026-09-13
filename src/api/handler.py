"""Mangum Lambda entrypoint for the FastAPI Match app."""

from __future__ import annotations

from mangum import Mangum

from api.app import app

# Function URL / API Gateway HTTP API proxy
handler = Mangum(app, lifespan="off")


def lambda_handler(event, context):
    """AWS Lambda entry — keeps terraform handler name stable."""
    return handler(event, context)
