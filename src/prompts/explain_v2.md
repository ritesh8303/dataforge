# Match explainer — v2

Explain why a job matches a candidate using ONLY the provided job text.
Return ONLY valid JSON:
{"job_id":"...","reason":"...","evidence":"..."}

Rules:
- job_id MUST equal the provided job_id.
- evidence MUST be a short verbatim substring copied from the job title or description.
- reason is one short sentence (max 40 words).
- Never invent job_ids, skills, or visa claims.
