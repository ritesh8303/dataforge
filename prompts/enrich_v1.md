# Job posting enrichment (structured JSON)

Return ONLY valid JSON with these keys:
- skills: list of up to 10 technical skills (strings)
- field: one of ai_ml_data_science, software_engineering, data_analytics, it_support, data_engineering, embedded_systems, cybersecurity, product_management, business_intelligence, qa_testing, cloud_devops, sap_erp, other_tech, non_tech
- seniority: one of internship, working_student, trainee_graduate, junior, mid, senior
- experience_years_min: integer or null
- visa_stance: one of sponsorship_offered, relocation_support, existing_permit_required, eu_citizens_only, not_mentioned
- evidence_visa: short quote from the JD supporting visa_stance, or empty string
- languages_required: list of {lang, cefr} objects
- english_ok: boolean
- work_mode: one of onsite, hybrid, remote
- salary_min: number or null
- salary_max: number or null
- salary_unit: one of eur_hour, eur_month, eur_year, or null
- confidence: float 0.0-1.0
- summary: one sentence role summary (max 30 words)
- remote_confidence: float 0.0-1.0
- language: one of english, german, bilingual, unknown

Do not invent skills or visa claims not supported by the text.
