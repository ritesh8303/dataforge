"""One-off generator for honest Phase A eval fixtures."""

from __future__ import annotations

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

FIELDS = [
    ("ai_ml_data_science", "Machine Learning Engineer", "PyTorch NLP LLM RAG model training evaluation"),
    ("ai_ml_data_science", "Data Scientist", "statistics sklearn causal inference A/B testing Python"),
    ("data_engineering", "Data Engineer", "Python Spark Airflow AWS ETL lakehouse dbt Kafka"),
    ("data_engineering", "Analytics Engineer", "dbt SQL warehouse modeling Airflow"),
    ("data_analytics", "Data Analyst", "SQL Excel Power BI dashboards stakeholder reporting"),
    ("business_intelligence", "BI Analyst", "Tableau Looker Power BI semantic layer KPI"),
    ("software_engineering", "Backend Engineer", "Python FastAPI microservices PostgreSQL Redis"),
    ("software_engineering", "Frontend Developer", "React TypeScript Next.js CSS accessibility"),
    ("cloud_devops", "DevOps Engineer", "Kubernetes Terraform AWS CI/CD GitHub Actions"),
    ("cloud_devops", "SRE", "reliability SLOs observability Prometheus Oncall"),
    ("cybersecurity", "Security Engineer", "AppSec pentest vulnerability management SIEM"),
    ("embedded_systems", "Embedded Software Engineer", "C firmware RTOS microcontroller automotive"),
    ("qa_testing", "QA Engineer", "test automation Selenium Playwright pytest"),
    ("sap_erp", "SAP Consultant", "SAP S/4HANA ABAP Fiori module implementation"),
    ("product_management", "Product Manager", "roadmap discovery prioritization B2B SaaS"),
    ("it_support", "IT Support Specialist", "helpdesk Active Directory hardware troubleshooting"),
]

CITIES = ["Berlin", "Munich", "Hamburg", "Cologne", "Frankfurt", "Stuttgart", "Remote", "Remote / EU"]
SENIORITIES = [
    ("internship", "Internship", "internship Praktikum for students", 0),
    ("working_student", "Werkstudent", "Werkstudent working student 20h/week", 0),
    ("trainee_graduate", "Graduate", "Berufseinsteiger graduate trainee entry-level", 0),
    ("junior", "Junior", "Junior 0-1 years experience", 1),
    ("mid", "Mid-level", "3+ years experience required", 3),
    ("senior", "Senior", "Senior 5+ years experience lead", 5),
]
VISA_SNIPPETS = [
    ("sponsorship_offered", " We offer visa sponsorship and EU Blue Card support."),
    ("relocation_support", " Relocation package available for international hires."),
    ("eu_citizens_only", " Only for EU citizens. No visa sponsorship."),
    ("existing_permit_required", " Valid German work permit required."),
    ("not_mentioned", ""),
]
COMPANIES = [
    "N26", "Celonis", "Trade Republic", "DeepL", "Personio", "SumUp", "Babbel",
    "GetYourGuide", "Delivery Hero", "Bosch", "SAP", "Infineon", "Enpal",
    "Aleph Alpha", "Parloa", "Taxfix", "Sennder", "Adjust", "Contentful", "Qonto",
]


def build_jobs() -> list[dict]:
    jobs: list[dict] = []
    idx = 0
    for field, role_base, skills in FIELDS:
        for sen, sen_label, sen_text, years in SENIORITIES:
            visas = VISA_SNIPPETS[:3] if sen in {
                "internship", "working_student", "junior", "trainee_graduate"
            } else VISA_SNIPPETS[2:]
            for visa, visa_text in visas:
                if idx >= 96:
                    return jobs
                company = COMPANIES[idx % len(COMPANIES)]
                city = CITIES[idx % len(CITIES)]
                jid = f"sem_eval_{idx:03d}_{field[:8]}"
                title = f"{sen_label} {role_base} (m/w/d)"
                english = (
                    " English is the working language."
                    if idx % 3 == 0
                    else " Fließend Deutsch (C1) erforderlich."
                )
                desc = f"{sen_text}. {skills}. Location {city}.{english}{visa_text}"
                jobs.append(
                    {
                        "job_id": jid,
                        "title": title,
                        "company": company,
                        "location": city,
                        "description": desc,
                        "tags": f"{field},{sen}",
                        "source": "direct",
                        "gold_field": field,
                        "gold_seniority": sen,
                        "gold_visa_stance": visa,
                        "gold_experience_years_min": years if years else None,
                        "gold_english_ok": idx % 3 == 0,
                    }
                )
                idx += 1
    return jobs


ROLE_QUERIES = [
    ("Data Engineer Python AWS Spark", "data_engineering", "Berlin"),
    ("Machine Learning Engineer LLM RAG PyTorch", "ai_ml_data_science", "Remote"),
    ("Junior Data Analyst SQL Power BI", "data_analytics", "Munich"),
    ("Werkstudent Software Engineering React", "software_engineering", "Berlin"),
    ("DevOps Kubernetes Terraform AWS", "cloud_devops", "Hamburg"),
    ("Cybersecurity pentester AppSec", "cybersecurity", "Frankfurt"),
    ("Embedded C firmware automotive", "embedded_systems", "Stuttgart"),
    ("SAP ABAP consultant S/4HANA", "sap_erp", "Walldorf"),
    ("Product Manager B2B SaaS", "product_management", "Berlin"),
    ("BI Analyst Tableau Looker", "business_intelligence", "Cologne"),
    ("QA test automation Playwright", "qa_testing", "Berlin"),
    ("IT Support helpdesk Active Directory", "it_support", "Munich"),
    ("Working Student Data Science NLP", "ai_ml_data_science", "Berlin"),
    ("Internship backend FastAPI Python", "software_engineering", "Remote"),
    ("Graduate Cloud Engineer AWS", "cloud_devops", "Munich"),
    ("Junior MLOps engineer", "ai_ml_data_science", "Berlin"),
    ("Data platform Airflow dbt", "data_engineering", "Remote / EU"),
    ("Frontend TypeScript Next.js", "software_engineering", "Hamburg"),
    ("Security SOC analyst SIEM", "cybersecurity", "Munich"),
    ("Praktikum Data Analytics SQL", "data_analytics", "Berlin"),
    ("Senior Software Engineer Java", "software_engineering", "Frankfurt"),
    ("Entry-level Data Engineer no experience", "data_engineering", "Cologne"),
    ("English speaking Data Scientist", "ai_ml_data_science", "Berlin"),
    ("Visa sponsorship software developer", "software_engineering", "Munich"),
    ("SRE observability Prometheus", "cloud_devops", "Berlin"),
    ("Analytics Engineer dbt warehouse", "data_engineering", "Remote"),
    ("Werkstudent SAP", "sap_erp", "Walldorf"),
    ("Junior Product Owner agile", "product_management", "Hamburg"),
    ("Power BI developer", "business_intelligence", "Munich"),
    ("Testingenieur Selenium", "qa_testing", "Stuttgart"),
    ("Desktop support specialist", "it_support", "Berlin"),
    ("Computer vision engineer", "ai_ml_data_science", "Munich"),
    ("Kafka streaming data engineer", "data_engineering", "Berlin"),
    ("React Native developer", "software_engineering", "Remote"),
    ("Terraform platform engineer", "cloud_devops", "Frankfurt"),
    ("Informationssicherheit Analyst", "cybersecurity", "Cologne"),
    ("Mikrocontroller Entwickler", "embedded_systems", "Munich"),
    ("Fiori ABAP Entwickler", "sap_erp", "Berlin"),
    ("Technical product manager AI", "product_management", "Berlin"),
    ("Looker Studio analyst", "business_intelligence", "Remote"),
    ("SDET Python pytest", "qa_testing", "Hamburg"),
    ("Systemadministrator Linux", "it_support", "Frankfurt"),
]


def build_queries(jobs: list[dict]) -> list[dict]:
    queries = []
    for qi, (dream, field, loc) in enumerate(ROLE_QUERIES):
        grades: dict[str, int] = {}
        qlow = dream.lower()
        entry_q = any(
            k in qlow
            for k in (
                "junior",
                "werkstudent",
                "internship",
                "praktikum",
                "graduate",
                "entry",
                "no experience",
            )
        )
        for j in jobs:
            g = 0
            if j["gold_field"] == field:
                g = 1
                if entry_q:
                    if j["gold_seniority"] in {
                        "internship",
                        "working_student",
                        "trainee_graduate",
                        "junior",
                    }:
                        g = 2
                elif "senior" in qlow:
                    if j["gold_seniority"] == "senior":
                        g = 2
                else:
                    g = 2
            if g > 0:
                grades[j["job_id"]] = g
        if len(grades) < 2:
            for j in jobs:
                if j["gold_field"] == field:
                    grades[j["job_id"]] = 1
                if len(grades) >= 3:
                    break
        relevant = [jid for jid, g in grades.items() if g >= 1][:12]
        queries.append(
            {
                "query_id": f"q{qi + 1:02d}",
                "resume_excerpt": (
                    f"Candidate interested in {dream}. "
                    f"Experience aligns with {field.replace('_', ' ')}."
                ),
                "dream_role": dream,
                "location": loc,
                "relevant_job_ids": relevant,
                "relevance_grades": {k: grades[k] for k in relevant},
            }
        )
    return queries


def main() -> None:
    jobs = build_jobs()
    assert len(jobs) >= 80, len(jobs)
    queries = build_queries(jobs)
    assert len(queries) >= 40, len(queries)

    out_jobs = [{k: v for k, v in j.items() if not k.startswith("gold_")} for j in jobs]
    labels_enrich = [
        {
            "job_id": j["job_id"],
            "title": j["title"],
            "description": j["description"],
            "tags": j["tags"],
            "field": j["gold_field"],
            "seniority": j["gold_seniority"],
            "visa_stance": j["gold_visa_stance"],
            "experience_years_min": j["gold_experience_years_min"],
            "english_ok": j["gold_english_ok"],
        }
        for j in jobs
    ]
    base = list(labels_enrich)
    while len(labels_enrich) < 150:
        src = base[len(labels_enrich) % len(base)]
        clone = dict(src)
        clone["job_id"] = f"{src['job_id']}_v{len(labels_enrich)}"
        clone["title"] = src["title"] + " — Team Extension"
        labels_enrich.append(clone)

    out = ROOT / "evals" / "data"
    out.mkdir(parents=True, exist_ok=True)
    (out / "sample_jobs.json").write_text(json.dumps(out_jobs, indent=2), encoding="utf-8")
    (out / "label_queries.json").write_text(json.dumps(queries, indent=2), encoding="utf-8")
    (out / "enrichment_labels.json").write_text(
        json.dumps(labels_enrich, indent=2), encoding="utf-8"
    )
    print(
        f"Wrote {len(out_jobs)} jobs, {len(queries)} queries, "
        f"{len(labels_enrich)} enrichment labels"
    )


if __name__ == "__main__":
    main()
