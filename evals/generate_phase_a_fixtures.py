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
    # --- data_engineering (8) ---
    ("Data Engineer Python AWS Spark", "data_engineering", "Berlin"),
    ("Data platform Airflow dbt", "data_engineering", "Remote / EU"),
    ("Kafka streaming data engineer", "data_engineering", "Berlin"),
    ("Entry-level Data Engineer no experience", "data_engineering", "Cologne"),
    ("Analytics Engineer dbt warehouse", "data_engineering", "Remote"),
    ("Senior Data Platform Engineer Snowflake", "data_engineering", "Munich"),
    ("Datenbankentwickler SQL ETL Pipeline", "data_engineering", "Hamburg"),
    ("Remote Data Engineer EU timezone", "data_engineering", "Remote"),
    # --- ai_ml_data_science (10) ---
    ("Machine Learning Engineer LLM RAG PyTorch", "ai_ml_data_science", "Remote"),
    ("Working Student Data Science NLP", "ai_ml_data_science", "Berlin"),
    ("Junior MLOps engineer", "ai_ml_data_science", "Berlin"),
    ("English speaking Data Scientist", "ai_ml_data_science", "Berlin"),
    ("Computer vision engineer", "ai_ml_data_science", "Munich"),
    ("Senior ML Engineer production models", "ai_ml_data_science", "Frankfurt"),
    ("NLP Engineer Transformer BERT fine-tuning", "ai_ml_data_science", "Hamburg"),
    ("Praktikum Machine Learning TensorFlow", "ai_ml_data_science", "Munich"),
    ("Datenwissenschaftler Statistik Python", "ai_ml_data_science", "Cologne"),
    ("Graduate AI Research Engineer", "ai_ml_data_science", "Stuttgart"),
    # --- data_analytics (7) ---
    ("Junior Data Analyst SQL Power BI", "data_analytics", "Munich"),
    ("Praktikum Data Analytics SQL", "data_analytics", "Berlin"),
    ("Business Analyst Excel dashboards stakeholder", "data_analytics", "Hamburg"),
    ("Datenanalyst Reporting KPI Controlling", "data_analytics", "Frankfurt"),
    ("Senior Analytics Consultant SQL", "data_analytics", "Berlin"),
    ("Werkstudent Business Analytics", "data_analytics", "Munich"),
    ("Remote Data Analyst Tableau", "data_analytics", "Remote"),
    # --- business_intelligence (5) ---
    ("BI Analyst Tableau Looker", "business_intelligence", "Cologne"),
    ("Power BI developer", "business_intelligence", "Munich"),
    ("Looker Studio analyst", "business_intelligence", "Remote"),
    ("Business Intelligence Developer SSRS", "business_intelligence", "Berlin"),
    ("Senior BI Architect semantic layer KPI", "business_intelligence", "Frankfurt"),
    # --- software_engineering (12) ---
    ("Werkstudent Software Engineering React", "software_engineering", "Berlin"),
    ("Internship backend FastAPI Python", "software_engineering", "Remote"),
    ("Senior Software Engineer Java", "software_engineering", "Frankfurt"),
    ("Frontend TypeScript Next.js", "software_engineering", "Hamburg"),
    ("React Native developer", "software_engineering", "Remote"),
    ("Visa sponsorship software developer", "software_engineering", "Munich"),
    ("Full Stack Developer Node.js PostgreSQL", "software_engineering", "Berlin"),
    ("Backend Engineer Go microservices", "software_engineering", "Munich"),
    ("Softwareentwickler Java Spring Boot", "software_engineering", "Stuttgart"),
    ("Junior Python Developer Django REST", "software_engineering", "Cologne"),
    ("Senior Staff Engineer distributed systems", "software_engineering", "Berlin"),
    ("Praktikum Softwareentwicklung C# .NET", "software_engineering", "Hamburg"),
    # --- cloud_devops (8) ---
    ("DevOps Kubernetes Terraform AWS", "cloud_devops", "Hamburg"),
    ("Graduate Cloud Engineer AWS", "cloud_devops", "Munich"),
    ("SRE observability Prometheus", "cloud_devops", "Berlin"),
    ("Terraform platform engineer", "cloud_devops", "Frankfurt"),
    ("Cloud Engineer Azure migration", "cloud_devops", "Cologne"),
    ("Site Reliability Engineer on-call SLO", "cloud_devops", "Munich"),
    ("Werkstudent DevOps Docker CI/CD", "cloud_devops", "Berlin"),
    ("Remote Platform Engineer GitOps", "cloud_devops", "Remote"),
    # --- cybersecurity (6) ---
    ("Cybersecurity pentester AppSec", "cybersecurity", "Frankfurt"),
    ("Security SOC analyst SIEM", "cybersecurity", "Munich"),
    ("Informationssicherheit Analyst", "cybersecurity", "Cologne"),
    ("Senior Security Architect zero trust", "cybersecurity", "Berlin"),
    ("Werkstudent Cyber Security", "cybersecurity", "Hamburg"),
    ("Penetration Tester OSCP certified", "cybersecurity", "Remote"),
    # --- embedded_systems (4) ---
    ("Embedded C firmware automotive", "embedded_systems", "Stuttgart"),
    ("Mikrocontroller Entwickler", "embedded_systems", "Munich"),
    ("RTOS Embedded Linux developer", "embedded_systems", "Berlin"),
    ("Praktikum Embedded Systems IoT", "embedded_systems", "Cologne"),
    # --- qa_testing (5) ---
    ("QA test automation Playwright", "qa_testing", "Berlin"),
    ("Testingenieur Selenium", "qa_testing", "Stuttgart"),
    ("SDET Python pytest", "qa_testing", "Hamburg"),
    ("Senior QA Lead test strategy", "qa_testing", "Munich"),
    ("Werkstudent QA Manual Testing", "qa_testing", "Frankfurt"),
    # --- sap_erp (4) ---
    ("SAP ABAP consultant S/4HANA", "sap_erp", "Walldorf"),
    ("Werkstudent SAP", "sap_erp", "Walldorf"),
    ("Fiori ABAP Entwickler", "sap_erp", "Berlin"),
    ("SAP HANA Datenbankadministrator", "sap_erp", "Stuttgart"),
    # --- product_management (5) ---
    ("Product Manager B2B SaaS", "product_management", "Berlin"),
    ("Junior Product Owner agile", "product_management", "Hamburg"),
    ("Technical product manager AI", "product_management", "Berlin"),
    ("Senior Product Manager fintech", "product_management", "Munich"),
    ("Produktmanager Digitalisierung", "product_management", "Cologne"),
    # --- it_support (5) ---
    ("IT Support helpdesk Active Directory", "it_support", "Munich"),
    ("Desktop support specialist", "it_support", "Berlin"),
    ("Systemadministrator Linux", "it_support", "Frankfurt"),
    ("Werkstudent IT Helpdesk", "it_support", "Hamburg"),
    ("Senior IT Service Desk Lead", "it_support", "Stuttgart"),
    # --- cross-cutting: visa / remote / language / seniority (19) ---
    ("Visa sponsorship data engineer Germany", "data_engineering", "Berlin"),
    ("EU Blue Card machine learning role", "ai_ml_data_science", "Berlin"),
    ("Relocation support backend developer", "software_engineering", "Munich"),
    ("Fully remote software job EU citizen", "software_engineering", "Remote / EU"),
    ("Home office Data Scientist English speaking", "ai_ml_data_science", "Remote"),
    ("Remote Cloud DevOps no office required", "cloud_devops", "Remote"),
    ("International hire frontend visa support", "software_engineering", "Berlin"),
    ("English working language product manager", "product_management", "Berlin"),
    ("Deutsch C1 erforderlich Datenanalyst", "data_analytics", "Munich"),
    ("Mid-level Backend Engineer 3 years Python", "software_engineering", "Berlin"),
    ("Trainee graduate program data analytics", "data_analytics", "Munich"),
    ("Berufseinsteiger DevOps Engineer", "cloud_devops", "Frankfurt"),
    ("Senior Lead Engineer 5+ years microservices", "software_engineering", "Berlin"),
    ("Internship summer AI research", "ai_ml_data_science", "Berlin"),
    ("Working student 20h ML engineering", "ai_ml_data_science", "Berlin"),
    ("Hybrid work BI developer", "business_intelligence", "Cologne"),
    ("Entry level QA no prior experience", "qa_testing", "Berlin"),
    ("Lead SRE incident management", "cloud_devops", "Munich"),
    ("SAP consultant visa sponsorship", "sap_erp", "Walldorf"),
    ("Security engineer valid work permit Germany", "cybersecurity", "Frankfurt"),
    ("Embedded firmware intern automotive", "embedded_systems", "Stuttgart"),
    ("Analytics engineer Spark Scala remote", "data_engineering", "Remote / EU"),
    ("NLP research scientist PhD preferred", "ai_ml_data_science", "Berlin"),
    ("Helpdesk technician Windows Server", "it_support", "Frankfurt"),
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
                "working student",
                "internship",
                "praktikum",
                "graduate",
                "trainee",
                "berufseinsteiger",
                "entry",
                "no experience",
                "no prior",
            )
        )
        senior_q = any(
            k in qlow
            for k in ("senior", "lead", "architect", "staff engineer", "5+ years")
        )
        mid_q = "mid-level" in qlow or "3 years" in qlow
        visa_q = any(
            k in qlow
            for k in (
                "visa",
                "sponsorship",
                "blue card",
                "relocation",
                "international hire",
            )
        )
        permit_q = "work permit" in qlow or "eu citizen" in qlow
        remote_q = any(
            k in qlow
            for k in ("remote", "home office", "fully remote", "no office")
        ) or loc.lower().startswith("remote")
        english_q = any(
            k in qlow for k in ("english speaking", "english working", "english-only")
        )
        german_q = "deutsch c1" in qlow or "fließend deutsch" in qlow

        for j in jobs:
            if j["gold_field"] != field:
                continue
            g = 1
            if entry_q:
                if j["gold_seniority"] in {
                    "internship",
                    "working_student",
                    "trainee_graduate",
                    "junior",
                }:
                    g = 2
            elif senior_q:
                if j["gold_seniority"] == "senior":
                    g = 2
            elif mid_q:
                if j["gold_seniority"] in {"mid", "junior"}:
                    g = 2
            else:
                g = 2

            if visa_q and j["gold_visa_stance"] in {
                "sponsorship_offered",
                "relocation_support",
            }:
                g = max(g, 2)
            if permit_q and j["gold_visa_stance"] in {
                "existing_permit_required",
                "eu_citizens_only",
            }:
                g = max(g, 2)
            if remote_q and j["location"].lower().startswith("remote"):
                g = max(g, 2)
            if english_q and j["gold_english_ok"]:
                g = max(g, 2)
            if german_q and not j["gold_english_ok"]:
                g = max(g, 2)

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
                "query_id": f"q{qi + 1:03d}",
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
    assert len(queries) >= 100, len(queries)

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
