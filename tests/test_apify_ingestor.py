from src.ingest_apify import (
    RECOMMENDED_INDEED_ACTOR,
    normalize_job_item,
    select_indeed_tasks,
)


def test_normalize_standard_indeed_format():
    item = {
        "id": "12345",
        "title": "Software Engineer",
        "companyName": "Acme Corp",
        "location": "Berlin, Germany",
        "url": "https://indeed.com/viewjob?jk=12345",
        "description": "Looking for a Software Engineer",
        "isRemote": True,
        "jobType": "full_time",
    }

    normalized = normalize_job_item(item, "indeed")
    assert normalized is not None
    assert normalized["job_id"] == "apify_12345"
    assert normalized["title"] == "Software Engineer"
    assert normalized["company"] == "Acme Corp"
    assert normalized["location"] == "Berlin, Germany"
    assert normalized["url"] == "https://indeed.com/viewjob?jk=12345"
    assert normalized["remote"] is True
    assert normalized["job_types"] == "full_time"
    assert normalized["source"] == "indeed"


def test_normalize_valig_indeed_format():
    """valig/indeed-jobs-scraper output shape (nested location + employer)."""
    item = {
        "jobKey": "abc123def456",
        "title": "Data Engineer",
        "employer": {"name": "Tech GmbH"},
        "location": {
            "city": "Berlin",
            "countryName": "Germany",
            "countryCode": "DE",
        },
        "jobUrl": "https://de.indeed.com/viewjob?jk=abc123def456",
        "descriptionText": "Build data pipelines in Python and SQL.",
        "isRemote": False,
        "jobType": "Full-time",
        "skills": ["Python", "SQL", "Spark"],
        "datePublished": "2026-06-05T00:00:00.000Z",
    }

    normalized = normalize_job_item(item, "indeed")
    assert normalized is not None
    assert normalized["job_id"] == "apify_abc123def456"
    assert normalized["title"] == "Data Engineer"
    assert normalized["company"] == "Tech GmbH"
    assert normalized["location"] == "Berlin, Germany"
    assert normalized["url"] == "https://de.indeed.com/viewjob?jk=abc123def456"
    assert normalized["description"] == "Build data pipelines in Python and SQL."
    assert normalized["tags"] == "Python,SQL,Spark"
    assert normalized["published_at"] == "2026-06-05T00:00:00.000Z"
    assert normalized["source"] == "indeed"


def test_select_indeed_tasks():
    tasks = {
        "indeed": "task_main",
        "indeed_munich": "task_munich",
        "linkedin": "task_li",
        "eures": "task_eu",
    }
    selected = select_indeed_tasks(tasks)
    assert selected == {"indeed": "task_main", "indeed_munich": "task_munich"}


def test_recommended_actor_is_valig():
    assert RECOMMENDED_INDEED_ACTOR == "valig/indeed-jobs-scraper"
