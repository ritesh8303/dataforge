from src.ingest_apify import normalize_job_item


def test_normalize_standard_linkedin_indeed_format():
    item = {
        "id": "12345",
        "title": "Software Engineer",
        "companyName": "Acme Corp",
        "location": "Berlin, Germany",
        "url": "https://linkedin.com/jobs/view/12345",
        "description": "Looking for a Software Engineer",
        "isRemote": True,
        "jobType": "full_time"
    }
    
    normalized = normalize_job_item(item, "linkedin")
    assert normalized is not None
    assert normalized["job_id"] == "apify_12345"
    assert normalized["title"] == "Software Engineer"
    assert normalized["company"] == "Acme Corp"
    assert normalized["location"] == "Berlin, Germany"
    assert normalized["url"] == "https://linkedin.com/jobs/view/12345"
    assert normalized["remote"] is True
    assert normalized["job_types"] == "full_time"
    assert normalized["source"] == "linkedin"


