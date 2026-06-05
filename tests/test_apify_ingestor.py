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


def test_normalize_eures_basic_format():
    item = {
        "id": "eures_777",
        "jobTitle": "Data Analyst",
        "employerName": "EuroData GmbH",
        "country": "Austria",
        "urlToJob": "https://ec.europa.eu/eures/portal/jv-se/job/777",
        "descriptionText": "Analyzing European labor data",
        "remote": "true",
        "employmentType": "permanent"
    }

    normalized = normalize_job_item(item, "eures")
    assert normalized is not None
    assert normalized["job_id"] == "apify_eures_777"
    assert normalized["title"] == "Data Analyst"
    assert normalized["company"] == "EuroData GmbH"
    assert normalized["location"] == "Austria"
    assert normalized["url"] == "https://ec.europa.eu/eures/portal/jv-se/job/777"
    assert normalized["remote"] is True
    assert normalized["job_types"] == "permanent"
    assert normalized["source"] == "eures"


def test_normalize_eures_alternative_company_and_location_formats():
    # Test dictionary / list variants that occur in scraper outputs
    item_dict_employer = {
        "id": "eures_888",
        "positionName": "Data Scientist",
        "employer": {"name": "ScienceLab"},
        "countryName": "Germany",
        "applyUrl": "https://science.de/apply",
        "descriptionHtml": "<p>Scientific computing</p>",
        "isRemote": "yes"
    }

    normalized = normalize_job_item(item_dict_employer, "eures")
    assert normalized is not None
    assert normalized["company"] == "ScienceLab"
    assert normalized["location"] == "Germany"
    assert normalized["url"] == "https://science.de/apply"
    assert normalized["remote"] is True

    # Test string employer fallback
    item_str_employer = {
        "id": "eures_889",
        "title": "Data Scientist",
        "employer": "ScienceLab Str",
        "location": {"city": "Paris", "countryName": "France"},
        "link": "https://science.fr/apply",
        "description": "Scientific computing"
    }
    normalized_str = normalize_job_item(item_str_employer, "eures")
    assert normalized_str is not None
    assert normalized_str["company"] == "ScienceLab Str"
    assert normalized_str["location"] == "Paris"

    # Test list locations format fallback
    item_list_locations = {
        "id": "eures_890",
        "title": "Machine Learning Eng",
        "company": "ML Labs",
        "country": [{"city": "Dublin", "countryName": "Ireland"}],
        "url": "https://mllabs.ie/apply",
        "description": "ML stuff"
    }
    normalized_list = normalize_job_item(item_list_locations, "eures")
    assert normalized_list is not None
    assert normalized_list["location"] == "Dublin"


def test_normalize_eures_non_eu_filtering():
    # Test that jobs in non-EU locations are filtered out
    item_non_eu = {
        "id": "eures_999",
        "title": "Cloud Architect",
        "company": "Global Tech",
        "country": "United States",
        "urlToJob": "https://us-jobs.com/999",
        "description": "Building cloud architecture in California"
    }

    normalized = normalize_job_item(item_non_eu, "eures")
    assert normalized is None
