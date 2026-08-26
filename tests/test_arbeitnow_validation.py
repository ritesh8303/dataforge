from processing.typing_inspection.arbeitnow import validate_api_response


def test_arbeitnow_coerces_dict_job_types():
    raw = {
        "data": [
            {
                "slug": "job-1",
                "company_name": "Acme",
                "title": "Data Engineer",
                "description": "Build pipelines",
                "remote": True,
                "url": "https://example.com/1",
                "tags": {"0": "python", "1": "aws"},
                "job_types": {"1": "manager"},
                "location": "Berlin",
                "created_at": 1710000000,
            }
        ],
        "links": {},
        "meta": {},
    }
    validated = validate_api_response(raw)
    assert validated["data"][0]["job_types"] == ["manager"]
    assert validated["data"][0]["tags"] == ["python", "aws"]
