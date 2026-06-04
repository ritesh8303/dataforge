import json

from src import ingest_company_careers as careers


def test_normalize_target_detects_supported_career_urls():
    assert careers._normalize_target(
        {
            "company": "Example",
            "careers_url": "https://jobs.eu.lever.co/example",
        }
    ) == {
        "slug": "example",
        "base_url": "https://api.eu.lever.co",
        "company": "Example",
        "careers_url": "https://jobs.eu.lever.co/example",
        "ats": "lever",
    }

    workday = careers._normalize_target(
        {
            "company": "Example Workday",
            "careers_url": "https://example.wd3.myworkdayjobs.com/en-US/External",
        }
    )
    assert workday["ats"] == "workday"
    assert workday["host"] == "https://example.wd3.myworkdayjobs.com"
    assert workday["tenant"] == "example"
    assert workday["site"] == "External"

    myworkdaysite = careers._normalize_target(
        {
            "company": "Example Workday Site",
            "careers_url": "https://wd1.myworkdaysite.com/en-US/recruiting/example/External",
        }
    )
    assert myworkdaysite["tenant"] == "example"
    assert myworkdaysite["site"] == "External"


def test_fetch_greenhouse_normalizes_public_board_response(monkeypatch):
    def fake_json(url, **kwargs):
        assert url == "https://boards-api.greenhouse.io/v1/boards/acme/jobs"
        assert kwargs["params"] == {"content": "true"}
        return {
            "jobs": [
                {
                    "id": 123,
                    "title": "Data Engineer",
                    "location": {"name": "Remote - Germany"},
                    "absolute_url": "https://boards.greenhouse.io/acme/jobs/123",
                    "content": "<p>Build pipelines</p>",
                    "departments": [{"name": "Data"}],
                    "updated_at": "2026-05-28T10:00:00Z",
                }
            ]
        }

    monkeypatch.setattr(careers, "_request_json", fake_json)

    jobs = careers.fetch_greenhouse({"ats": "greenhouse", "slug": "acme", "company": "Acme"})

    assert jobs[0]["job_id"] == "direct_greenhouse_acme_123"
    assert jobs[0]["source"] == "direct"
    assert jobs[0]["ats"] == "greenhouse"
    assert jobs[0]["remote"] is True
    assert jobs[0]["description"] == "Build pipelines"
    assert jobs[0]["department"] == "Data"


def test_fetch_lever_paginates_and_preserves_region_base_url(monkeypatch):
    calls = []

    def fake_json(url, **kwargs):
        calls.append((url, kwargs["params"]["skip"]))
        if kwargs["params"]["skip"] == 0:
            return [
                {
                    "id": "abc",
                    "text": "Analytics Engineer",
                    "hostedUrl": "https://jobs.eu.lever.co/acme/abc",
                    "descriptionPlain": "SQL and dbt",
                    "createdAt": 1779970000000,
                    "workplaceType": "hybrid",
                    "categories": {
                        "location": "Berlin",
                        "commitment": "Full-time",
                        "department": "Data",
                    },
                }
            ]
        return []

    monkeypatch.setattr(careers, "_request_json", fake_json)

    jobs = careers.fetch_lever(
        {
            "ats": "lever",
            "slug": "acme",
            "company": "Acme",
            "base_url": "https://api.eu.lever.co",
        }
    )

    assert calls == [
        ("https://api.eu.lever.co/v0/postings/acme", 0),
    ]
    assert jobs[0]["job_id"] == "direct_lever_acme_abc"
    assert jobs[0]["job_types"] == "Full-time"
    assert jobs[0]["remote"] is True


def test_fetch_workday_uses_cxs_search_and_detail(monkeypatch):
    seen = []

    def fake_json(url, **kwargs):
        seen.append((url, kwargs.get("method", "GET")))
        if url.endswith("/jobs"):
            return {
                "total": 1,
                "jobPostings": [
                    {
                        "title": "ML Engineer",
                        "externalPath": "/job/Germany-Berlin/JR-1",
                        "locationsText": "Berlin",
                        "postedOn": "Posted Today",
                        "remoteType": "Remote",
                        "bulletFields": ["JR-1"],
                    }
                ],
            }
        return {
            "jobPostingInfo": {
                "id": "wd-id",
                "title": "ML Engineer",
                "jobReqId": "JR-1",
                "jobDescription": "<p>Train models</p>",
                "location": "Germany, Berlin",
                "timeType": "Full Time",
                "startDate": "2026-05-28",
                "remoteType": "Remote",
                "externalUrl": "https://example.wd3.myworkdayjobs.com/External/job/Germany-Berlin/JR-1",
            }
        }

    monkeypatch.setattr(careers, "_request_json", fake_json)

    jobs = careers.fetch_workday(
        {
            "ats": "workday",
            "slug": "example",
            "tenant": "example",
            "site": "External",
            "host": "https://example.wd3.myworkdayjobs.com",
            "company": "Example",
        }
    )

    assert seen == [
        ("https://example.wd3.myworkdayjobs.com/wday/cxs/example/External/jobs", "POST"),
        ("https://example.wd3.myworkdayjobs.com/wday/cxs/example/External/job/Germany-Berlin/JR-1", "GET"),
    ]
    assert jobs[0]["job_id"] == "direct_workday_example_jr-1"
    assert jobs[0]["remote"] is True
    assert jobs[0]["description"] == "Train models"


def test_load_company_targets_can_replace_defaults(monkeypatch):
    inline = {
        "companies": [
            {
                "company": "Only Acme",
                "careers_url": "https://jobs.ashbyhq.com/acme",
            }
        ]
    }
    monkeypatch.setenv("COMPANY_CAREERS_CONFIG", json.dumps(inline))
    monkeypatch.setenv("COMPANY_CAREERS_CONFIG_MODE", "replace")

    targets = careers.load_company_targets()

    assert len(targets) == 1
    assert targets[0]["ats"] == "ashby"
    assert targets[0]["slug"] == "acme"


def test_load_company_targets_from_url(monkeypatch):
    class FakeResponse:
        def __init__(self, data):
            self.data = data
            self.status_code = 200

        def raise_for_status(self):
            pass

        def json(self):
            return self.data

    fake_payload = {
        "companies": [
            {
                "company": "URL Acme",
                "careers_url": "https://jobs.ashbyhq.com/url-acme",
            }
        ]
    }

    def fake_get(url, **kwargs):
        assert url == "https://example.com/configs.json"
        return FakeResponse(fake_payload)

    monkeypatch.setattr(careers.requests, "get", fake_get)
    monkeypatch.setenv("COMPANY_CAREERS_CONFIG_URL", "https://example.com/configs.json")
    monkeypatch.setenv("COMPANY_CAREERS_CONFIG_MODE", "replace")
    monkeypatch.setenv("COMPANY_CAREERS_USE_DEFAULTS", "false")

    targets = careers.load_company_targets()

    assert len(targets) == 1
    assert targets[0]["ats"] == "ashby"
    assert targets[0]["slug"] == "url-acme"
    assert targets[0]["company"] == "URL Acme"


def test_is_europe_job():
    # Valid Europe locations
    assert careers._is_europe_job({"title": "ML Engineer", "location": "Berlin, Germany"}) is True
    assert careers._is_europe_job({"title": "ML Engineer", "location": "Munich, DE"}) is True
    assert careers._is_europe_job({"title": "Data Scientist", "location": "Paris, France"}) is True
    assert careers._is_europe_job({"title": "Software Engineer, Spain", "location": "Hybrid"}) is True
    assert careers._is_europe_job({"title": "Data Engineer", "location": "Amsterdam"}) is True
    assert careers._is_europe_job({"title": "ML Engineer", "location": "London, UK"}) is True
    assert careers._is_europe_job({"title": "Data Engineer, Zurich", "location": "Remote"}) is False
    assert careers._is_europe_job({"title": "Data Engineer, Switzerland", "location": "Remote"}) is True

    # Invalid non-Europe locations
    assert careers._is_europe_job({"title": "ML Engineer", "location": "Hawthorne, CA"}) is False
    assert careers._is_europe_job({"title": "Software Engineer", "location": "San Francisco, CA"}) is False
    assert careers._is_europe_job({"title": "Sales Rep", "location": "Sydney, Australia"}) is False
