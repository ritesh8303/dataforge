from processing.fetchers import _is_tech_job, _parse_title_company


def test_parse_title_company_from_double_slash():
    title, company = _parse_title_company(
        "Senior Backend Developer – Go & Kubernetes // DATATRONiQ",
        "https://berlinstartupjobs.com/engineering/senior-backend-developer-golang-m-f-d-datatroniq/",
    )
    assert "Backend" in title
    assert company == "DATATRONiQ"


def test_is_tech_job_title_match():
    assert _is_tech_job("Senior Data Engineer", "") is True
    assert _is_tech_job("Head of Marketing", "analytics team") is True  # secondary desc match
    assert _is_tech_job("Head of Marketing", "brand campaigns") is False
    assert _is_tech_job("Growth Lead", "machine learning focus") is True
