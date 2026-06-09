from processing.company_normalize import normalize_company


def test_normalize_rejects_placeholders():
    assert normalize_company("non renseigné") is None
    assert normalize_company("Unknown Employer") is None
    assert normalize_company("Startup") is None
    assert normalize_company("n/a") is None
    assert normalize_company("") is None


def test_normalize_keeps_valid():
    assert normalize_company("Databricks") == "Databricks"
    assert normalize_company("  Stripe  ") == "Stripe"
