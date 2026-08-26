from processing.fetchers import BAFetcher


def test_ba_normalize_v6_job():
    raw = {
        "stellenangebotsTitel": "Data Engineer",
        "firma": "Example GmbH",
        "referenznummer": "11949-17331093-S",
        "homeofficemoeglich": True,
        "externeURL": "https://example.com/apply",
        "aenderungsdatum": "2026-08-25T17:04:04.723",
        "eintrittszeitraum": {"von": "2026-08-26"},
        "stellenlokationen": [
            {"adresse": {"plz": "10115", "ort": "Berlin", "region": "BERLIN", "land": "DEUTSCHLAND"}}
        ],
    }
    job = BAFetcher._normalize_job(raw)
    assert job["refnr"] == "11949-17331093-S"
    assert job["titel"] == "Data Engineer"
    assert job["arbeitgeber"] == "Example GmbH"
    assert job["arbeitsort"]["ort"] == "Berlin"
    assert job["homeoffice"] is True
    assert job["externe_url"].startswith("http")
