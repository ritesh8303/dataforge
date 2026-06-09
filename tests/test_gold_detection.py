from gold_generator import detect_is_english, detect_language_requirement, detect_work_style


def test_detect_is_english_title_based():
    row = {"title": "Senior Data Engineer (m/w/d)", "description": "Wir suchen einen erfahrenen Mitarbeiter."}
    assert detect_is_english(row) is True


def test_detect_language_bilingual():
    row = {
        "title": "Data Engineer",
        "description": "Wir bieten spannende Aufgaben und gute Benefits.",
        "is_english": True,
    }
    assert detect_language_requirement(row) == "bilingual"


def test_detect_work_style_hybrid():
    row = {
        "title": "Engineer",
        "description": "Hybrid work model with 2 days in office",
        "remote": False,
    }
    assert detect_work_style(row) == "hybrid"
