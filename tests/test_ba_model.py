import sys
from pathlib import Path

# Ensuring the 'src' directory is in the path
SRC_DIR = Path(__file__).resolve().parents[1] / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from processing.typing_inspection.ba_api import validate_ba_response

def test_ba_model_validation():
    print("--- Starting BA Model Validation Test ---")
    
    # Mock snippet based on BA API documentation structure
    mock_raw_data = {
        "stellenangebote": [
            {
                "refnr": "10000-1192837465-S",
                "titel": "  Softwareentwickler (m/w/d)  ",
                "arbeitgeber": "Example GmbH",
                "eintrittsdatum": "2024-01-01",
                "modifikationsdatum": "2023-11-20T10:00:00",
                "arbeitsort": {
                    "zip_code": "10115",
                    "city": "Berlin",
                    "state": "Berlin"
                }
            }
        ],
        "max_results": 100,
        "current_page": 1
    }
    
    validated = validate_ba_response(mock_raw_data)
    
    assert validated["stellenangebote"][0]["refnr"] == "10000-1192837465-S"
    assert validated["stellenangebote"][0]["titel"] == "Softwareentwickler (m/w/d)" # Verify strip_whitespace
    print("✅ BA Model validation successful!")

if __name__ == "__main__":
    test_ba_model_validation()