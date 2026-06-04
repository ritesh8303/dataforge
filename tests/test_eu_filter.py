import pytest
from src.processing.eu_filter import is_in_europe


@pytest.mark.parametrize(
    "loc,title,desc,expected",
    [
        # Europe Countries & Cities (Accept)
        ("Berlin, Germany", "Data Scientist", "Python developer in Berlin", True),
        ("Munich", "ML Engineer", "Role based in Munich, Germany", True),
        ("Paris, France", "Software Engineer", "Join our Paris office", True),
        ("Amsterdam", "BI Analyst", "Relocate to Amsterdam", True),
        ("Spain", "Data Engineer", "Work from Spain", True),
        ("Vienna, Austria", "Data Analyst", "Vienna office", True),
        ("Sofia", "QA Engineer", "Sofia, Bulgaria", True),
        ("Warsaw, Poland", "System Architect", "Warsaw tech hub", True),
        ("Dublin, Ireland", "DevOps Engineer", "Dublin-based hybrid role", True),
        ("Stockholm", "Cloud Engineer", "Stockholm team", True),
        # Standalone ISO Codes (Accept)
        ("Berlin (DE)", "Product Manager", "", True),
        ("Location: ES", "UX Designer", "", True),
        ("AT / Vienna", "Front End Developer", "", True),
        # Non-EU but European Countries & Cities (Accept under Europe Filter)
        ("London, UK", "Backend Developer", "UK-based role", True),
        ("London, United Kingdom", "Data Analyst", "London office", True),
        ("Zurich, Switzerland", "Quant Researcher", "Swiss office", True),
        ("Geneva", "Project Manager", "Geneva office", True),
        # Non-European Countries & Cities (Reject)
        ("New York, NY", "Data Scientist", "USA role", False),
        ("San Francisco, CA", "ML Engineer", "Silicon Valley", False),
        ("Toronto, Canada", "Software Dev", "Canada branch", False),
        ("Sydney, Australia", "Analyst", "Australia team", False),
        ("Bangalore, India", "Data Engineer", "India tech hub", False),
        ("Tel Aviv, Israel", "Security Engineer", "", False),
        # General Keywords (Accept)
        ("Remote (Europe)", "Software Engineer", "Must reside in Europe", True),
        ("Remote (EU)", "Data Scientist", "Eligible to work in the EU", True),
        ("EMEA Remote", "Solution Architect", "EMEA region role", True),
        # General Remote without Europe indication (Reject)
        ("Remote", "Staff Engineer", "Join our global team", False),
        ("Remote", "Software Engineer", "Worldwide remote", False),
        # False positives from title abbreviations/US states (Reject)
        ("Indianapolis, IN", "Systems Administrator - Corporate IT", "", False),
        ("San Diego", "HR Compliance & Leave Specialist", "", False),
        ("Lancaster", "Victim Advocate PT", "", False),
        ("Berlin, NJ", "Graphic Designer", "", False),
        # Specific edge cases (Indeed dict-like items)
        (None, None, None, False),
        ("", "", "", False),
    ],
)
def test_is_in_europe(loc, title, desc, expected):
    assert is_in_europe(location_str=loc, title_str=title, description_str=desc) == expected


def test_is_in_europe_with_indeed_dict():
    # Indeed item representation with nested location dict
    item_in_europe = {"location": {"cityName": "Bakersfield", "countryCode": "DE", "countryName": "Germany"}}
    assert is_in_europe(location_str="Bakersfield", item=item_in_europe) is True

    item_outside_europe = {"location": {"cityName": "Bakersfield", "countryCode": "US", "countryName": "United States"}}
    assert is_in_europe(location_str="Bakersfield", item=item_outside_europe) is False
