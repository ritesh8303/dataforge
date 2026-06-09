import re

# European Countries (ISO codes, English/German/Native names)
EUROPE_COUNTRIES = {
    # EU ISO Codes
    "at", "be", "bg", "cy", "cz", "de", "dk", "ee", "es", "fi", "fr", "gr", "hr", "hu", "ie", "it", "lt", "lu", "lv", "mt", "nl", "pl", "pt", "ro", "se", "si", "sk",
    # Non-EU European ISO Codes
    "uk", "gb", "ch", "no", "is", "li", "mc", "ad", "sm", "va", "ua", "tr", "by", "md", "rs", "me", "ba", "al", "mk", "xk",
    
    # English names
    "austria", "belgium", "bulgaria", "croatia", "cyprus", "czechia", "czech republic", "denmark", "estonia", "finland", "france", "germany", "greece", "hungary", "ireland", "italy", "latvia", "lithuania", "luxembourg", "malta", "netherlands", "poland", "portugal", "romania", "slovakia", "slovenia", "spain", "sweden",
    "united kingdom", "great britain", "england", "scotland", "wales", "switzerland", "norway", "iceland", "liechtenstein", "monaco", "andorra", "san marino", "vatican", "ukraine", "turkey", "belarus", "moldova", "serbia", "montenegro", "bosnia", "herzegovina", "albania", "macedonia", "kosovo",
    
    # German names
    "österreich", "belgien", "bulgarien", "kroatien", "zypern", "tschechien", "dänemark", "estland", "finnland", "frankreich", "deutschland", "griechenland", "ungarn", "irland", "italien", "lettland", "litauen", "luxemburg", "niederlande", "polen", "rumänien", "slowakei", "slowenien", "spanien", "schweden",
    "schweiz", "norwegen", "island", "türkei", "weißrussland", "serbien", "bosnien"
}

# Major European cities
EUROPE_CITIES = {
    # Germany
    "berlin", "munich", "münchen", "hamburg", "frankfurt", "cologne", "köln", "stuttgart", "düsseldorf", "dortmund", "essen", "leipzig", "bremen", "dresden", "hannover", "nuremberg", "nürnberg", "duisburg", "bochum", "wuppertal", "bielefeld", "bonn", "munster", "münster", "karlsruhe", "mannheim", "augsburg", "wiesbaden", "gelsenkirchen", "mönchengladbach", "braunschweig", "chemnitz", "aachen", "halle", "magdeburg", "freiburg", "krefeld", "lubeck", "lübeck", "mainz", "erfurt", "rostock", "kassel", "hagen", "hamm", "saarbrucken", "saarbrücken", "mulheim", "mülheim", "herne", "ludwigshafen", "osnabruck", "osnabrück", "solingen", "leverkusen", "oldenburg", "neuss", "potsdam", "heidelberg", "paderborn", "darmstadt", "wurzburg", "würzburg", "regensburg", "ingolstadt", "heilbronn", "ulm", "wolfsburg", "gottingen", "göttingen", "offenbach", "pforzheim", "recklinghausen", "bottrop", "furth", "fürth", "remscheid", "reutlingen", "moers", "koblenz", "siegen", "bergisch gladbach", "jena", "erlangen", "trier", "salzgitter",
    
    # Other European Cities
    "paris", "amsterdam", "madrid", "barcelona", "rome", "roma", "milan", "milano", "vienna", "wien", "brussels", "bruxelles", "brüssel", "warsaw", "warszawa", "budapest", "prague", "praha", "copenhagen", "københavn", "stockholm", "helsinki", "lisbon", "lisboa", "dublin", "athens", "sofia", "bucharest", "bucuresti", "bratislava", "ljubljana", "zagreb", "tallinn", "riga", "vilnius", "valletta", "nicosia", "malaga", "málaga", "valencia", "seville", "sevilla", "lyon", "marseille", "toulouse", "bordeaux", "nice", "porto", "utrecht", "rotterdam", "hague", "den haag", "antwerp", "antwerpen", "ghent", "liege", "liège", "gothenburg", "göteborg", "malmo", "malmö", "uppsala", "krakow", "kraków", "wroclaw", "wrocław", "gdansk", "gdańsk", "poznan", "poznań", "graz", "linz", "salzburg", "innsbruck", "tallin",
    
    # Non-EU European Cities
    "london", "manchester", "birmingham", "leeds", "glasgow", "sheffield", "liverpool", "bristol", "edinburgh", "zurich", "zürich", "geneva", "genf", "basel", "lausanne", "bern", "oslo", "bergen", "trondheim", "stavanger", "reykjavik", "kyiv", "kiev", "kharkiv", "lviv", "odessa", "dnipro", "istanbul", "ankara", "izmir", "belgrade", "sarajevo", "skopje", "tirana", "pristina", "podgorica", "chisinau", "minsk"
}


def _safe_str(val):
    if val is None or str(val) in ("nan", "<NA>"):
        return ""
    return str(val).strip().lower()


def is_in_europe(location_str, title_str="", description_str="", item=None):
    """
    Unified check to determine if a job is located in Europe (EU + non-EU European countries).
    Returns True if the location is in Europe, False otherwise.
    """
    # 1. Check direct nested location dict from Indeed
    if isinstance(item, dict):
        loc_dict = item.get("location")
        if isinstance(loc_dict, dict):
            c_code = str(loc_dict.get("countryCode", "")).lower().strip()
            c_name = str(loc_dict.get("countryName", "")).lower().strip()
            if c_code in EUROPE_COUNTRIES or c_name in EUROPE_COUNTRIES:
                return True

    loc = _safe_str(location_str)
    title = _safe_str(title_str)

    # 2. Blocklist of multi-word non-European phrases (US, Canada, etc.)
    non_europe_phrases = {
        "united states",
        "new york",
        "san francisco",
        "los angeles",
        "south africa",
        "new zealand",
        "mountain view",
        "palo alto",
        "redwood city",
        "menlo park",
        "tel aviv",
        "buenos aires",
        "cape canaveral",
        "ho chi minh",
    }
    if any(phrase in loc for phrase in non_europe_phrases) or any(phrase in title for phrase in non_europe_phrases):
        return False

    # Extract standalone words
    loc_words = set(re.findall(r"\b[a-z]+\b", loc))
    title_words = set(re.findall(r"\b[a-z]+\b", title))
    combined_words = loc_words.union(title_words)

    # 3. Standalone general non-European words
    non_europe_general = {
        "usa",
        "us",
        "america",
        "canada",
        "toronto",
        "vancouver",
        "australia",
        "sydney",
        "melbourne",
        "singapore",
        "sg",
        "japan",
        "tokyo",
        "india",
        "bangalore",
        "bengaluru",
        "china",
        "shanghai",
        "suzhou",
        "wuxi",
        "cn",
        "brazil",
        "mexico",
        "russia",
        "israel",
        "boston",
        "seattle",
        "austin",
        "texas",
        "california",
        "chicago",
        "denver",
        "atlanta",
        "miami",
        "hawthorne",
        "bastrop",
        "redmond",
        "wa",
        "starbase",
        "saddleback",
        "capitol",
        "sunnyvale",
        "woodinville",
        "egypt",
        "maadi",
        "eg",
        "dubai",
        "ae",
        "vietnam",
        "vn",
        "argentina",
        "ar",
        "latam",
    }
    if not non_europe_general.isdisjoint(combined_words):
        return False

    # 4. US State abbreviations
    us_state_codes = {
        "al", "ak", "az", "ar", "co", "ct", "fl", "ga", "hi", "id", "il", "in", "ia", "ks", "ky", "la", "me", "md", "ma", "mi", "mn", "ms", "mo", "ne", "nv", "nh", "nj", "nm", "ny", "nc", "nd", "oh", "ok", "or", "pa", "ri", "sc", "sd", "tn", "tx", "ut", "vt", "va", "wa", "wv", "wi", "wy",
    }
    if not us_state_codes.isdisjoint(loc_words):
        return False

    # 5. Check explicit European country names
    for country in EUROPE_COUNTRIES:
        if len(country) > 2 and (country in loc or country in title):
            return True

    # 6. Check for explicit European country ISO codes in location words only
    two_letter_europe_codes = {
        "at", "be", "bg", "cy", "cz", "de", "dk", "ee", "es", "fi", "fr", "gr", "hr", "hu", "ie", "it", "lt", "lu", "lv", "mt", "nl", "pl", "pt", "ro", "se", "si", "sk",
        "uk", "gb", "ch", "no", "is", "li", "mc", "ad", "sm", "va", "ua", "tr", "by", "md", "rs", "me", "ba", "al", "mk", "xk"
    }
    if not two_letter_europe_codes.isdisjoint(loc_words):
        return True

    # 7. Check for known European cities
    for city in EUROPE_CITIES:
        if city in loc:
            return True

    # 8. Check for general European keywords
    europe_keywords = {"eu", "european", "union", "europe", "emea"}
    if not europe_keywords.isdisjoint(combined_words):
        return True

    return False


# Mapping of European countries (names & codes) to country names
COUNTRY_MAPPING = {
    # Western Europe
    "at": "Austria", "be": "Belgium", "fr": "France", "de": "Germany",
    "li": "Liechtenstein", "lu": "Luxembourg", "mc": "Monaco", "nl": "Netherlands",
    "ch": "Switzerland",
    "austria": "Austria", "belgium": "Belgium", "france": "France",
    "germany": "Germany", "liechtenstein": "Liechtenstein", "luxembourg": "Luxembourg",
    "monaco": "Monaco", "netherlands": "Netherlands", "switzerland": "Switzerland",
    "österreich": "Austria", "belgien": "Belgium", "frankreich": "France",
    "deutschland": "Germany", "luxemburg": "Luxembourg", "niederlande": "Netherlands",
    "schweiz": "Switzerland",

    # Northern Europe
    "uk": "United Kingdom", "gb": "United Kingdom", "ie": "Ireland", "dk": "Denmark",
    "fi": "Finland", "is": "Iceland", "no": "Norway", "se": "Sweden",
    "ee": "Estonia", "lv": "Latvia", "lt": "Lithuania",
    "united kingdom": "United Kingdom", "great britain": "United Kingdom", "england": "United Kingdom",
    "scotland": "United Kingdom", "wales": "United Kingdom", "ireland": "Ireland",
    "denmark": "Denmark", "finland": "Finland", "iceland": "Iceland",
    "norway": "Norway", "sweden": "Sweden", "estonia": "Estonia",
    "latvia": "Latvia", "lithuania": "Lithuania",
    "dänemark": "Denmark", "finnland": "Finland", "irland": "Ireland",
    "norwegen": "Norway", "island": "Iceland", "schweden": "Sweden",
    "estland": "Estonia", "lettland": "Latvia", "litauen": "Lithuania",

    # Southern Europe
    "es": "Spain", "it": "Italy", "gr": "Greece", "hr": "Croatia",
    "bg": "Bulgaria", "cy": "Cyprus", "mt": "Malta", "si": "Slovenia",
    "al": "Albania", "ad": "Andorra", "ba": "Bosnia and Herzegovina", "xk": "Kosovo",
    "me": "Montenegro", "mk": "North Macedonia", "sm": "San Marino", "va": "Vatican City",
    "spain": "Spain", "italy": "Italy", "greece": "Greece",
    "croatia": "Croatia", "bulgaria": "Bulgaria", "cyprus": "Cyprus",
    "malta": "Malta", "slovenia": "Slovenia", "albania": "Albania",
    "andorra": "Andorra", "bosnia": "Bosnia and Herzegovina", "herzegovina": "Bosnia and Herzegovina",
    "kosovo": "Kosovo", "montenegro": "Montenegro", "macedonia": "North Macedonia",
    "san marino": "San Marino", "vatican": "Vatican City", "portugal": "Portugal",
    "pt": "Portugal",
    "kroatien": "Croatia", "zypern": "Cyprus", "griechenland": "Greece",
    "spanien": "Spain", "italien": "Italy", "bosnien": "Bosnia and Herzegovina",

    # Eastern Europe
    "pl": "Poland", "ro": "Romania", "sk": "Slovakia", "cz": "Czechia",
    "ua": "Ukraine", "tr": "Turkey", "by": "Belarus", "md": "Moldova",
    "hu": "Hungary", "rs": "Serbia",
    "poland": "Poland", "romania": "Romania", "slovakia": "Slovakia",
    "czechia": "Czechia", "czech republic": "Czechia", "ukraine": "Ukraine",
    "turkey": "Turkey", "belarus": "Belarus", "moldova": "Moldova",
    "hungary": "Hungary", "serbia": "Serbia",
    "polen": "Poland", "rumänien": "Romania", "slowakei": "Slovakia",
    "tschechien": "Czechia", "türkei": "Turkey", "weißrussland": "Belarus",
    "serbien": "Serbia",
}

# Mapping of major European cities to country names
CITY_COUNTRY_MAPPING = {
    # Germany
    "berlin": "Germany", "munich": "Germany", "münchen": "Germany", "hamburg": "Germany",
    "frankfurt": "Germany", "cologne": "Germany", "köln": "Germany", "stuttgart": "Germany",
    "düsseldorf": "Germany", "dortmund": "Germany", "essen": "Germany", "leipzig": "Germany",
    "bremen": "Germany", "dresden": "Germany", "hannover": "Germany", "nuremberg": "Germany",
    "nürnberg": "Germany", "duisburg": "Germany", "bochum": "Germany", "wuppertal": "Germany",
    "bielefeld": "Germany", "bonn": "Germany", "munster": "Germany", "münster": "Germany",
    "karlsruhe": "Germany", "mannheim": "Germany", "augsburg": "Germany", "wiesbaden": "Germany",
    "gelsenkirchen": "Germany", "mönchengladbach": "Germany", "braunschweig": "Germany",
    "chemnitz": "Germany", "aachen": "Germany", "halle": "Germany", "magdeburg": "Germany",
    "freiburg": "Germany", "krefeld": "Germany", "lubeck": "Germany", "lübeck": "Germany",
    "mainz": "Germany", "erfurt": "Germany", "rostock": "Germany", "kassel": "Germany",
    "hagen": "Germany", "hamm": "Germany", "saarbrucken": "Germany", "saarbrücken": "Germany",
    "mulheim": "Germany", "mülheim": "Germany", "herne": "Germany", "ludwigshafen": "Germany",
    "osnabruck": "Germany", "osnabrück": "Germany", "solingen": "Germany", "leverkusen": "Germany",
    "oldenburg": "Germany", "neuss": "Germany", "potsdam": "Germany", "heidelberg": "Germany",
    "paderborn": "Germany", "darmstadt": "Germany", "wurzburg": "Germany", "würzburg": "Germany",
    "regensburg": "Germany", "ingolstadt": "Germany", "heilbronn": "Germany", "ulm": "Germany",
    "wolfsburg": "Germany", "gottingen": "Germany", "göttingen": "Germany", "offenbach": "Germany",
    "pforzheim": "Germany", "recklinghausen": "Germany", "bottrop": "Germany", "furth": "Germany",
    "fürth": "Germany", "remscheid": "Germany", "reutlingen": "Germany", "moers": "Germany",
    "koblenz": "Germany", "siegen": "Germany", "bergisch gladbach": "Germany", "jena": "Germany",
    "erlangen": "Germany", "trier": "Germany", "salzgitter": "Germany",
    
    # France
    "paris": "France", "lyon": "France", "marseille": "France", "toulouse": "France",
    "bordeaux": "France", "nice": "France",
    
    # Netherlands
    "amsterdam": "Netherlands", "utrecht": "Netherlands", "rotterdam": "Netherlands",
    "hague": "Netherlands", "den haag": "Netherlands",
    
    # Austria
    "vienna": "Austria", "wien": "Austria", "graz": "Austria", "linz": "Austria",
    "salzburg": "Austria", "innsbruck": "Austria",
    
    # Belgium
    "brussels": "Belgium", "bruxelles": "Belgium", "brüssel": "Belgium",
    "antwerp": "Belgium", "antwerpen": "Belgium", "ghent": "Belgium",
    "liege": "Belgium", "liège": "Belgium",
    
    # Switzerland
    "zurich": "Switzerland", "zürich": "Switzerland", "geneva": "Switzerland",
    "genf": "Switzerland", "basel": "Switzerland", "lausanne": "Switzerland",
    "bern": "Switzerland",
    
    # United Kingdom
    "london": "United Kingdom", "manchester": "United Kingdom", "birmingham": "United Kingdom",
    "leeds": "United Kingdom", "glasgow": "United Kingdom", "sheffield": "United Kingdom",
    "liverpool": "United Kingdom", "bristol": "United Kingdom", "edinburgh": "United Kingdom",
    
    # Denmark
    "copenhagen": "Denmark", "københavn": "Denmark",
    
    # Sweden
    "stockholm": "Sweden", "gothenburg": "Sweden", "göteborg": "Sweden",
    "malmo": "Sweden", "malmö": "Sweden", "uppsala": "Sweden",
    
    # Finland
    "helsinki": "Finland",
    
    # Ireland
    "dublin": "Ireland",
    
    # Estonia
    "tallinn": "Estonia", "tallin": "Estonia",
    
    # Latvia
    "riga": "Latvia",
    
    # Lithuania
    "vilnius": "Lithuania",
    
    # Norway
    "oslo": "Norway", "bergen": "Norway", "trondheim": "Norway", "stavanger": "Norway",
    
    # Iceland
    "reykjavik": "Iceland",
    
    # Spain
    "madrid": "Spain", "barcelona": "Spain", "malaga": "Spain", "málaga": "Spain",
    "valencia": "Spain", "seville": "Spain", "sevilla": "Spain",
    
    # Italy
    "rome": "Italy", "roma": "Italy", "milan": "Italy", "milano": "Italy",
    
    # Greece
    "athens": "Greece",
    
    # Bulgaria
    "sofia": "Bulgaria",
    
    # Slovenia
    "ljubljana": "Slovenia",
    
    # Croatia
    "zagreb": "Croatia",
    
    # Malta
    "valletta": "Malta",
    
    # Cyprus
    "nicosia": "Cyprus",
    
    # Portugal
    "porto": "Portugal", "lisbon": "Portugal", "lisboa": "Portugal",
    
    # Serbia
    "belgrade": "Serbia",
    
    # Bosnia
    "sarajevo": "Bosnia and Herzegovina",
    
    # North Macedonia
    "skopje": "North Macedonia",
    
    # Albania
    "tirana": "Albania",
    
    # Kosovo
    "pristina": "Kosovo",
    
    # Montenegro
    "podgorica": "Montenegro",
    
    # Poland
    "warsaw": "Poland", "warszawa": "Poland", "krakow": "Poland", "kraków": "Poland",
    "wroclaw": "Poland", "wrocław": "Poland", "gdansk": "Poland", "gdańsk": "Poland",
    "poznan": "Poland", "poznań": "Poland",
    
    # Hungary
    "budapest": "Hungary",
    
    # Czechia
    "prague": "Czechia", "praha": "Czechia",
    
    # Romania
    "bucharest": "Romania", "bucuresti": "Romania",
    
    # Slovakia
    "bratislava": "Slovakia",
    
    # Ukraine
    "kyiv": "Ukraine", "kiev": "Ukraine", "kharkiv": "Ukraine", "lviv": "Ukraine",
    "odessa": "Ukraine", "dnipro": "Ukraine",
    
    # Turkey
    "istanbul": "Turkey", "ankara": "Turkey", "izmir": "Turkey",
    
    # Moldova
    "chisinau": "Moldova",
    
    # Belarus
    "minsk": "Belarus",
}


INVALID_COUNTRY_REGIONS = frozenset({"Remote"})


def normalize_region_bucket(region: str) -> str:
    """Map invalid geographic buckets (e.g. work-style 'Remote') to Unspecified."""
    r = str(region or "").strip()
    if r in INVALID_COUNTRY_REGIONS:
        return "Unspecified"
    return r or "Other"


def classify_region(location_str, title_str="", description_str="", item=None):
    """
    Classifies a job location into a country name, 'Unspecified', or 'Other'.
    Work style (remote/hybrid/on-site) is handled separately via work_style.
    """
    # 1. Check nested location dict from Indeed
    if isinstance(item, dict):
        loc_dict = item.get("location")
        if isinstance(loc_dict, dict):
            c_code = str(loc_dict.get("countryCode", "")).lower().strip()
            c_name = str(loc_dict.get("countryName", "")).lower().strip()
            if c_code in COUNTRY_MAPPING:
                return COUNTRY_MAPPING[c_code]
            if c_name in COUNTRY_MAPPING:
                return COUNTRY_MAPPING[c_name]

    loc = _safe_str(location_str)
    title = _safe_str(title_str)

    loc_words = set(re.findall(r"\b[a-z]+\b", loc))

    # 2. Check explicit country name in location
    for country, country_name in COUNTRY_MAPPING.items():
        if len(country) > 2 and country in loc:
            return country_name

    # 3. Check ISO codes in location words
    for code in loc_words:
        if code in COUNTRY_MAPPING:
            return COUNTRY_MAPPING[code]

    # 4. Check cities in location
    for city, country_name in CITY_COUNTRY_MAPPING.items():
        if city in loc:
            return country_name

    # 5. Check country in title (fallback)
    for country, country_name in COUNTRY_MAPPING.items():
        if len(country) > 2 and country in title:
            return country_name

    # 6. Determine Remote vs Other fallback
    is_remote = False
    if item is not None:
        try:
            work_style = item.get("work_style")
            if _safe_str(work_style) == "remote":
                is_remote = True
            
            is_remote_val = item.get("remote")
            if is_remote_val is True or _safe_str(is_remote_val) == "true":
                is_remote = True
                
            is_remote_val2 = item.get("is_remote")
            if is_remote_val2 is True or _safe_str(is_remote_val2) == "true":
                is_remote = True
        except Exception:
            pass

    if not is_remote:
        # Check location string for remote terms
        if any(k in loc for k in ("remote", "worldwide", "anywhere", "telecommute", "home office", "home-office", "work from home")):
            is_remote = True
        # Check title string for remote keywords
        elif any(k in title for k in ("remote", "worldwide", "anywhere")):
            is_remote = True

    if is_remote:
        return "Unspecified"

    # Fallback for Bundesagentur für Arbeit (German Federal Employment Agency)
    if item is not None:
        try:
            source = item.get("source")
            if _safe_str(source) == "ba_api":
                return "Germany"
        except Exception:
            pass

    return "Other"
