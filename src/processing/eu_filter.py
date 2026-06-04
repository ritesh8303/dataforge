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
    
    # Other EU Cities
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
    # 1. Check direct nested location dict from Indeed/LinkedIn
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


# Mapping of European countries (names & codes) to regions
REGION_MAPPING = {
    # Western Europe
    "at": "Western Europe", "be": "Western Europe", "fr": "Western Europe", "de": "Western Europe",
    "li": "Western Europe", "lu": "Western Europe", "mc": "Western Europe", "nl": "Western Europe",
    "ch": "Western Europe",
    "austria": "Western Europe", "belgium": "Western Europe", "france": "Western Europe",
    "germany": "Western Europe", "liechtenstein": "Western Europe", "luxembourg": "Western Europe",
    "monaco": "Western Europe", "netherlands": "Western Europe", "switzerland": "Western Europe",
    "österreich": "Western Europe", "belgien": "Western Europe", "frankreich": "Western Europe",
    "deutschland": "Western Europe", "luxemburg": "Western Europe", "niederlande": "Western Europe",
    "schweiz": "Western Europe",

    # Northern Europe
    "uk": "Northern Europe", "gb": "Northern Europe", "ie": "Northern Europe", "dk": "Northern Europe",
    "fi": "Northern Europe", "is": "Northern Europe", "no": "Northern Europe", "se": "Northern Europe",
    "ee": "Northern Europe", "lv": "Northern Europe", "lt": "Northern Europe",
    "united kingdom": "Northern Europe", "great britain": "Northern Europe", "england": "Northern Europe",
    "scotland": "Northern Europe", "wales": "Northern Europe", "ireland": "Northern Europe",
    "denmark": "Northern Europe", "finland": "Northern Europe", "iceland": "Northern Europe",
    "norway": "Northern Europe", "sweden": "Northern Europe", "estonia": "Northern Europe",
    "latvia": "Northern Europe", "lithuania": "Northern Europe",
    "dänemark": "Northern Europe", "finnland": "Northern Europe", "irland": "Northern Europe",
    "norwegen": "Northern Europe", "island": "Northern Europe", "schweden": "Northern Europe",
    "estland": "Northern Europe", "lettland": "Northern Europe", "litauen": "Northern Europe",

    # Southern Europe
    "es": "Southern Europe", "it": "Southern Europe", "gr": "Southern Europe", "hr": "Southern Europe",
    "bg": "Southern Europe", "cy": "Southern Europe", "mt": "Southern Europe", "si": "Southern Europe",
    "al": "Southern Europe", "ad": "Southern Europe", "ba": "Southern Europe", "xk": "Southern Europe",
    "me": "Southern Europe", "mk": "Southern Europe", "sm": "Southern Europe", "va": "Southern Europe",
    "spain": "Southern Europe", "italy": "Southern Europe", "greece": "Southern Europe",
    "croatia": "Southern Europe", "bulgaria": "Southern Europe", "cyprus": "Southern Europe",
    "malta": "Southern Europe", "slovenia": "Southern Europe", "albania": "Southern Europe",
    "andorra": "Southern Europe", "bosnia": "Southern Europe", "herzegovina": "Southern Europe",
    "kosovo": "Southern Europe", "montenegro": "Southern Europe", "macedonia": "Southern Europe",
    "san marino": "Southern Europe", "vatican": "Southern Europe", "portugal": "Southern Europe",
    "pt": "Southern Europe",
    "kroatien": "Southern Europe", "zypern": "Southern Europe", "griechenland": "Southern Europe",
    "spanien": "Southern Europe", "italien": "Southern Europe", "bosnien": "Southern Europe",

    # Eastern Europe
    "pl": "Eastern Europe", "ro": "Eastern Europe", "sk": "Eastern Europe", "cz": "Eastern Europe",
    "ua": "Eastern Europe", "tr": "Eastern Europe", "by": "Eastern Europe", "md": "Eastern Europe",
    "hu": "Eastern Europe", "rs": "Eastern Europe",
    "poland": "Eastern Europe", "romania": "Eastern Europe", "slovakia": "Eastern Europe",
    "czechia": "Eastern Europe", "czech republic": "Eastern Europe", "ukraine": "Eastern Europe",
    "turkey": "Eastern Europe", "belarus": "Eastern Europe", "moldova": "Eastern Europe",
    "hungary": "Eastern Europe", "serbia": "Eastern Europe",
    "polen": "Eastern Europe", "rumänien": "Eastern Europe", "slowakei": "Eastern Europe",
    "tschechien": "Eastern Europe", "türkei": "Eastern Europe", "weißrussland": "Eastern Europe",
    "serbien": "Eastern Europe",
}

# Mapping of major European cities to regions
CITY_REGION_MAPPING = {
    # Western Europe Cities
    "berlin": "Western Europe", "munich": "Western Europe", "münchen": "Western Europe", "hamburg": "Western Europe",
    "frankfurt": "Western Europe", "cologne": "Western Europe", "köln": "Western Europe", "stuttgart": "Western Europe",
    "düsseldorf": "Western Europe", "dortmund": "Western Europe", "essen": "Western Europe", "leipzig": "Western Europe",
    "bremen": "Western Europe", "dresden": "Western Europe", "hannover": "Western Europe", "nuremberg": "Western Europe",
    "nürnberg": "Western Europe", "duisburg": "Western Europe", "bochum": "Western Europe", "wuppertal": "Western Europe",
    "bielefeld": "Western Europe", "bonn": "Western Europe", "munster": "Western Europe", "münster": "Western Europe",
    "karlsruhe": "Western Europe", "mannheim": "Western Europe", "augsburg": "Western Europe", "wiesbaden": "Western Europe",
    "gelsenkirchen": "Western Europe", "mönchengladbach": "Western Europe", "braunschweig": "Western Europe",
    "chemnitz": "Western Europe", "aachen": "Western Europe", "halle": "Western Europe", "magdeburg": "Western Europe",
    "freiburg": "Western Europe", "krefeld": "Western Europe", "lubeck": "Western Europe", "lübeck": "Western Europe",
    "mainz": "Western Europe", "erfurt": "Western Europe", "rostock": "Western Europe", "kassel": "Western Europe",
    "hagen": "Western Europe", "hamm": "Western Europe", "saarbrucken": "Western Europe", "saarbrücken": "Western Europe",
    "mulheim": "Western Europe", "mülheim": "Western Europe", "herne": "Western Europe", "ludwigshafen": "Western Europe",
    "osnabruck": "Western Europe", "osnabrück": "Western Europe", "solingen": "Western Europe", "leverkusen": "Western Europe",
    "oldenburg": "Western Europe", "neuss": "Western Europe", "potsdam": "Western Europe", "heidelberg": "Western Europe",
    "paderborn": "Western Europe", "darmstadt": "Western Europe", "wurzburg": "Western Europe", "würzburg": "Western Europe",
    "regensburg": "Western Europe", "ingolstadt": "Western Europe", "heilbronn": "Western Europe", "ulm": "Western Europe",
    "wolfsburg": "Western Europe", "gottingen": "Western Europe", "göttingen": "Western Europe", "offenbach": "Western Europe",
    "pforzheim": "Western Europe", "recklinghausen": "Western Europe", "bottrop": "Western Europe", "furth": "Western Europe",
    "fürth": "Western Europe", "remscheid": "Western Europe", "reutlingen": "Western Europe", "moers": "Western Europe",
    "koblenz": "Western Europe", "siegen": "Western Europe", "bergisch gladbach": "Western Europe", "jena": "Western Europe",
    "erlangen": "Western Europe", "trier": "Western Europe", "salzgitter": "Western Europe",
    "paris": "Western Europe", "amsterdam": "Western Europe", "vienna": "Western Europe", "wien": "Western Europe",
    "brussels": "Western Europe", "bruxelles": "Western Europe", "brüssel": "Western Europe",
    "lyon": "Western Europe", "marseille": "Western Europe", "toulouse": "Western Europe", "bordeaux": "Western Europe",
    "nice": "Western Europe", "utrecht": "Western Europe", "rotterdam": "Western Europe", "hague": "Western Europe",
    "den haag": "Western Europe", "antwerp": "Western Europe", "antwerpen": "Western Europe", "ghent": "Western Europe",
    "liege": "Western Europe", "liège": "Western Europe", "zurich": "Western Europe", "zürich": "Western Europe",
    "geneva": "Western Europe", "genf": "Western Europe", "basel": "Western Europe", "lausanne": "Western Europe",
    "bern": "Western Europe", "graz": "Western Europe", "linz": "Western Europe", "salzburg": "Western Europe",
    "innsbruck": "Western Europe",

    # Northern Europe Cities
    "london": "Northern Europe", "manchester": "Northern Europe", "birmingham": "Northern Europe", "leeds": "Northern Europe",
    "glasgow": "Northern Europe", "sheffield": "Northern Europe", "liverpool": "Northern Europe", "bristol": "Northern Europe",
    "edinburgh": "Northern Europe", "copenhagen": "Northern Europe", "københavn": "Northern Europe", "stockholm": "Northern Europe",
    "helsinki": "Northern Europe", "dublin": "Northern Europe", "tallinn": "Northern Europe", "riga": "Northern Europe",
    "vilnius": "Northern Europe", "gothenburg": "Northern Europe", "göteborg": "Northern Europe", "malmo": "Northern Europe",
    "malmö": "Northern Europe", "uppsala": "Northern Europe", "oslo": "Northern Europe", "bergen": "Northern Europe",
    "trondheim": "Northern Europe", "stavanger": "Northern Europe", "reykjavik": "Northern Europe", "tallin": "Northern Europe",

    # Southern Europe Cities
    "madrid": "Southern Europe", "barcelona": "Southern Europe", "rome": "Southern Europe", "roma": "Southern Europe",
    "milan": "Southern Europe", "milano": "Southern Europe", "athens": "Southern Europe", "sofia": "Southern Europe",
    "ljubljana": "Southern Europe", "zagreb": "Southern Europe", "valletta": "Southern Europe", "nicosia": "Southern Europe",
    "malaga": "Southern Europe", "málaga": "Southern Europe", "valencia": "Southern Europe", "seville": "Southern Europe",
    "sevilla": "Southern Europe", "porto": "Southern Europe", "belgrade": "Southern Europe", "sarajevo": "Southern Europe",
    "skopje": "Southern Europe", "tirana": "Southern Europe", "pristina": "Southern Europe", "podgorica": "Southern Europe",

    # Eastern Europe Cities
    "warsaw": "Eastern Europe", "warszawa": "Eastern Europe", "budapest": "Eastern Europe", "prague": "Eastern Europe",
    "praha": "Eastern Europe", "bucharest": "Eastern Europe", "bucuresti": "Eastern Europe", "bratislava": "Eastern Europe",
    "krakow": "Eastern Europe", "kraków": "Eastern Europe", "wroclaw": "Eastern Europe", "wrocław": "Eastern Europe",
    "gdansk": "Eastern Europe", "gdańsk": "Eastern Europe", "poznan": "Eastern Europe", "poznań": "Eastern Europe",
    "kyiv": "Eastern Europe", "kiev": "Eastern Europe", "kharkiv": "Eastern Europe", "lviv": "Eastern Europe",
    "odessa": "Eastern Europe", "dnipro": "Eastern Europe", "istanbul": "Eastern Europe", "ankara": "Eastern Europe",
    "izmir": "Eastern Europe", "chisinau": "Eastern Europe", "minsk": "Eastern Europe",
}


def classify_region(location_str, title_str="", description_str="", item=None):
    """
    Classifies a job location into one of the European regions:
    'Western Europe', 'Northern Europe', 'Southern Europe', 'Eastern Europe', or 'Other/Remote'
    """
    # 1. Check nested location dict from Indeed/LinkedIn
    if isinstance(item, dict):
        loc_dict = item.get("location")
        if isinstance(loc_dict, dict):
            c_code = str(loc_dict.get("countryCode", "")).lower().strip()
            c_name = str(loc_dict.get("countryName", "")).lower().strip()
            if c_code in REGION_MAPPING:
                return REGION_MAPPING[c_code]
            if c_name in REGION_MAPPING:
                return REGION_MAPPING[c_name]

    loc = _safe_str(location_str)
    title = _safe_str(title_str)

    loc_words = set(re.findall(r"\b[a-z]+\b", loc))

    # 2. Check explicit country name in location
    for country, region in REGION_MAPPING.items():
        if len(country) > 2 and country in loc:
            return region

    # 3. Check ISO codes in location words
    for code in loc_words:
        if code in REGION_MAPPING:
            return REGION_MAPPING[code]

    # 4. Check cities in location
    for city, region in CITY_REGION_MAPPING.items():
        if city in loc:
            return region

    # 5. Check country in title (fallback)
    for country, region in REGION_MAPPING.items():
        if len(country) > 2 and country in title:
            return region

    return "Other/Remote"
