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

    loc = str(location_str or "").strip().lower()
    title = str(title_str or "").strip().lower()

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
