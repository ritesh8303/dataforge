import re

# 27 EU Member States (ISO codes, English/German names)
EU_COUNTRIES = {
    # ISO Codes
    'at', 'be', 'bg', 'cy', 'cz', 'de', 'dk', 'ee', 'es', 'fi', 'fr', 'gr', 'hr', 'hu', 
    'ie', 'it', 'lt', 'lu', 'lv', 'mt', 'nl', 'pl', 'pt', 'ro', 'se', 'si', 'sk',
    # English names
    'austria', 'belgium', 'bulgaria', 'croatia', 'cyprus', 'czechia', 'czech republic', 'denmark',
    'estonia', 'finland', 'france', 'germany', 'greece', 'hungary', 'ireland', 'italy', 'latvia',
    'lithuania', 'luxembourg', 'malta', 'netherlands', 'poland', 'portugal', 'romania', 'slovakia',
    'slovenia', 'spain', 'sweden',
    # German names
    'österreich', 'belgien', 'bulgarien', 'kroatien', 'zypern', 'tschechien', 'dänemark',
    'estland', 'finnland', 'frankreich', 'deutschland', 'griechenland', 'ungarn', 'irland', 'italien',
    'lettland', 'litauen', 'luxemburg', 'niederlande', 'polen', 'rumänien', 'slowakei', 'slowenien',
    'spanien', 'schweden'
}

# Major EU cities
EU_CITIES = {
    'berlin', 'munich', 'münchen', 'hamburg', 'frankfurt', 'cologne', 'köln', 'stuttgart', 'düsseldorf',
    'dortmund', 'essen', 'leipzig', 'bremen', 'dresden', 'hannover', 'nuremberg', 'nürnberg', 'duisburg',
    'bochum', 'wuppertal', 'bielefeld', 'bonn', 'munster', 'münster', 'karlsruhe', 'mannheim', 'augsburg',
    'wiesbaden', 'gelsenkirchen', 'mönchengladbach', 'braunschweig', 'chemnitz', 'aachen', 'halle', 
    'magdeburg', 'freiburg', 'krefeld', 'lubeck', 'lübeck', 'mainz', 'erfurt', 'rostock', 'kassel', 
    'hagen', 'hamm', 'saarbrucken', 'saarbrücken', 'mulheim', 'mülheim', 'herne', 'ludwigshafen', 
    'osnabruck', 'osnabrück', 'solingen', 'leverkusen', 'oldenburg', 'neuss', 'potsdam', 'heidelberg', 
    'paderborn', 'darmstadt', 'wurzburg', 'würzburg', 'regensburg', 'ingolstadt', 'heilbronn', 'ulm', 
    'wolfsburg', 'gottingen', 'göttingen', 'offenbach', 'pforzheim', 'recklinghausen', 'bottrop', 
    'furth', 'fürth', 'remscheid', 'reutlingen', 'moers', 'koblenz', 'siegen', 'bergisch gladbach', 
    'jena', 'erlangen', 'trier', 'salzgitter', 'paris', 'amsterdam', 'madrid', 'barcelona', 'rome', 
    'roma', 'milan', 'milano', 'vienna', 'wien', 'brussels', 'bruxelles', 'brüssel', 'warsaw', 
    'warszawa', 'budapest', 'prague', 'praha', 'copenhagen', 'københavn', 'stockholm', 'helsinki', 
    'lisbon', 'lisboa', 'dublin', 'athens', 'sofia', 'bucharest', 'bucuresti', 'bratislava', 
    'ljubljana', 'zagreb', 'tallinn', 'riga', 'vilnius', 'valletta', 'nicosia',
    'malaga', 'málaga', 'valencia', 'seville', 'sevilla', 'lyon', 'marseille', 'toulouse', 'bordeaux',
    'nice', 'porto', 'utrecht', 'rotterdam', 'hague', 'den haag', 'antwerp', 'antwerpen', 'ghent',
    'liege', 'liège', 'gothenburg', 'göteborg', 'malmo', 'malmö', 'uppsala', 'krakow', 'kraków',
    'wroclaw', 'wrocław', 'gdansk', 'gdańsk', 'poznan', 'poznań', 'graz', 'linz', 'salzburg',
    'innsbruck', 'tallin'
}

def is_in_eu(location_str, title_str="", description_str="", item=None):
    """
    Unified check to determine if a job is in the European Union.
    Returns True if the location is in the EU, False otherwise.
    """
    # 1. Check direct nested location dict from Indeed/LinkedIn
    if isinstance(item, dict):
        loc_dict = item.get('location')
        if isinstance(loc_dict, dict):
            c_code = str(loc_dict.get('countryCode', '')).lower().strip()
            c_name = str(loc_dict.get('countryName', '')).lower().strip()
            if c_code in EU_COUNTRIES or c_name in EU_COUNTRIES:
                return True

    loc = str(location_str or "").strip().lower()
    title = str(title_str or "").strip().lower()
    desc = str(description_str or "").strip().lower()

    # 2. Blocklist of multi-word non-EU phrases (UK, US, Canada, etc.)
    non_eu_phrases = {
        "united kingdom", "united states", "new york", "san francisco", "los angeles",
        "south africa", "new zealand", "mountain view", "palo alto", "redwood city",
        "menlo park", "tel aviv", "buenos aires", "cape canaveral", "ho chi minh"
    }
    if any(phrase in loc for phrase in non_eu_phrases) or any(phrase in title for phrase in non_eu_phrases):
        return False

    # Extract standalone words
    loc_words = set(re.findall(r"\b[a-z]+\b", loc))
    title_words = set(re.findall(r"\b[a-z]+\b", title))
    combined_words = loc_words.union(title_words)

    # 3. Standalone non-EU words (UK, Switzerland, US, etc., and US State abbreviations)
    non_eu_words = {
        "uk", "london", "switzerland", "zurich", "zürich", "geneva", "genf",
        "norway", "oslo", "iceland", "usa", "us", "america", "canada", "toronto",
        "vancouver", "australia", "sydney", "melbourne", "singapore", "sg", "japan", "tokyo",
        "india", "bangalore", "bengaluru", "china", "shanghai", "suzhou", "wuxi", "cn",
        "brazil", "mexico", "ukraine", "russia", "israel", "boston", "seattle",
        "austin", "texas", "tx", "california", "ca", "chicago", "denver", "atlanta",
        "miami", "hawthorne", "bastrop", "redmond", "wa", "starbase", "saddleback",
        "capitol", "sunnyvale", "woodinville", "fl", "egypt", "maadi", "eg", "dubai",
        "ae", "vietnam", "vn", "argentina", "ar", "latam",
        # US State abbreviations (except 'de' Delaware / Germany and 'mt' Montana / Malta)
        "al", "ak", "az", "ar", "co", "ct", "ga", "hi", "id", "il", "in", "ia", "ks", "ky", 
        "la", "me", "md", "ma", "mi", "mn", "ms", "mo", "ne", "nv", "nh", "nj", "nm", "ny", 
        "nc", "nd", "oh", "ok", "or", "pa", "ri", "sc", "sd", "tn", "tx", "ut", "vt", "va", 
        "wa", "wv", "wi", "wy"
    }
    if not non_eu_words.isdisjoint(combined_words):
        return False

    # 4. Check explicit EU country names
    for country in EU_COUNTRIES:
        if len(country) > 2 and (country in loc or country in title):
            return True

    # 5. Check for explicit EU country ISO codes in location words only (to avoid 'it', 'hr', 'pt' false positives in title)
    two_letter_eu_codes = {
        'at', 'be', 'bg', 'cy', 'cz', 'de', 'dk', 'ee', 'es', 'fi', 'fr', 'gr', 'hr', 'hu', 
        'ie', 'it', 'lt', 'lu', 'lv', 'mt', 'nl', 'pl', 'pt', 'ro', 'se', 'si', 'sk'
    }
    if not two_letter_eu_codes.isdisjoint(loc_words):
        return True

    # 6. Check for known EU cities
    for city in EU_CITIES:
        if city in loc:
            return True

    # 7. Check for general EU keywords
    eu_keywords = {"eu", "european", "union", "europe", "emea"}
    if not eu_keywords.isdisjoint(combined_words):
        return True

    return False
