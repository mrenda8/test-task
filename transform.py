import re
from datetime import date

KNOWN_POSITIONS = {
    "goalkeeper": "goalkeeper",
    "centre-back": "centre-back",
    "center-back": "centre-back",
    "centre back": "centre-back",
    "left-back": "left-back",
    "left back": "left-back",
    "right-back": "right-back",
    "right back": "right-back",
    "wing-back": "wing-back",
    "wing back": "wing-back",
    "defensive midfielder": "defensive midfielder",
    "central midfielder": "central midfielder",
    "attacking midfielder": "attacking midfielder",
    "midfielder": "midfielder",
    "left winger": "left winger",
    "right winger": "right winger",
    "winger": "winger",
    "forward": "forward",
    "second striker": "second striker",
    "striker": "striker",
    "defender": "defender",
}

POSITION_ORDER = {
    "goalkeeper": 1,
    "right-back": 2,
    "left-back": 3,
    "wing-back": 4,
    "centre-back": 5,
    "defender": 6,
    "defensive midfielder": 7,
    "central midfielder": 8,
    "midfielder": 9,
    "attacking midfielder": 10,
    "right winger": 11,
    "left winger": 12,
    "winger": 13,
    "second striker": 14,
    "striker": 15,
    "forward": 16,
}


#Removing references of type [1] and normalizes whitespace
def strip_references(text):
    if text is None:
        return None

    text = re.sub(r"\[[^\]]+\]", "", str(text))
    text = re.sub(r"\s+", " ", text).strip()

    return text or None


#Cleaning up the player name and removes additions like "(footballer ...)"
def clean_name(name):
    name = strip_references(name)
    if not name:
        return None

    name = re.sub(r"\s*\((footballer.*?|born .*?)\)\s*", "", name, flags=re.IGNORECASE)
    name = re.sub(r"\s+", " ", name).strip(" ,")

    return name or None


#Cleaning full_name; if it doesn't exist use cleaned name
def clean_full_name(full_name, name):
    full_name = strip_references(full_name)
    if not full_name:
        return clean_name(name)

    return full_name


#Normalizing date_of_birth u yyyy-mm-dd if it is in ISO format
def normalize_date_of_birth(value):
    value = strip_references(value)
    if not value:
        return None

    match = re.search(r"\b(\d{4}-\d{2}-\d{2})\b", value)
    if match:
        return match.group(1)

    return None


#Calculating age from date of birth
def calculate_age_from_dob(dob):
    if not dob:
        return None

    try:
        year, month, day = map(int, dob.split("-"))
        born = date(year, month, day)
        today = date.today()
        return today.year - born.year - ((today.month, today.day) < (born.month, born.day))
    except Exception:
        return None


#Cleaning country string
def clean_country(value):
    value = strip_references(value)
    if not value:
        return None

    value = value.strip(" ,")
    return value or None


#Splitting raw birthplace to place and country
def split_birth_place_and_country(value):
    value = strip_references(value)
    if not value:
        return None, None

    value = re.sub(r"\(.*?\)", "", value).strip(" ,")
    parts = [p.strip() for p in value.split(",") if p.strip()]

    if len(parts) >= 2:
        return ", ".join(parts[:-1]), parts[-1]

    return value, None


#Splitting raw positions string to positions
def split_position_parts(value):
    if not value:
        return []

    value = strip_references(value)
    if not value:
        return []

    normalized = value.lower()
    normalized = normalized.replace("/", ",")
    normalized = normalized.replace(" and ", ",")
    normalized = normalized.replace(";", ",")
    normalized = re.sub(r"\s*,\s*", ",", normalized)
    normalized = re.sub(r"\s{2,}", ",", normalized)
    normalized = re.sub(r"\s+", " ", normalized).strip()

    return [part.strip() for part in normalized.split(",") if part.strip()]


#Normalizing positions to known names and sorting by row defined in the beginning of the code
def normalize_positions(value):
    parts = split_position_parts(value)
    if not parts:
        return None

    normalized_positions = []

    for part in parts:
        part = part.strip().lower()
        if not part:
            continue

        if part in KNOWN_POSITIONS:
            standardized = KNOWN_POSITIONS[part]
            if standardized not in normalized_positions:
                normalized_positions.append(standardized)
            continue

        matched = None
        for known in sorted(KNOWN_POSITIONS.keys(), key=len, reverse=True):
            pattern = r"\b" + re.escape(known) + r"\b"
            if re.search(pattern, part):
                matched = KNOWN_POSITIONS[known]
                break

        if matched:
            if matched not in normalized_positions:
                normalized_positions.append(matched)
        else:
            if part not in normalized_positions:
                normalized_positions.append(part)

    normalized_positions.sort(key=lambda x: POSITION_ORDER.get(x, 999))

    return ", ".join(normalized_positions) if normalized_positions else None


#Cleaning current club string and removing "(on loan from ...)" part
def clean_current_club(value):
    value = strip_references(value)
    if not value:
        return None

    value = re.sub(r"\s*\(on loan from .*?\)", "", value, flags=re.IGNORECASE).strip(" ,")
    return value or None


#Cleaning national team string
def clean_national_team(value):
    value = strip_references(value)
    if not value:
        return None

    return value.strip(" ,") or None


#Final normalization of player dict
def normalize_player_record(player):
    raw_birth = player.get("place_of_birth")
    raw_country = player.get("country_of_birth")

    place, country_from_place = split_birth_place_and_country(raw_birth)
    dob = normalize_date_of_birth(player.get("date_of_birth"))
    is_deceased = bool(player.get("_is_deceased"))

    if is_deceased:
        age = None
    else:
        age = player.get("age") or calculate_age_from_dob(dob)

    cleaned_name = clean_name(player.get("name"))
    cleaned_full_name = clean_full_name(player.get("full_name"), cleaned_name)
    country = clean_country(raw_country) or country_from_place

    player["name"] = cleaned_name
    player["full_name"] = cleaned_full_name
    player["date_of_birth"] = dob
    player["age"] = age
    player["place_of_birth"] = place
    player["country_of_birth"] = country

    if player.get("place_of_birth") and player.get("country_of_birth"):
        if player["place_of_birth"].strip().lower() == player["country_of_birth"].strip().lower():
            player["place_of_birth"] = None

    if not player.get("country_of_birth") and player.get("place_of_birth"):
        player["country_of_birth"] = player["place_of_birth"]
        player["place_of_birth"] = None

    player["positions"] = normalize_positions(player.get("positions"))
    player["current_club"] = clean_current_club(player.get("current_club"))
    player["national_team"] = clean_national_team(player.get("national_team"))

    return player