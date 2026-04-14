import csv
import re
import sqlite3
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urldefrag

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from transform import normalize_player_record

DB_PATH = Path("data/db/players.sqlite")
DEFAULT_URLS_PATH = Path("data/raw/playersURLs.csv")
PLAYERS_DATA_PATH = Path("data/raw/playersData.csv")
#OUTPUT_PATH = Path("output/scraped_players.csv")
SCHEMA_PATH = Path("createSchema.sql")


# -----------------------------------------------------------------------------
# Database and session setup
# -----------------------------------------------------------------------------

# Base creation and createSchema.sql load 
def init_db():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)

    with open(SCHEMA_PATH, "r", encoding="utf-8") as f:
        conn.executescript(f.read())

    conn.commit()
    return conn


# Request session with retry logic and HTTP headers creation
def create_session():
    session = requests.Session()

    retry_strategy = Retry(
        total=3,
        backoff_factor=2,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )

    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)

    session.headers.update(
        {
            "User-Agent": "FootballPlayerScraper/1.0",
            "Accept-Language": "en-US,en;q=0.9",
        }
    )

    return session


# -----------------------------------------------------------------------------
# URL loading and normalization
# -----------------------------------------------------------------------------

# URL trim, fragment erasal, normalized URL return 
def normalize_url(raw_url):
    if not raw_url:
        return None

    raw_url = str(raw_url).strip()
    if not raw_url:
        return None

    clean_url, _ = urldefrag(raw_url)
    clean_url = clean_url.strip()

    return clean_url or None


# Load URLs from playersURLs.csv + deduplication
def load_urls(path):
    urls = []
    seen = set()

    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.reader(f)

        for row in reader:
            if not row:
                continue

            clean_url = normalize_url(row[0])

            if clean_url and clean_url not in seen:
                seen.add(clean_url)
                urls.append(clean_url)

    return urls


# Load URLs from playersData.csv + deduplication
def load_urls_from_players_data(path):
    urls = []
    seen = set()

    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f, delimiter=";")

        for row in reader:
            clean_url = normalize_url(row.get("URL"))

            if clean_url and clean_url not in seen:
                seen.add(clean_url)
                urls.append(clean_url)

    return urls


# URLs list merge 
def merge_url_lists(*url_lists):
    merged = []
    seen = set()

    for url_list in url_lists:
        for url in url_list:
            if url and url not in seen:
                seen.add(url)
                merged.append(url)

    return merged


# -----------------------------------------------------------------------------
# HTTP fetch and generic page validation
# -----------------------------------------------------------------------------

# HTML page fetch
def fetch_page(session, url):
    response = session.get(url, timeout=20)
    response.raise_for_status()
    return response.text


# Non-player URL rejecting function
def is_invalid_url(url):
    lowered = url.lower()

    invalid_patterns = [
        "/wiki/category:",
        "/wiki/list_of_",
        "disambiguation",
    ]

    return any(pattern in lowered for pattern in invalid_patterns)


# Disambiguation pages rejecting function
def is_disambiguation_page(soup):
    page_text = soup.get_text(" ", strip=True).lower()

    disambiguation_markers = [
        "may refer to:",
        "can refer to:",
        "refer to:",
    ]

    return any(marker in page_text for marker in disambiguation_markers)


# player_id generator from url
def generate_player_id_from_url(url):
    return str(uuid.uuid5(uuid.NAMESPACE_URL, url))


# Football page checkup function for other sports players rejection
def has_football_category_signal(soup):
    categories_div = soup.find("div", id="mw-normal-catlinks")
    if not categories_div:
        return False

    category_text = categories_div.get_text(" ", strip=True).lower()

    positive_markers = [
        "association football",
        "men's footballers",
        "women's footballers",
        "international footballers",
        "f.c. players",
        "fc players",
    ]

    negative_markers = [
        "australian rules football",
        "gaelic football",
        "american football",
        "rugby",
        "hurling",
        "cricket",
    ]

    has_positive = any(marker in category_text for marker in positive_markers)
    has_negative = any(marker in category_text for marker in negative_markers)

    return has_positive and not has_negative


# Finali validator for importing only football players data
def looks_like_valid_player_page(url, soup, infobox_data):
    if is_invalid_url(url):
        return False

    if is_disambiguation_page(soup):
        return False

    if not infobox_data:
        return False

    return has_football_category_signal(soup)


# -----------------------------------------------------------------------------
# Generic HTML helpers
# -----------------------------------------------------------------------------

# Text cleanup like references [1], [a] and double spaces
def clean_text(value):
    if value is None:
        return None

    text = value.get_text(" ", strip=True) if hasattr(value, "get_text") else str(value)
    text = re.sub(r"\[[^\]]+\]", "", text)
    text = re.sub(r"\s+", " ", text).strip()

    return text or None


# get main glavni Wiki infobox
def get_infobox(soup):
    return soup.find("table", class_=lambda c: c and "infobox" in c)


# Parsing label/value pairs from infobox to dict
def parse_infobox(soup):
    infobox = get_infobox(soup)
    if not infobox:
        return {}

    data = {}

    for tr in infobox.find_all("tr"):
        th = tr.find("th")
        td = tr.find("td")

        if th and td:
            label = clean_text(th)
            value = clean_text(td)

            if label:
                data[label.lower()] = value

    return data


# Page name = player name
def extract_name(soup):
    title = soup.find("h1")
    return clean_text(title)


# -----------------------------------------------------------------------------
# Date parsing
# -----------------------------------------------------------------------------

# Date format to yyyy-mm-dd normalizing function
def normalize_date_string(value):
    if not value:
        return None

    value = clean_text(value)
    if not value:
        return None

    iso_match = re.search(r"\b(\d{4}-\d{2}-\d{2})\b", value)
    if iso_match:
        return iso_match.group(1)

    dot_match = re.search(r"\b(\d{1,2})\.(\d{1,2})\.(\d{4})\b", value)
    if dot_match:
        day, month, year = dot_match.groups()
        return f"{year}-{int(month):02d}-{int(day):02d}"

    text_match = re.search(r"\b(\d{1,2}\s+[A-Za-z]+\s+\d{4})\b", value)
    if text_match:
        candidate = text_match.group(1)
        for fmt in ("%d %B %Y", "%d %b %Y"):
            try:
                return datetime.strptime(candidate, fmt).date().isoformat()
            except ValueError:
                pass

    text_match = re.search(r"\b([A-Za-z]+\s+\d{1,2},\s+\d{4})\b", value)
    if text_match:
        candidate = text_match.group(1)
        for fmt in ("%B %d, %Y", "%b %d, %Y"):
            try:
                return datetime.strptime(candidate, fmt).date().isoformat()
            except ValueError:
                pass

    return None


# date_of_birth extraction from bday span or infobox fallback
def extract_birth_date(soup, infobox_data):
    bday = soup.find("span", class_="bday")
    if bday:
        normalized = normalize_date_string(clean_text(bday))
        if normalized:
            return normalized

    for key in ["date of birth", "born"]:
        value = infobox_data.get(key)
        if not value:
            continue

        normalized = normalize_date_string(value)
        if normalized:
            return normalized

    return None


# age extraction
def extract_age(infobox_data):
    for key in ["date of birth", "born"]:
        value = infobox_data.get(key)
        if not value:
            continue

        match = re.search(r"age\s+(\d+)", value.lower())
        if match:
            return int(match.group(1))

        match = re.search(r"\((?:[^)]*?)aged?\s+(\d+)(?:[^)]*?)\)", value.lower())
        if match:
            return int(match.group(1))

    return None


# date_of_death extraction from deathdate span or infobox fallback
def extract_date_of_death(soup, infobox_data):
    death_span = soup.find("span", class_="dday deathdate") or soup.find("span", class_="deathdate")
    if death_span:
        normalized = normalize_date_string(clean_text(death_span))
        if normalized:
            return normalized

    for key in ["died", "date of death"]:
        value = infobox_data.get(key)
        if not value:
            continue

        normalized = normalize_date_string(value)
        if normalized:
            return normalized

    return None


# -----------------------------------------------------------------------------
# Birthplace, positions, national team
# -----------------------------------------------------------------------------

#Parsing place anc country of birth from birth / born field
def extract_place_and_country(infobox_data):
     value = infobox_data.get("place of birth") or infobox_data.get("born")
     if not value:
         return None, None

     value = re.sub(r"\(.*?\)", "", value).strip(" ,")
     parts = [p.strip() for p in value.split(",") if p.strip()]

     if len(parts) >= 2:
         return ", ".join(parts[:-1]), parts[-1]

     return value, None


# Parsing positions from HTML links in infobox to keep multiple position separation
def extract_positions_from_html(soup):
    infobox = get_infobox(soup)
    if not infobox:
        return None

    for tr in infobox.find_all("tr"):
        th = tr.find("th")
        td = tr.find("td")

        if not th or not td:
            continue

        label = clean_text(th)
        if not label:
            continue

        if label.lower() in {"position", "position(s)"}:
            link_texts = []
            for link in td.find_all("a"):
                text = clean_text(link)
                if text:
                    link_texts.append(text)

            if link_texts:
                unique_positions = []
                for pos in link_texts:
                    if pos not in unique_positions:
                        unique_positions.append(pos)
                return ", ".join(unique_positions)

            raw_text = td.get_text(" | ", strip=True)
            raw_text = re.sub(r"\[\s*\d+\s*\]", "", raw_text)
            raw_text = re.sub(r"\s+", " ", raw_text).strip()
            return raw_text or None

    return None


# Extraction positions from HTML or infobox dict in case of faliure 
def extract_positions(soup, infobox_data):
    html_positions = extract_positions_from_html(soup)
    if html_positions:
        return html_positions

    for key in ["position(s)", "position"]:
        if key in infobox_data:
            return infobox_data[key]

    return None


# Parsing international career rows from infobox
def extract_international_career_rows(soup):
    infobox = get_infobox(soup)
    if not infobox:
        return []

    rows = infobox.find_all("tr")
    international_rows = []
    in_international_section = False

    stop_markers = [
        "managerial career",
        "teams managed",
        "medal record",
        "honours",
        "personal information",
        "club career",
        "senior career",
    ]

    for tr in rows:
        th = tr.find("th")
        header_text = clean_text(th) if th else None

        if header_text:
            normalized_header = header_text.lower().strip()

            if "international career" in normalized_header:
                in_international_section = True
                continue

            if in_international_section and any(marker in normalized_header for marker in stop_markers):
                break

        if not in_international_section:
            continue

        cells = tr.find_all(["th", "td"])
        cell_texts = []

        for cell in cells:
            text = clean_text(cell)
            if text:
                cell_texts.append(text)

        if cell_texts:
            international_rows.append(cell_texts)

    return international_rows


# National team name normalization, especially different youth formats like U20 UAE -> UAE U20
def normalize_national_team_name(value):
    if not value:
        return None

    value = clean_text(value)
    if not value:
        return None

    value = re.sub(r"\(\d+\)", "", value).strip()
    value = re.sub(r"\s+", " ", value).strip()
    value = re.sub(r"\bU\s*-\s*(\d+)\b", r"U\1", value, flags=re.IGNORECASE)
    value = re.sub(r"\bU\s+(\d+)\b", r"U\1", value, flags=re.IGNORECASE)

    match = re.match(r"^U(\d+)\s+(.+)$", value, flags=re.IGNORECASE)
    if match:
        age_group = match.group(1)
        country = match.group(2).strip()
        value = f"{country} U{age_group}"

    return value or None


# National team from flat field or international career section
def extract_national_team(soup, infobox_data):
    for key in ["national team", "nationalteam"]:
        if key in infobox_data and infobox_data[key]:
            return normalize_national_team_name(infobox_data[key])

    rows = extract_international_career_rows(soup)
    if not rows:
        return None

    senior_team = None
    youth_teams = []

    for row in rows:
        candidates = []

        for cell in row:
            cell_text = clean_text(cell)
            if not cell_text:
                continue

            lowered = cell_text.lower().strip()
            normalized = lowered.replace("–", "-").replace("—", "-").strip()

            skip_markers = [
                "club domestic league appearances",
                "national team caps and goals as of",
                "caps and goals as of",
                "appearances and goals as of",
                "as of ",
            ]
            if any(marker in lowered for marker in skip_markers):
                continue

            if re.fullmatch(r"\d{4}", normalized):
                continue
            if re.fullmatch(r"\d{4}\s*-\s*\d{0,4}", normalized):
                continue
            if re.fullmatch(r"\d+", normalized):
                continue
            if re.fullmatch(r"\(?\d+\)?", normalized):
                continue
            if not re.search(r"[A-Za-z]", cell_text):
                continue

            candidates.append(cell_text)

        if not candidates:
            continue

        team_name = normalize_national_team_name(candidates[0])
        if not team_name:
            continue

        if re.search(r"\bU\d+\b", team_name, flags=re.IGNORECASE):
            youth_teams.append(team_name)
        else:
            senior_team = team_name

    if senior_team:
        return senior_team

    if youth_teams:
        return youth_teams[-1]

    return None


# -----------------------------------------------------------------------------
# Club parsing and career rows
# -----------------------------------------------------------------------------

# Current team value cleanup, non-player roles deletion and loan additions
def clean_current_team_value(value):
    if not value:
        return None

    value = clean_text(value)
    if not value:
        return None

    lowered = value.lower()

    non_player_role_markers = [
        "sporting director",
        "manager",
        "assistant manager",
        "head coach",
        "coach",
        "goalkeeping coach",
        "director",
        "president",
        "chairman",
    ]

    if any(marker in lowered for marker in non_player_role_markers):
        return None

    value = re.sub(r"^\s*[→]+\s*", "", value)
    value = re.sub(r"\s*\(on loan from .*?\)", "", value, flags=re.IGNORECASE)
    value = re.sub(r"\s*\(loan\)\s*", "", value, flags=re.IGNORECASE)
    value = re.sub(r"\s+", " ", value).strip(" ,")

    return value or None


# Current team extraction from team info row in infobox
def extract_current_team_from_html(soup):
    infobox = get_infobox(soup)
    if not infobox:
        return None

    for tr in infobox.find_all("tr"):
        th = tr.find("th")
        td = tr.find("td")

        if not th or not td:
            continue

        label = clean_text(th)
        if not label:
            continue

        if label.lower() == "current team":
            return clean_text(td)

    return None


# Current team fallback / club from infobox dict
def extract_current_club(infobox_data):
    if infobox_data.get("current team"):
        return infobox_data.get("current team")

    if infobox_data.get("club"):
        return infobox_data.get("club")

    return None


# Recognizing an open range of years, e.g. 2018– or 2018-present
def is_open_ended_years(years_text):
    if not years_text:
        return False

    years_text = clean_text(years_text)
    if not years_text:
        return False

    normalized = years_text.lower().replace("–", "-").replace("—", "-").strip()

    if re.fullmatch(r"\d{4}\s*-\s*", normalized):
        return True

    if re.fullmatch(r"\d{4}\s*-\s*present", normalized):
        return True

    return False

def looks_like_years_value(value):
    if not value:
        return False

    value = clean_text(value)
    if not value:
        return False

    normalized = value.lower().replace("–", "-").replace("—", "-").strip()

    patterns = [
        r"^-?\d{4}$",                 # npr. -2006
        r"^\d{4}$",                   # npr. 2019
        r"^\d{4}\s*-\s*\d{4}$",       # npr. 2011-2018
        r"^\d{4}\s*-\s*$",            # npr. 2021-
        r"^\d{4}\s*-\s*present$",     # npr. 2021-present
    ]

    return any(re.fullmatch(pattern, normalized) for pattern in patterns)

# Parsing senior career lines from infobox
def extract_senior_career_rows(soup):
    infobox = get_infobox(soup)
    if not infobox:
        return []

    rows = infobox.find_all("tr")
    senior_rows = []
    in_senior_section = False

    stop_markers = [
        "international career",
        "managerial career",
        "teams managed",
        "medal record",
        "honours",
        "personal information",
        "team information",
        "youth career",
    ]

    for tr in rows:
        th = tr.find("th")
        header_text = clean_text(th) if th else None

        if header_text:
            normalized_header = header_text.lower().strip()

            if "senior career" in normalized_header:
                in_senior_section = True
                continue

            if in_senior_section and any(marker in normalized_header for marker in stop_markers):
                break

        if not in_senior_section:
            continue

        cells = tr.find_all(["th", "td"])
        cell_texts = []

        for cell in cells:
            text = clean_text(cell)
            if text:
                cell_texts.append(text)

        if len(cell_texts) < 2:
            continue

        senior_rows.append(cell_texts)

    return senior_rows

def has_year_based_senior_career_rows(soup):
    rows = extract_senior_career_rows(soup)
    if not rows:
        return False

    for row in rows:
        if len(row) < 2:
            continue

        if looks_like_years_value(row[0]):
            return True

    return False

# Returning the last meaningful line from the senior career section
def get_last_senior_career_row(soup):
    rows = extract_senior_career_rows(soup)
    if not rows:
        return None

    for row in reversed(rows):
        if len(row) >= 2:
            return row

    return None


# Returns all current clubs from the senior career section.
def extract_active_current_clubs_from_career(soup):
    rows = extract_senior_career_rows(soup)
    if not rows:
        return []

    active_clubs = []

    for row in rows:
        if len(row) < 2:
            continue

        years_text = row[0]
        team_text = row[1]

        if is_open_ended_years(years_text):
            cleaned_team = clean_current_team_value(team_text)
            if cleaned_team and cleaned_team not in active_clubs:
                active_clubs.append(cleaned_team)

    return active_clubs


# Normalizing club name for more reliable string comparison
def normalize_team_name(value):
    if not value:
        return None

    value = value.lower().strip()
    value = re.sub(r"\[\s*\d+\s*\]", "", value)
    value = re.sub(r"\s*\(loan\)\s*", "", value, flags=re.IGNORECASE)
    value = re.sub(r"\s*\(on loan.*?\)\s*", "", value, flags=re.IGNORECASE)
    value = re.sub(r"[^\w\s\-&.']", " ", value)
    value = re.sub(r"\s+", " ", value).strip()

    return value or None


# 2 clubs comparison after normalization
def clubs_match(club_a, club_b):
    if not club_a or not club_b:
        return False

    norm_a = normalize_team_name(club_a)
    norm_b = normalize_team_name(club_b)

    if not norm_a or not norm_b:
        return False

    return norm_a == norm_b


# -----------------------------------------------------------------------------
# Apps/goals parsing
# -----------------------------------------------------------------------------

# Parsing apps and goals from senior career row
def parse_apps_goals_from_row(cell_texts):
    apps = None
    goals = None

    cleaned_cells = []
    for text in cell_texts:
        if not text:
            continue

        cleaned = clean_text(text)
        if not cleaned:
            continue

        cleaned = cleaned.strip()
        cleaned_cells.append(cleaned)

        if apps is None and re.fullmatch(r"\d+", cleaned):
            apps = int(cleaned)

        goal_match = re.search(r"\(\s*(\d+)\s*\)", cleaned)
        if goal_match:
            goals = int(goal_match.group(1))

    joined = " ".join(cleaned_cells)
    joined = re.sub(r"\s+", " ", joined).strip()

    pair_match = re.search(r"\b(\d+)\s*\(\s*(\d+)\s*\)", joined)
    if pair_match:
        apps = int(pair_match.group(1))
        goals = int(pair_match.group(2))

    return apps, goals


# Returning apps/goals for the current club by finding the last matching row in the senior career section
def extract_current_club_stats(soup, current_team):
    if not current_team:
        return None, None

    rows = extract_senior_career_rows(soup)
    if not rows:
        return None, None

    current_team_normalized = normalize_team_name(current_team)
    if not current_team_normalized:
        return None, None

    matched_rows = []

    for row in rows:
        row_joined = " | ".join(row)
        normalized_row = normalize_team_name(row_joined)

        if not normalized_row:
            continue

        if current_team_normalized in normalized_row:
            matched_rows.append(row)

    if not matched_rows:
        return None, None

    last_row = matched_rows[-1]
    apps, goals = parse_apps_goals_from_row(last_row)

    return apps, goals


# -----------------------------------------------------------------------------
# Player parsing
# -----------------------------------------------------------------------------

# Parsing a single player from HTML and returns a normalized dict for insertion into the database
def parse_player(html, url):
    soup = BeautifulSoup(html, "lxml")
    infobox_data = parse_infobox(soup)

    if not looks_like_valid_player_page(url, soup, infobox_data):
        raise ValueError("Not a valid football player page")

    name = extract_name(soup)
    full_name = infobox_data.get("full name")
    date_of_birth = extract_birth_date(soup, infobox_data)
    date_of_death = extract_date_of_death(soup, infobox_data)
    age = extract_age(infobox_data)

    if date_of_death:
        age = None

    positions = extract_positions(soup, infobox_data)

    place_of_birth, country_of_birth = extract_place_and_country(infobox_data)

    infobox_current_club = clean_current_team_value(
        extract_current_team_from_html(soup) or extract_current_club(infobox_data)
    )
    active_career_clubs = extract_active_current_clubs_from_career(soup)
    has_senior_rows = has_year_based_senior_career_rows(soup)

    if infobox_current_club:
        if active_career_clubs:
            if infobox_current_club and any(clubs_match(infobox_current_club, club) for club in active_career_clubs):
                current_club = infobox_current_club
            else:
                current_club = active_career_clubs[-1]
        else:
            if has_senior_rows:
                current_club = None
            else:
                current_club = infobox_current_club
    else:
        if active_career_clubs:
            current_club = active_career_clubs[-1]
        else:
            current_club = None

    clear_current_club = current_club is None
    national_team = extract_national_team(soup, infobox_data)

    if current_club:
        current_club_appearances, current_club_goals = extract_current_club_stats(soup, current_club)
    else:
        current_club_appearances, current_club_goals = None, None

    is_deceased = bool(date_of_death)

    player = {
        "player_id": generate_player_id_from_url(url),
        "url": url,
        "name": name,
        "full_name": full_name,
        "date_of_birth": date_of_birth,
        "age": age,
        "place_of_birth": place_of_birth,
        "country_of_birth": country_of_birth,
        "positions": positions,
        "current_club": current_club,
        "national_team": national_team,
        "current_club_appearances": current_club_appearances,
        "current_club_goals": current_club_goals,
        "scraping_timestamp": datetime.now(timezone.utc).isoformat(),
        "_is_deceased": is_deceased,
    }

    player = normalize_player_record(player)
    player.pop("_is_deceased", None)

    return player, is_deceased, clear_current_club


# -----------------------------------------------------------------------------
# Database write helpers
# -----------------------------------------------------------------------------

# Insert/update player records by URL
def upsert_player(conn, player, is_deceased=False, clear_current_club=False):
    conn.execute(
        """
        insert into players (
            player_id, url, name, full_name, date_of_birth, age,
            place_of_birth, country_of_birth, positions, current_club,
            national_team, current_club_appearances, current_club_goals,
            scraping_timestamp
        )
        values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        on conflict(url) do update set
            player_id = coalesce(players.player_id, excluded.player_id),
            name = coalesce(excluded.name, players.name),
            full_name = coalesce(excluded.full_name, players.full_name),
            date_of_birth = coalesce(excluded.date_of_birth, players.date_of_birth),
            age = case
                when ? = 1 then null
                else coalesce(excluded.age, players.age)
            end,
            place_of_birth = coalesce(excluded.place_of_birth, players.place_of_birth),
            country_of_birth = coalesce(excluded.country_of_birth, players.country_of_birth),
            positions = coalesce(excluded.positions, players.positions),
            current_club = case
                when ? = 1 then null
                else coalesce(excluded.current_club, players.current_club)
            end,
            national_team = coalesce(excluded.national_team, players.national_team),
            current_club_appearances = case
                when ? = 1 then null
                else coalesce(excluded.current_club_appearances, players.current_club_appearances)
            end,
            current_club_goals = case
                when ? = 1 then null
                else coalesce(excluded.current_club_goals, players.current_club_goals)
            end,
            scraping_timestamp = coalesce(excluded.scraping_timestamp, players.scraping_timestamp)
        """,
        (
            player["player_id"],
            player["url"],
            player["name"],
            player["full_name"],
            player["date_of_birth"],
            player["age"],
            player["place_of_birth"],
            player["country_of_birth"],
            player["positions"],
            player["current_club"],
            player["national_team"],
            player["current_club_appearances"],
            player["current_club_goals"],
            player["scraping_timestamp"],
            1 if is_deceased else 0,
            1 if clear_current_club else 0,
            1 if clear_current_club else 0,
            1 if clear_current_club else 0,
        ),
    )


# Saving scraped output in CSV format
# def save_scraped_csv(players, path):
#     path.parent.mkdir(parents=True, exist_ok=True)

#     fieldnames = [
#         "player_id",
#         "url",
#         "name",
#         "full_name",
#         "date_of_birth",
#         "age",
#         "place_of_birth",
#         "country_of_birth",
#         "positions",
#         "current_club",
#         "national_team",
#         "current_club_appearances",
#         "current_club_goals",
#         "scraping_timestamp",
#     ]

#     with open(path, "w", encoding="utf-8", newline="") as f:
#         writer = csv.DictWriter(f, fieldnames=fieldnames)
#         writer.writeheader()
#         writer.writerows(players)


# -----------------------------------------------------------------------------
# Cleanup
# -----------------------------------------------------------------------------

# CLI argument for custom URLs file, if not specified, use default
def resolve_urls_path_from_cli():
    if len(sys.argv) > 1:
        return Path(sys.argv[1])
    return DEFAULT_URLS_PATH


# Deleting invalid rows that ended up in the database
def cleanup_invalid_rows(conn):
    conn.execute(
        """
        delete from players
        where
            lower(url) like '%/wiki/category:%'
            or lower(url) like '%/wiki/list_of_%'
            or lower(url) like '%disambiguation%'
            or (
                positions is null
                and current_club is null
                and national_team is null
                and date_of_birth is null
            )
        """
    )
    conn.commit()


# Deleting rows for URLs that were marked as invalid during scraping
def cleanup_invalid_player_urls(conn, invalid_urls):
    if not invalid_urls:
        return

    placeholders = ",".join("?" for _ in invalid_urls)

    conn.execute(
        f"""
        delete from players
        where url in ({placeholders})
        """,
        list(invalid_urls),
    )
    conn.commit()


# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------

# Main scraping pipeline: load URLs, scrape, upsert, cleanup, and output CSV
def main():
    conn = init_db()
    session = create_session()

    urls_path = resolve_urls_path_from_cli()
    urls_from_input = load_urls(urls_path)
    urls_from_players_data = load_urls_from_players_data(PLAYERS_DATA_PATH)
    urls = merge_url_lists(urls_from_input, urls_from_players_data)

    print(f"Loaded {len(urls_from_input)} URLs from {urls_path}")
    print(f"Loaded {len(urls_from_players_data)} URLs from {PLAYERS_DATA_PATH}")
    print(f"Total unique URLs to scrape: {len(urls)}")

    #scraped_players = []
    success_count = 0
    error_count = 0
    invalid_player_urls = set()

    for index, url in enumerate(urls, start=1):
        try:
            html = fetch_page(session, url)
            player, is_deceased, clear_current_club = parse_player(html, url)
            upsert_player(
                conn,
                player,
                is_deceased=is_deceased,
                clear_current_club=clear_current_club,
            )
            #scraped_players.append(player)
            success_count += 1
            print(f"[{index}/{len(urls)}] OK - {url}")
            time.sleep(0.1)

        except Exception as first_error:
            if "Not a valid football player page" in str(first_error):
                invalid_player_urls.add(url)

            print(f"[{index}/{len(urls)}] RETRY - {url} - {first_error}")
            time.sleep(1.0)

            try:
                html = fetch_page(session, url)
                player, is_deceased, clear_current_club = parse_player(html, url)
                upsert_player(
                    conn,
                    player,
                    is_deceased=is_deceased,
                    clear_current_club=clear_current_club,
                )
                #scraped_players.append(player)
                success_count += 1
                print(f"[{index}/{len(urls)}] OK AFTER RETRY - {url}")
                time.sleep(1.0)

            except Exception as second_error:
                if "Not a valid football player page" in str(second_error):
                    invalid_player_urls.add(url)

                error_count += 1
                print(f"[{index}/{len(urls)}] ERROR - {url} - {second_error}")
                time.sleep(1.0)

    conn.commit()
    cleanup_invalid_rows(conn)
    cleanup_invalid_player_urls(conn, invalid_player_urls)
    conn.close()

    #save_scraped_csv(scraped_players, OUTPUT_PATH)

    print(f"Scraped successfully: {success_count}")
    print(f"Errors: {error_count}")
    #print(f"Saved CSV to: {OUTPUT_PATH}")
    print(f"Updated database: {DB_PATH}")


if __name__ == "__main__":
    main()
