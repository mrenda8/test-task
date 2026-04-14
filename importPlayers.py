import csv
import sqlite3
from pathlib import Path

from transform import normalize_player_record

DB_PATH = Path("data/db/players.sqlite")
CSV_PATH = Path("data/raw/playersData.csv")
SCHEMA_PATH = Path("createSchema.sql")


#Base creation and scheme load from createSchema.sql.
def init_db():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)

    with open(SCHEMA_PATH, "r", encoding="utf-8") as f:
        conn.executescript(f.read())

    conn.commit()
    return conn


#Turning string value to integer. Empty or invalid returns None 
def to_int(value):
    if value is None:
        return None

    value = str(value).strip()
    if value == "":
        return None

    try:
        return int(value)
    except ValueError:
        return None


#Mapping one CSV row to internal player dict format format
def normalize_row(row):
    return {
        "player_id": row.get("PlayerID"),
        "url": row.get("URL"),
        "name": row.get("Name"),
        "full_name": row.get("Full name"),
        "date_of_birth": row.get("Date of birth"),
        "age": to_int(row.get("Age")),
        "place_of_birth": row.get("City of birth"),
        "country_of_birth": row.get("Country of birth"),
        "positions": row.get("Position"),
        "current_club": row.get("Current club"),
        "national_team": row.get("National_team"),
        "current_club_appearances": None,
        "current_club_goals": None,
        "scraping_timestamp": None,
    }


# Writing the player record to the database, on conflict by URL updates basic fields
def insert_player(conn, player):
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
            player_id = excluded.player_id,
            name = excluded.name,
            full_name = excluded.full_name,
            date_of_birth = excluded.date_of_birth,
            age = excluded.age,
            place_of_birth = excluded.place_of_birth,
            country_of_birth = excluded.country_of_birth,
            positions = excluded.positions,
            current_club = excluded.current_club,
            national_team = excluded.national_team
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
        ),
    )


# playersData.csv load, row normalization and writes them to the database 
def main():
    conn = init_db()
    count = 0

    with open(CSV_PATH, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f, delimiter=";")

        for row in reader:
            player = normalize_row(row)
            player = normalize_player_record(player)

            if not player["url"]:
                continue

            insert_player(conn, player)
            count += 1

    conn.commit()
    conn.close()

    print(f"Imported {count} rows into {DB_PATH}")


if __name__ == "__main__":
    main()