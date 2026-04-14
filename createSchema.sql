create table if not exists players (
    player_id text,
    url text primary key,
    name text,
    full_name text,
    date_of_birth text,
    age integer,
    place_of_birth text,
    country_of_birth text,
    positions text,
    current_club text,
    national_team text,
    current_club_appearances integer,
    current_club_goals integer,
    scraping_timestamp text
);