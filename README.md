Wikipedia Football Pages Scraper

This project implements a Python-based Wikipedia football player scraper.
It imports the provided CSV data into SQLite, enriches player records using scraped data, and provides SQL queries for further analysis.

--------------------------------------------------

PROJECT STRUCTURE

- importPlayers.py – imports initial data from playersData.csv into SQLite
- playersScraper.py – scrapes Wikipedia player pages and updates existing records by URL
- transform.py – normalization and cleanup logic for player records
- createSchema.sql – SQLite schema definition

- sql/01_enrich_player.sql – enrichment query/view
- sql/02_club_aggregates.sql – club-level aggregate metrics
- sql/03_club_player_comparison.sql – player comparison query

- data/raw/playersData.csv – initial dataset
- data/raw/playersURLs.csv – list of URLs to scrape

- presentation.pptx – optional summary presentation

--------------------------------------------------

SETUP

Recommended Python version: 3.10+

Install dependencies:
pip install -r requirements.txt

--------------------------------------------------

HOW TO RUN

1. Create the database and import initial data:
python importPlayers.py

2. Run the scraper:
python playersScraper.py data/raw/playersURLs.csv

Database will be created at:
data/db/players.sqlite

SQL QUERIES RUN:

1. Open database:
sqlite3 data/db/players.sqlite

2. (Optional for query 1) Enable headers for readability:
.headers on
.mode column

3. Run queries:

Query 1 
.read sql/01_enrich_player.sql
select * from vw_players_enriched;

Query 2 
.read sql/02_club_aggregates.sql

Query 3 
.read sql/03_club_player_comparison.sql


--------------------------------------------------

DATABASE SCHEMA

players table contains:
- player_id
- url
- name
- full_name
- date_of_birth
- age
- place_of_birth
- country_of_birth
- positions
- current_club
- national_team
- current_club_appearances
- current_club_goals
- scraping_timestamp

--------------------------------------------------

DATA PIPELINE

1. Import CSV into SQLite
2. Load URLs from both CSV files
3. Scrape Wikipedia pages
4. Normalize and clean data
5. Upsert records by URL
6. Remove invalid pages

--------------------------------------------------

DATA CLEANING & DESIGN

- filters non-player, category, list and disambiguation pages
- only association football players are included
- player_id generated deterministically from URL
- deceased players → age = NULL
- inactive players → current_club = NULL
- current_club resolved using infobox + career table
- loan strings normalized
- positions standardized
- date of birth normalized
- national teams normalized

--------------------------------------------------

KNOWN LIMITATIONS

- Wikipedia structure is inconsistent
- infobox and career data may conflict
- birthplace may contain historical values
- stats depend on page structure

--------------------------------------------------

SQL QUERIES

01_enrich_player.sql:
- AgeCategory
- GoalsPerClubGame

02_club_aggregates.sql:
- avg age
- avg appearances
- total players per club

03_club_player_comparison.sql:
- younger players
- same position
- more appearances

--------------------------------------------------

PRESENTATION

presentation.pptx includes:
- total players and clubs
- last update timestamp
- updated players count
- query results

--------------------------------------------------
