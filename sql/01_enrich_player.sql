drop view if exists vw_players_enriched;

create view vw_players_enriched as
select
    player_id,
    url,
    name,
    full_name,
    date_of_birth,
    age,
    place_of_birth,
    country_of_birth,
    positions,
    current_club,
    national_team,
    current_club_appearances,
    current_club_goals,
    scraping_timestamp,
    case 
        when age <= 23 then 'Young'
        when age between 24 and 32 then 'MidAge'
        when age >= 33 then 'Old'
        else null
    end as AgeCategory,
    case
        when current_club_appearances is null or current_club_appearances = 0 then null
        when current_club_goals is null then null
        else cast(current_club_goals as real) / current_club_appearances
    end as GoalsPerClubGame
from players;

