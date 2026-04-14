.headers on
.mode column

select
    current_club,
    round(avg(age), 2) as avg_age,
    round(avg(current_club_appearances), 2) as avg_appearances,
    count(*) as total_players
from players
where current_club is not null
group by current_club
order by total_players desc, current_club asc;