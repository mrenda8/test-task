.headers on
.mode column

with players_grouped as (
    select
        player_id,
        url,
        name,
        current_club,
        positions,
        age,
        current_club_appearances,
        case
            when lower(positions) like '%goalkeeper%' then 'goalkeeper'
            when lower(positions) like '%back%'
              or lower(positions) like '%defender%'
              or lower(positions) like '%wing-back%' then 'defender'
            when lower(positions) like '%midfielder%' then 'midfielder'
            when lower(positions) like '%winger%'
              or lower(positions) like '%striker%'
              or lower(positions) like '%forward%' then 'forward'
            else null
        end as position_group
    from players
)
select
    p1.name,
    p1.current_club,
    p1.positions,
    p1.position_group,
    p1.age,
    p1.current_club_appearances,
    count(p2.url) as younger_same_position_more_apps_count
from players_grouped p1
left join players_grouped p2
    on p2.url <> p1.url
   and p2.position_group = p1.position_group
   and p2.age < p1.age
   and p2.current_club_appearances > p1.current_club_appearances
where p1.current_club = 'Barcelona'
group by
    p1.name,
    p1.current_club,
    p1.positions,
    p1.position_group,
    p1.age,
    p1.current_club_appearances
order by younger_same_position_more_apps_count desc, p1.name asc;