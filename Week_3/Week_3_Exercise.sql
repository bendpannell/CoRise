
/* Identify only the fields that are used in the downstream queries. 
Similarly, perform as many transformation/aggregations as possible 
as early as possible. */
with source as (
	select 
    	event_id,
    	session_id,
        event_timestamp,
        date_trunc(day, event_timestamp)    as day,
        to_object(event_details::variant)   as details,
        details:event::varchar              as event,
        details:recipe_id::varchar          as recipe_id
    from vk_data.events.website_activity
),

/* Unique Sessions by day */
unique_sessions as (
	select
    	source.day,
        count(distinct session_id)			as session_count
    from source
    group by day
    order by day desc
),

/* Average Session Length 
    - Find the length of each session, 
    - then average them by day
*/
session_length as (
	select distinct
    	session_id,
    	min(event_timestamp) over (partition by session_id) as start_time,
        max(event_timestamp) over (partition by session_id) as end_time,
        timediff(second, start_time, end_time)              as session_length
    from source
    order by session_id
),

average_session_length as (
	select
    	date_trunc(day, start_time) as day,
        avg(session_length)         as avg_session_duration
    from session_length
    group by day
    order by day desc
),

/* Average Searches before recipe display
	- Find sessions with a `view_recipe` event
    - Then count the number of `search` events that from those that
      viewed a recipe
    - Average the counts by day
*/
sessions_view_recipe as (
	select
    	session_id,
        day,
        event
    from source
    where event ilike 'view_recipe'
),

search_counts as (
	select
    	source.event_id,
    	source.session_id,
        source.day,
        source.event,
        count(distinct case 
        	when source.event ilike 'search'
        		then event_id
            else null
        end) over (partition by source.session_id) as cnt
    from source
    inner join sessions_view_recipe
    	on source.session_id = sessions_view_recipe.session_id
),

average_search_count as (
	select
    	day,
        avg(cnt) as avg_search_count
    from search_counts
    group by day
    order by day
),

/* Count the number of recipe views per recipe and return the 
    one with the highest count
    */
recipe_counts as (
	select
    	day,
        recipe_id,
        count(recipe_id) as recipe_count
        
    from source
    where event ilike 'view_recipe'
    group by day, recipe_id
    qualify row_number() over (partition by day order by recipe_count desc) = 1
)

select distinct
	source.day,
    unique_sessions.session_count,
    average_session_length.avg_session_duration,
    average_search_count.avg_search_count,
    recipe_counts.recipe_id
from source
left join unique_sessions
	on source.day = unique_sessions.day
left join average_session_length
	on source.day = average_session_length.day
left join average_search_count
	on source.day = average_search_count.day
left join recipe_counts
	on source.day = recipe_counts.day
    order by source.day;

/* Based on the query profile, the `distinct` and `joins` are the most expensive
nodes. I attempted to perform a lot of the aggregation and transformation 
as early as possible to prevent repeating myself. In order to further optizime
I would extract the date columns to avoid left joins and prevent missing 
information. I would also resolve the select distinct from the final query.
*/