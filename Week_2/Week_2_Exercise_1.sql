/* Week 2 */


/* I like start my CTEs with a select statement from a single table and extract 
only the data I need to carry through. This allows any simple transformations 
to occur first and everytime I use the data later in the CTE, I won't have to 
transform anything. I also like to end my CTEs with a `select * from` my final 
table. */

/* Extract customer id and concatenate first/last name from customer_data. */
with customers as (
    select 
        customer_id,
        first_name || ' ' || last_name as customer_name
        
    from vk_data.customers.customer_data
),

/* Extract and transform customer id and customer city and state for joining. */
addresses as (
    select
        customer_id,
        trim(initcap(customer_city))  as customer_city,
        trim(upper(customer_state))   as customer_state
    from vk_data.customers.customer_address
),

/* Extract all data from survey table */
surveys as (
    select * from vk_data.customers.customer_survey
),

/* Extract and transform city data */
cities as (
    select 
        trim(initcap(city_name))   as city_name,
        trim(upper(state_abbr))    as state_abbr,
        geo_location
    from vk_data.resources.us_cities
),

/* Extract and aggregate food preference data */
food_pref_counts as (
    select 
        customer_id,
        count(*) as food_pref_count
    from surveys
    where is_active = true
    group by customer_id
),

/* Extract geo-location for Chicago. Notice city name and state abbreviation are
in the format I transformed to earlier in the CTE. */
chicago as (
    select 
        geo_location
    from cities
    where city_name = 'Chicago' and state_abbr = 'IL'
   
),

/* Extract geo-location for Gary. Notice city name and state abbreviation are
in the format I transformed to earlier in the CTE. */
gary as (
    select 
        geo_location
    from cities 
    where city_name = 'Gary' and state_abbr = 'IN'
),

/* Combine and join all relevant information. I prefer performing all joins in 
one place when it can be done cleanly so I can easily identify the source of
all of the data in the final query. Also the where clause has be updated to
reflect the original query. */
results as (
    
    select 
        customers.customer_name,
        addresses.customer_city,
        addresses.customer_state,
        food_pref_counts.food_pref_count,
        (st_distance(cities.geo_location, chicago.geo_location) / 1609)::int as chicago_distance_miles,
        (st_distance(cities.geo_location, gary.geo_location) / 1609)::int as gary_distance_miles
    from addresses
    inner join customers 
        on addresses.customer_id = customers.customer_id
    left join cities
        on addresses.customer_state = cities.state_abbr
        and addresses.customer_city = cities.city_name
    inner join food_pref_counts 
        on addresses.customer_id = food_pref_counts.customer_id
    cross join 
        chicago
    cross join 
        gary
    where 
        (cities.city_name in ('Concord', 'Georgetown', 'Ashland') and customer_state = 'KY')
        or (cities.city_name in ('Oakland', 'Pleasant Hill') and customer_state = 'CA')
        or (cities.city_name ilike 'Arlington' and customer_state = 'TX')
        or cities.city_name ilike 'Brownsville'
)

/* Select all rows from the final query. */
select * from results;