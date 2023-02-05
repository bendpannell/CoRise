*****************************************
Week 1 Exercise 2
*****************************************;


-- EDA on relevant tables
select * from vk_data.customers.customer_data limit 100;
select * from vk_data.customers.customer_survey limit 100;
select * from vk_data.resources.recipe_tags limit 100;
select * from vk_data.chefs.recipe limit 100;

-- Query construction

-- Repeat of part 1 to find eligible customers
with customer_address as (
    select
        data.customer_id,
        data.first_name,
        data.last_name,
        data.email,
        address.address_id,
        upper(address.customer_city) as city,
        upper(address.customer_state) as state
    from vk_data.customers.customer_data as data
    left join vk_data.customers.customer_address as address
        on data.customer_id = address.customer_id
),

-- Repeat of part 1
city_data as (
    select distinct
        upper(city_name)        as city,
        upper(state_abbr)       as state
    from vk_data.resources.us_cities
),

-- Repeat of part 1
eligible_customers as (
    select
        *
    from customer_address
    inner join
    city_data
    on trim(customer_address.city) = trim(city_data.city)
    and trim(customer_address.state) = trim(city_data.state)
),

-- Find the top 3 tags for each customer by joining on customers who have 
-- completed the survey and decode the tags into usable information by 
-- joining with the recipe_tags table
customer_tags as (
    select
        eligible_customers.customer_id,
        eligible_customers.email,
        eligible_customers.first_name,
        tags.tag_property,
        rank() over (partition by eligible_customers.customer_id order by tags.tag_property) as rnk
    from eligible_customers
    inner join vk_data.customers.customer_survey as survey
        on eligible_customers.customer_id = survey.customer_id
    left join vk_data.resources.recipe_tags tags
        on survey.tag_id = tags.tag_id
),

-- Reorder tag properties into 1st, 2nd, and 3rd preferences. There has to be a
-- better way to get these tag preferences.
preferences as (
    select distinct
        customer_id,
        email,
        first_name,
        max(case
            when rnk = 1
                then trim(tag_property)
            else null
        end) over (partition by customer_id)  as pref_1,
        max(case
            when rnk = 2
                then trim(tag_property)
            else null
        end) over (partition by customer_id)    as pref_2,
        max(case
            when rnk = 3
                then trim(tag_property)
            else null
        end) over (partition by customer_id)     as pref_3
    from customer_tags
),

-- Reorder and trim the tag list values. Mostly just needed to trim each entry
-- in order to match to preferences table.
recipes as (
    select
        recipe_name,
        array_agg(trim(value)) within group (order by value) as sorted_tags
    from vk_data.chefs.recipe,
    lateral flatten(input => tag_list) group by recipe_name
    order by recipe_name
),

-- Final query to join the customer preferences table with the recipe table.
recipe_select as (
    select
        customer_id,
        email,
        first_name,
        pref_1,
        pref_2,
        pref_3,
        recipe_name
    from preferences
    left join recipes on (array_contains(preferences.pref_1::variant, recipes.sorted_tags))
    qualify rank() over (partition by customer_id order by recipe_name) = 1
)

select * from recipe_select
order by email;