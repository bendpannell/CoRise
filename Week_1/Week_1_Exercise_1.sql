*****************************************
Week 1 Exercise 1
*****************************************;

-- EDA on relevant tables
select * from vk_data.customers.customer_data limit 100;
select * from vk_data.customers.customer_address limit 100;
select * from vk_data.resources.us_cities limit 100;
select * from vk_data.suppliers.supplier_info limit 10;

-- Query construction

-- Update geography format in order to perform distance functions
alter session set geography_output_format='WKT';

-- Join customer data w/ address info and standardize city/state case for 
-- later joins.
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

-- Standardize city/state case for later joins.
supplier as (
    select 
        supplier_id,
        supplier_name,
        upper(supplier_city)    as city,
        upper(supplier_state)   as state
    from vk_data.suppliers.supplier_info
),

-- Standardize city/state case for later joins.
city_data as (
    select distinct
        upper(city_name)        as city,
        upper(state_abbr)       as state,
        geo_location
    from vk_data.resources.us_cities
),

-- Join customer info on city data to get geography info for distance functions
-- Trim city/state due to unwanted whitespace
customers_city as (
    select
        customer_address.*,
        city_data.geo_location
    from customer_address
    inner join
    city_data
    on trim(customer_address.city) = trim(city_data.city)
    and trim(customer_address.state) = trim(city_data.state)
),

-- Join supplier info on city data to get geography info for distance functions
-- Trim city/state due to unwanted whitespace
suppliers_city as (
    select
        supplier.*,
        city_data.geo_location
    from supplier
    inner join city_data
    on trim(supplier.city) = trim(city_data.city)
    and trim(supplier.state) = trim(city_data.state)
),

-- Cross join customers and city info and calculate distance between each, 
--  convert meters to miles.
supplier_cross as (
    select
        customer_id,
        first_name,
        last_name,
        email,
        supplier_id,
        supplier_name,
        (0.000621371 * (st_distance(
        to_geography(customers_city.geo_location),
        to_geography(suppliers_city.geo_location))))::number(6,2) as distance_miles,
        rank() over (partition by customer_id order by distance_miles) as rnk
    from customers_city
    cross join suppliers_city
),

-- Final query to put everything together
results as (
    select
        customer_id,
        first_name,
        last_name,
        email,
        supplier_id,
        supplier_name,
        distance_miles
    from supplier_cross
    where rnk = 1
    order by last_name, first_name
)

select * from results;