/*++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
                        WEEK 4 EXERCISE - PART 1
++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
*/
/*  As a dbt user, I love bringing in my source tables one at a time (no joins)
    as it makes it very easy to determine lineage. This also allows any 
    transformation or filtering to take place, such as in the query below.
*/

/*  Select only the required data; apply filters, type-casting, and field
    re-naming to the next 3 sub-queries. Indent the renamed fields to 
    increase read-ability.
*/
with auto_customers as (
    select 
    	c_custkey::integer as customer_key
    from customer
    where c_mktsegment ilike 'AUTOMOBILE'
),

urgent_orders as (
	select
    	o_custkey::integer 	as customer_key,
        o_orderkey::integer	as order_key,
        o_orderdate::date	as order_date
    from orders
    where o_orderpriority ilike '1-URGENT'
),

line_items as (
	select
    	l_orderkey::integer 			as order_key,
        l_partkey::integer 				as part_key,
        l_quantity::integer				as quantity,
        l_extendedprice::number(10, 2)	as price
    from lineitem
),


/*  Break out the orders table to include the order line item data and apply a
    row_number function over the dataset. End the query by filtering the 
    results to only show the top 3 most expensive order line items. This table
    will be the basis for the remaining transformations.
*/

order_line_items as (
	select 
    	urgent_orders.customer_key,
    	urgent_orders.order_key,
        urgent_orders.order_date,
        line_items.part_key,
        line_items.quantity,
        line_items.price,
        row_number() over (partition by customer_key order by price desc) as price_rank
    from urgent_orders
    inner join line_items
    	on urgent_orders.order_key = line_items.order_key
    qualify price_rank <= 3
),

/*  Using the above table to start, aggregate the order summary information 
    defined in the problem statement.

    Using aggregates with a group by clause allows the customer_key to 
    occupy a single row as a result. Window functions break this pattern.
*/

order_summary as (
    select 
    	customer_key,
        max(order_date) as last_order_date,
        listagg(order_key, ', ') as order_numbers,
        sum(price) 	as total_spent
    from order_line_items
    group by customer_key
),

/*  Again using the order_line_items table, extract the interested information
    by self-joining and filtering each join. This query was very tricky (and I 
    used part 2 as directed inspiration), I really wanted to avoid using the 
    'distinct' keyword so many other approaches did not work. I tried extensive
    window functions (first_value(), nth_value(), etc.), case-when statements,
    array manipulation, etc. The method below returns all of the data with 
    customer_key on a single row as desired.
*/

part_breakdown as (
	select
    	oli.customer_key,
        oli.part_key 	as part_1_key,
        oli.quantity	as part_1_quantity,
        oli.price		as part_1_price,
        oli2.part_key	as part_2_key,
        oli2.quantity	as part_2_quantity,
        oli2.price		as part_2_price,
        oli3.part_key	as part_3_key,
        oli3.quantity	as part_3_quantity,
        oli3.price		as part_3_price

    from order_line_items as oli
    inner join order_line_items as oli2
    	on oli.customer_key = oli2.customer_key
    inner join order_line_items as oli3
    	on oli2.customer_key = oli3.customer_key
    where oli.price_rank = 1
    and oli2.price_rank = 2
    and oli3.price_rank = 3
),

/*  Since such care was taken to ensure the customer_key is unique in the 
    previous queries. This final query can consist of simple joins. Since we 
    did not account for the filtered customers in the previous queries, we must
    perform an inner join so that only customers that show up in both tables
    will be returned.
*/

results as (
	select
    	auto_customers.customer_key,
        order_summary.last_order_date,
        order_summary.order_numbers,
        order_summary.total_spent,
        part_breakdown.part_1_key,
        part_breakdown.part_1_quantity,
        part_breakdown.part_1_price,
        part_breakdown.part_2_key,
        part_breakdown.part_2_quantity,
        part_breakdown.part_2_price,
        part_breakdown.part_3_key,
        part_breakdown.part_3_quantity,
        part_breakdown.part_3_price
    
    from auto_customers
    inner join order_summary
    	on auto_customers.customer_key = order_summary.customer_key
    inner join part_breakdown
    	on auto_customers.customer_key = part_breakdown.customer_key
)

/*  Select everything from the final query and order/filter as desired. 
*/

select * from results
order by last_order_date desc
limit 100;

/*++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
                        WEEK 4 EXERCISE - PART 2
++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

Overall, I think the query produces the correct results in a very efficient and 
effective manner. After having worked through the same exercise above everything
makes sense. However, if I were seeing this for the very first time I would most
likely have difficulty following along because it is not very readable at first
glance. I like the use of CTEs but breaking up the code further would allow for 
more descriptive naming and more modular code. There are also no comments to 
describe the CTE subqueries, which makes it even more difficult to follow.

*/

