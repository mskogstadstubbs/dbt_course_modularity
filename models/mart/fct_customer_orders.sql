with

orders as (
    select * from {{ ref('int_orders') }}
),

customers as (
    select * from {{ ref("stg_jaffle_shop__customers") }}
),

-- MARTS
customer_order_history as (

    select

        customers.customer_id,
        customers.surname,
        customers.givenname,
        customers.full_name,

        min(orders.order_date) as first_order_date,
        min(orders.valid_order_date) as first_non_returned_order_date,

        max(orders.valid_order_date) as most_recent_non_returned_order_date,

        coalesce(
            max(orders.user_order_seq), 0) 
        as order_count,

        coalesce(
            count(case 
                when orders.valid_order_date is not null
                then 1 
                end), 0
            ) as non_returned_order_count,

        sum(
            case
                when orders.valid_order_date is not null
                then orders.order_value_dollars
                else 0
            end
        ) as total_lifetime_value,

        sum(
            case
                when orders.valid_order_date is not null
                then orders.order_value_dollars
                else 0
            end
        ) 
        / 
        nullif(
            count(
                case
                    when orders.valid_order_date is not null
                    then 1
                end
            ),
            0
        ) as avg_non_returned_order_value,

        array_agg(distinct orders.order_id) as order_ids

    from orders

    join customers 
        on orders.customer_id = customers.customer_id

    group by
        customers.customer_id,
        customers.full_name,
        customers.surname,
        customers.givenname

),

-- Final CTEs 
final as (

    select

        orders.order_id,
        orders.customer_id,
        customers.surname,
        customers.givenname,
        first_order_date,
        order_count,
        total_lifetime_value,
        orders.order_value_dollars,
        orders.order_status,
        orders.payment_status

    from orders

    join customers 
        on orders.customer_id = customers.customer_id

    join customer_order_history
        on orders.customer_id = customer_order_history.customer_id

)

-- Simple Select Statement
select *
from final
