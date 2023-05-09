with 

base_orders as (
    
    select * 
    
    from {{ source("jaffle_shop", "orders") }}
    
),

orders as (
    select
        
        user_id as customer_id,
        order_date,
        id as order_id,
        status as order_status,

        case
            when status not in ('returned', 'return_pending')
            then order_date 
        end as valid_order_date,
        
        row_number() over (
            partition by user_id order by order_date, id
        ) as user_order_seq,

    from base_orders

)

select * from orders