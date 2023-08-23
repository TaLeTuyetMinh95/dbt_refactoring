with 
orders as (
    
    select *
    
    from {{ ref('int_orders') }}
),

customers as (

    select *

    from {{ ref('stg_customers') }}

),

lifetime_value as (

    select

        o1.order_id,
        sum(o2.total_amount_paid) as clv_bad

    from orders o1
    left join orders o2 on o1.customer_id = o2.customer_id and o1.order_id >= o2.order_id
    group by 1

), 

customer_orders as (
    select 

        orders.order_id,
        orders.customer_id,
        orders.order_placed_at,
        orders.order_status,
        orders.transaction_seq,
        orders.customer_sales_seq,
        orders.total_amount_paid,
        orders.payment_finalized_date,
        orders.customer_first_order_date as fdos,
        customers.customer_first_name,
        customers.customer_last_name,
        case 
            when orders.customer_first_order_date = orders.order_placed_at
            then 'new'
        else 'return' end as nvsr,
        lifetime_value.clv_bad as customer_lifetime_value

    from orders
    left join customers on orders.customer_id = customers.customer_id 
    left join lifetime_value on orders.order_id = lifetime_value.order_id
)

select
    order_id,
    customer_id,
    order_placed_at,
    order_status,
    customer_first_name,
    customer_last_name,
    transaction_seq,
    customer_sales_seq,
    nvsr,
    customer_lifetime_value,
    fdos
from customer_orders
order by order_id