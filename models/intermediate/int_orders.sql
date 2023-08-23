with payments as (

    select 
        order_id, 
        max(payment_date) as payment_finalized_date, 
        sum(payment_amount) as total_amount_paid

    from {{ ref('stg_payments') }}
    group by 1

)

select 
    orders.*,

    min(order_placed_at) over (
        partition by customer_id
    ) as customer_first_order_date,

    max(order_placed_at) over (
        partition by customer_id
    ) as customer_most_recent_order_date,

    count(*) over (
        partition by customer_id
    ) as customer_number_of_orders,

    row_number() over (
        order by orders.order_id
    ) as transaction_seq,

    row_number() over (
        partition by customer_id 
        order by orders.order_id
    ) as customer_sales_seq,

    payments.payment_finalized_date,
    payments.total_amount_paid

from {{ ref('stg_orders') }} orders

left join payments on orders.order_id = payments.order_id
