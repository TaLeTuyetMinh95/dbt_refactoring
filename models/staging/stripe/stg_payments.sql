select 

    orderid as order_id, 
    created as payment_date, 
    amount / 100.0 as payment_amount

from {{ source('stripe', 'payment') }}
where status <> 'fail'