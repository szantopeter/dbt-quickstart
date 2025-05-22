with 

source as (

    select * from {{ source('stripe', 'payment') }}

),

transformed as (

  select

    id as payment_id,
    orderid as order_id,
    created as payment_created_at,
    status as payment_status,
    paymentmethod as payment_method,
    {{cents_to_dollars()}} as payment_amount

  from source

)

select * from transformed