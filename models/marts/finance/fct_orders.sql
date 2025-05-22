{{
    config(
        materialized='incremental',
        unique_key='order_id',
        incremental_strategy = 'merge',
        on_schema_change = 'sync_all_columns'
    )
}}

with orders as ( 

    select * from {{ ref('stg_jaffle_shop__orders') }}

),

payments as (

    select
        order_id,
        sum (case when payment_status = 'success' then payment_amount end) as amount

    from {{ ref('stg_stripe__payments') }}
    group by order_id

),

final as (

    select
        orders.order_id,
        orders.customer_id,
        coalesce (payments.amount, 0) as amount,
        order_placed_at
    from orders
    left join payments using (order_id)

)

select * from final
{% if is_incremental() %}
   
where order_placed_at >= (select max(order_placed_at) from {{ this }})

{% endif %}