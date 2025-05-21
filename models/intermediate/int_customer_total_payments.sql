with orders as ( 

    select * from {{ ref('stg_jaffle_shop__orders') }}

),

payments as (

    select * from {{ ref('stg_stripe__payments') }}

),

final as (

SELECT
  o.customer_id,
  SUM(p.payment_amount) AS total_payments
FROM orders o
LEFT JOIN payments p USING (order_id)
GROUP BY o.customer_id

)

select * from final
