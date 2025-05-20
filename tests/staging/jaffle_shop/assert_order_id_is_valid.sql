SELECT customers.*
FROM {{ ref('stg_jaffle_shop__orders') }}
left join {{ ref('stg_jaffle_shop__customers') }} as customers using (customer_id)
WHERE customers.customer_id is null