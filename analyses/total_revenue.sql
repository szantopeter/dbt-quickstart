select sum(payment_amount)
from {{ ref('stg_stripe__payments') }}
where payment_status = 'success';

select * from {{ ref('employees') }}