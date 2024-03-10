with source as (

    select
        od.order_id,
        od.product_id,
        o.employee_id,
        o.customer_id,
        o.shipper_id,
        od.inventory_id,
        od.purchase_order_id,
        od.status_id,
        od.discount,
        od.quantity,
        od.unit_price,
        date(o.order_date) as order_date,
        o.shipped_date,
        o.paid_date,
        od.date_allocated,
        current_timestamp() as insertion_timestamp,
    from {{ ref('stg_orders') }} o
    left join {{ ref('stg_orders_details') }} od
    on od.order_id = o.id
    where od.order_id is not null
),
unique_source as (

    select *,
            row_number() over(partition by customer_id, employee_id, order_id,product_id,shipper_id,purchase_order_id,shipper_id,order_date) as row_number
    from source
)

select *
except
(row_number),
from unique_source
where row_number = 1