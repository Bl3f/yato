with source as (
    select *
    from raw_orders
),
renamed as (
    select ----------  ids
        id as order_id,
        store_id as location_id,
        customer as customer_id,
        ---------- numerics
        subtotal as subtotal_cents,
        tax_paid as tax_paid_cents,
        order_total as order_total_cents,
        cast(subtotal_cents as double) / 100.0 as subtotal,
        cast(tax_paid_cents as double) / 100.0 as tax_paid,
        cast(order_total_cents as double) / 100.0 as order_total,
        ---------- timestamps
        date_trunc('day', ordered_at) as ordered_at
    from source
)
select *
from renamed