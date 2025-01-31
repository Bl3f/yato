with source as (
    select *
    from raw_supplies
),
renamed as (
    select ----------  ids
        id || '_' || sku as supply_uuid,
        id as supply_id,
        sku as product_id,
        ---------- text
        name as supply_name,
        ---------- numerics
        cast(cost as double) / 100.0 as supply_cost,
        ---------- booleans
        perishable as is_perishable_supply
    from source
)
select *
from renamed