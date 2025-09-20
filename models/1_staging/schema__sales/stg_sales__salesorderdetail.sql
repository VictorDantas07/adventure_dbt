
with 
    source_data as (
        select *
        from {{ source('raw_snowflake', 'SALESORDERDETAIL') }}
    )

    , cast_and_rename_columns as (
        select 
            cast(salesorderdetailid as number) as sales_order_detail_id
            , cast(unitpricediscount as float) as unit_price_discount
            , cast(productid as number) as product_id
            , cast(specialofferid as number) as special_offer_id
            , cast(modifieddate as varchar) as modified_date
            , cast(salesorderid as number) as salesorder_id
            , cast(rowguid as varchar) as rowgu_id
            , cast(unitprice as float) as unit_price
            , cast(carriertrackingnumber as varchar) as carrier_tracking_number
            , cast(orderqty as number) as qty_order
            , current_timestamp as update_at
        from source_data
    )

select *
from cast_and_rename_columns
