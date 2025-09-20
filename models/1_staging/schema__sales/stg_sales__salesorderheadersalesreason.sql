
with 
    source_data as (
        select *
        from {{ source('raw_snowflake', 'SALESORDERHEADERSALESREASON') }}
    )

    , cast_and_rename_columns as (
        select 
            cast(salesreasonid as number) as sales_reason_id
            , cast(salesorderid as number) as sales_order_id
            , cast(modifieddate as varchar) as modified_date
            , current_timestamp as update_at
        from source_data
    )

select *
from cast_and_rename_columns
