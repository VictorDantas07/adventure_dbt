
with 
    source_data as (
        select *
        from {{ source('raw_snowflake', 'SALESREASON') }}
    )

    , cast_and_rename_columns as (
        select 
            cast(reasontype as varchar) as reason_type
            , cast(name as varchar) as name_reason
            , cast(salesreasonid as number) as sales_reason_id
            , cast(modifieddate as varchar) as modified_date
            , current_timestamp as update_at
        from source_data
    )

select *
from cast_and_rename_columns
