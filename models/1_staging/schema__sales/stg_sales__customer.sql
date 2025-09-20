
with 
    source_data as (
        select *
        from {{ source('raw_snowflake', 'CUSTOMER') }}
    )

    , cast_and_rename_columns as (
        select 
            cast(storeid as number) as store_id
            , cast(modifieddate as varchar) as modified_date
            , cast(rowguid as varchar) as rowgu_id
            , cast(territoryid as number) as territory_id
            , cast(personid as number) as person_id
            , cast(customerid as number) as customer_id
            , current_timestamp as update_at
        from source_data
    )

select *
from cast_and_rename_columns
