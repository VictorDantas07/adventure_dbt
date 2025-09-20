
with 
    source_data as (
        select *
        from {{ source('raw_snowflake', 'BUSINESSENTITY') }}
    )

    , cast_and_rename_columns as (
        select 
            cast(rowguid as varchar) as id_rowgu
            , cast(businessentityid as number) as id_business_entity
            , cast(modifieddate as varchar) as modified_date
            , current_timestamp as update_at
        from source_data
    )

select *
from cast_and_rename_columns
