
with 
    source_data as (
        select *
        from {{ source('raw_snowflake', 'STORE') }}
    )

    , cast_and_rename_columns as (
        select 
            cast(modifieddate as varchar) as modified_date
            , cast(salespersonid as number) as salesperson_id
            , cast(demographics as varchar) as demographics
            , cast(businessentityid as number) as business_entity_id
            , cast(name as varchar) as name_store
            , cast(rowguid as varchar) as rowgu_id
        from source_data
    )

select *
from cast_and_rename_columns
