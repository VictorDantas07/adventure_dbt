
with 
    source_data as (
        select *
        from {{ source('raw_snowflake', 'PRODUCTMODEL') }}
    )

    , cast_and_rename_columns as (
        select 
            cast(productmodelid as number) as id_product_model
            , cast(rowguid as varchar) as id_rowgu
            , cast(instructions as varchar) as instructions
            , cast(catalogdescription as varchar) as catalog_description
            , cast(modifieddate as varchar) as modified_date
            , cast(name as varchar) as name_product_model
        from source_data
    )

select *
from cast_and_rename_columns


