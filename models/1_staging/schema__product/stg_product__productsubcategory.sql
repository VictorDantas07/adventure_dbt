
with 
    source_data as (
        select *
        from {{ source('raw_snowflake', 'PRODUCTSUBCATEGORY') }}
    )

    , cast_and_rename_columns as (
        select 
            cast(productsubcategoryid as number) as id_product_subcategory
            , cast(name as varchar) as name_product_subcategory
            , cast(productcategoryid as number) as id_product_category
            , cast(modifieddate as varchar) as modified_date
            , cast(rowguid as varchar) as id_rowgu
        from source_data
    )

select *
from cast_and_rename_columns
