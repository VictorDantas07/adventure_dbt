
with 
    source_data as (
        select *
        from {{ source('raw_snowflake', 'PRODUCTCOSTHISTORY') }}
    )

    , cast_and_rename_columns as (
        select 
            cast(enddate as varchar) as end_date
            , cast(startdate as varchar) as start_date
            , cast(productid as number) as id_product
            , cast(modifieddate as varchar) as modified_date
            , cast(standardcost as float) as standard_cost
        from source_data
    )

select *
from cast_and_rename_columns
