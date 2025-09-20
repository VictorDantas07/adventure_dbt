
with 
    source_data as (
        select *
        from {{ source('raw_snowflake', 'SALESTERRITORY') }}
    )

    , cast_and_rename_columns as (
        select 
            cast(costlastyear as number) as cost_last_year
            , cast(modifieddate as timestamp) as modified_date
            , cast(territoryid as number) as territory_id
            , cast(costytd as number) as cost_ytd
            , cast(countryregioncode as varchar) as country_region_code
            , cast("group" as varchar) as group_sales_territory
            , cast(saleslastyear as varchar) as sales_last_year
            , cast(name as varchar) as name_sales_territory
            , cast(salesytd as varchar) as sales_ytd
            , cast(rowguid as varchar) as rowgu_id
        from source_data
    )

select *
from cast_and_rename_columns
