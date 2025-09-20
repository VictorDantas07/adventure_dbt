
with 
    source_data as (
        select *
        from {{ source('raw_snowflake', 'COUNTRYREGION') }}
    )

    , cast_and_rename_columns as (
        select 
            cast(name as varchar) as name_country
            , cast(countryregioncode as varchar) as code_country_region
            , cast(modifieddate as varchar) as modified_date
            , current_timestamp as update_at
        from source_data
    )

select *
from cast_and_rename_columns
