
with 
    source_data as (
        select *
        from {{ source('raw_snowflake', 'STATEPROVINCE') }}
    )

    , cast_and_rename_columns as (
        select 
            cast(stateprovincecode as varchar) as code_state_province
            , cast(territoryid as number) as id_territory
            , cast(name as varchar) as name_state_province
            , cast(isonlystateprovinceflag as boolean) as is_only_state_province_flag
            , cast(stateprovinceid as number) as id_state_province
            , cast(modifieddate as varchar) as modified_date
            , cast(countryregioncode as varchar) as code_country_region
            , cast(rowguid as varchar) as id_rowgu
            , current_timestamp as update_at
        from source_data
    )

select *
from cast_and_rename_columns
