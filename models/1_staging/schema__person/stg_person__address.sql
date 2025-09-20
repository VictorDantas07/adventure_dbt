
with 
    source_data as (
        select *
        from {{ source('raw_snowflake', 'ADDRESS') }}
    )

    , cast_and_rename_columns as (
        select 
            cast(postalcode as varchar) as code_postal
            , cast(addressline2 as varchar) as address_line_2
            , cast(city as varchar) as city
            , cast(rowguid as varchar) as rowguid
            , cast(modifieddate as varchar) as modified_date
            , cast(spatiallocation as varchar) as spatial_location
            , cast(addressline1 as varchar) as address_line_1
            , cast(addressid as number) as id_address
            , cast(stateprovinceid as number) as id_state_province
            , current_timestamp as update_at
        from source_data
    )

    , adding_address as (
        select 
            coalesce(address_line_1, '') || ' ' || coalesce(address_line_2, '') as address_line
            , code_postal
            , city
            , rowguid
            , modified_date
            , spatial_location
            , id_address
            , id_state_province
            , update_at
        from cast_and_rename_columns
    )

select *
from adding_address
