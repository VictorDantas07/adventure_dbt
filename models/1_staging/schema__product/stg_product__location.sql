
with 
    source_data as (
        select *
        from {{ source('raw_snowflake', 'LOCATION') }}
    )

    , cast_and_rename_columns as (
        select 
            cast("costrate" as float) as costrate
            , cast("name" as varchar) as name_location
            , cast("availability" as number) as availability_location
            , cast("locationid" as number) as id_location
            , cast("modifieddate" as varchar) as modified_date
            , current_timestamp as update_at
        from source_data
    )

select *
from cast_and_rename_columns
