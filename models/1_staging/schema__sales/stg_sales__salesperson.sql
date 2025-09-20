
with 
    source_data as (
        select *
        from {{ source('raw_snowflake', 'SALESPERSON') }}
    )

    , cast_and_rename_columns as (
        select 
            cast(salesquota as number) as sales_quota
            , cast(saleslastyear as float) as sales_last_year
            , cast(rowguid as varchar) as rowgu_id
            , cast(modifieddate as varchar) as modified_date
            , cast(salesytd as float) as sales_ytd
            , cast(territoryid as number) as territory_id
            , cast(commissionpct as float) as commission_pct
            , cast(businessentityid as number) as business_entity_id
            , cast(bonus as number) as bonus
            , current_timestamp as update_at
        from source_data
    )

select *
from cast_and_rename_columns
