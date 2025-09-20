
with 
    source_data as (
        select *
        from {{ source('raw_snowflake', 'CREDITCARD') }}
    )

    , cast_and_rename_columns as (
        select 
            cast(expyear as number) as exp_year
            , cast(cardtype as varchar) as card_type
            , cast(expmonth as number) as exp_month
            , cast(creditcardid as number) as credit_card_id
            , cast(cardnumber as number) as card_number
            , cast(modifieddate as varchar) as modified_date
            , current_timestamp as update_at
        from source_data
    )

select *
from cast_and_rename_columns
