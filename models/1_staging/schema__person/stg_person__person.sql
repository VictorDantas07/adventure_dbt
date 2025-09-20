
with 
    source_data as (
        select *
        from {{ source('raw_snowflake', 'PERSON') }}
    )

    , cast_and_rename_columns as (
        select 
            cast(demographics as varchar) as demographics
            , cast(businessentityid as number) as id_business_entity
            , cast(additionalcontactinfo as varchar) as additional_contact_info
            , cast(modifieddate as varchar) as modified_date
            , cast(firstname as varchar) as name_person
            , cast(middlename as varchar) as middle_name_person
            , cast(lastname as varchar) as last_name_person
            , cast(emailpromotion as number) as email_promotion
            , cast(rowguid as varchar) as id_rowgu
            , cast(suffix as varchar) as name_suffix
            , cast(persontype as varchar) as person_type
            , cast(namestyle as boolean) as name_style
            , cast(title as varchar) as title_person
            , current_timestamp as update_at
        from source_data
    )

    , adding_names as (
        select 
            replace(
                coalesce(name_person, '') || ' ' ||
                coalesce(middle_name_person, '') || ' ' ||
                coalesce(last_name_person, '') || ' ' ||
                coalesce(name_suffix, '')
            , '  ', ' ') as name_person
            , id_business_entity
            , demographics
            , additional_contact_info
            , modified_date
            , email_promotion
            , id_rowgu
            , person_type
            , name_style
            , title_person
            , update_at
        from cast_and_rename_columns
    )

select *
from adding_names
