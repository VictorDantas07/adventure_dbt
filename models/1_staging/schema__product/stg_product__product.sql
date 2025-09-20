
with 
    source_data as (
        select *
        from {{ source('raw_snowflake','PRODUCT') }}
    )

    , cast_and_rename_columns as (
        select 
            cast(weightunitmeasurecode as varchar) as code_weight_unit_measure
            , cast(modifieddate as varchar) as modified_date
            , cast(makeflag as boolean) as make_flag
            , cast(discontinueddate as number) as discontinued_date
            , cast(finishedgoodsflag as boolean) as finished_goods_flag
            , cast(sellstartdate as varchar) as sell_start_date
            , cast(rowguid as varchar) as id_rowgu
            , cast(daystomanufacture as number) as days_to_manufacture
            , cast(productmodelid as number) as id_product_model
            , cast(size as varchar) as size_product
            , cast(weight as float) as weight_product
            , cast(reorderpoint as number) as reorder_point
            , cast(listprice as float) as list_price
            , cast(standardcost as float) as standard_cost
            , cast(sizeunitmeasurecode as varchar) as code_size_unit_measure
            , cast(color as varchar) as color_product
            , cast(style as varchar) as style_product
            , cast(class as varchar) as class_product
            , cast(productid as number) as id_product
            , cast(safetystocklevel as number) as safety_stock_level
            , cast(sellenddate as varchar) as sellend_date
            , cast(productline as varchar) as product_line
            , cast(name as varchar) as name_product
            , cast(productnumber as varchar) as product_number
            , cast(productsubcategoryid as number) as id_product_subcategory
        from source_data
    )

select *
from cast_and_rename_columns
