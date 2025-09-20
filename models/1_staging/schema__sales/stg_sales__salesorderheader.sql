
with 
    source_data as (
        select *
        from {{ source('raw_snowflake', 'SALESORDERHEADER') }}
    )

    , cast_and_rename_columns as (
        select 
            cast(duedate as varchar) as due_date
            , cast(creditcardapprovalcode as varchar) as credit_card_approval_code
            , cast(accountnumber as varchar) as account_number
            , cast(customerid as number) as customer_id
            , cast(shipmethodid as number) as ship_method_id
            , cast(territoryid as number) as territory_id
            , cast(onlineorderflag as boolean) as is_online_order
            , cast(status as number) as status_order
            , cast(salespersonid as number) as salesperson_id
            , cast(subtotal as float) as subtotal
            , cast(totaldue as float) as total_due
            , cast(purchaseordernumber as varchar) as purchase_order_number
            , cast(creditcardid as number) as credit_card_id
            , cast(freight as float) as freight
            , cast(comment as number) as comment
            , cast(orderdate as varchar) as order_date
            , cast(billtoaddressid as number) as bill_to_address_id
            , cast(rowguid as varchar) as rowguid
            , cast(shipdate as varchar) as ship_date
            , cast(currencyrateid as number) as currency_rate_id
            , cast(revisionnumber as number) as revision_number
            , cast(taxamt as float) as tax_amt
            , cast(modifieddate as varchar) as modified_date
            , cast(shiptoaddressid as number) as ship_to_address_id
            , cast(salesorderid as number) as sales_order_id
            , current_timestamp as update_at
        from source_data
    )

select *
from cast_and_rename_columns
