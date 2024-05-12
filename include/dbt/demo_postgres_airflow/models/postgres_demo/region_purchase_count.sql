{{
    config(
        materialized = 'table'
)
}}


with source as (
select region , count(*) as orders 
    from {{ref('customer_purchase_enriched_dbt')}}
    group by region 
)


SELECT * from source 