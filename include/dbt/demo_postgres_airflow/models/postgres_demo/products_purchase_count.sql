{{
    config(
        materialized = 'table'
)
}}


with source as (
select product , count(*) as orders 
    from {{ref('customer_purchase_enriched_dbt')}}
    group by product 
)


SELECT * from source 