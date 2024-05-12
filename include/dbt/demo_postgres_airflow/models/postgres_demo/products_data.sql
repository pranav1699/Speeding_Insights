{{
    config(
        materialized = 'view'
)
}}


with source as (
select * from {{ source('postgres_local', 'products')}}
)


SELECT * from source 