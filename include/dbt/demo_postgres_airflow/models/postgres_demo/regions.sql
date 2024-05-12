{{
    config(
        materialized = 'view'
)
}}


with source as (
select * from {{ source('postgres_local', 'region')}}
)


SELECT * from source 