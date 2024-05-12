{{
    config(
        materialized = 'view'
)
}}


with source as (
select * from {{ source('postgres_local', 'customer_purchase')}}
    {% if is_incremental() %}
      where id > (
        select max(id)
        from {{ this }}
      )
    {% endif %}
)


SELECT * from source 