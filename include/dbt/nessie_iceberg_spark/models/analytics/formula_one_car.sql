{{
    config(
        materialized         = 'incremental',
        properties           = {
            "partitioning" : "ARRAY['team_name','full_name']",
        },
        incremental_strategy = 'append',
        on_schema_change     = 'sync_all_columns',
        views_enabled = false,
    )
}}

with source as (
select * from {{ source('iceberg_nessie', 'formula_one_car')}}
    {% if is_incremental() %}
      where date > (
        select max(date)
        from {{ this }}
      )
    {% endif %}
)


SELECT * from source 