{{
    config(
        materialized         = 'incremental',
        incremental_strategy = 'merge',
        on_schema_change     = 'sync_all_columns',
        views_enabled        = false,
        unique_key           = ['team_name' , 'full_name', 'location','date']
    )
}}

with source as (
select * from {{ ref('formula_one_car') }}
)


SELECT team_name , full_name, location,date(date) as date ,avg(speed) as average_speed from source
group by  team_name , full_name, location,date(date)
order by 1 ,2 , 3, date(date)


