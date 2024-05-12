{{
    config(
        materialized         = 'incremental',
        incremental_strategy = 'merge',
        on_schema_change     = 'sync_all_columns',
        views_enabled        = false,
        unique_key           = ['team_name' , 'full_name', 'location','date','n_gear']
    )
}}

with source as (
select * from {{ ref('formula_one_car') }}
),

avg_speed_gear as (
SELECT team_name , full_name, location,date(date) as date, n_gear, avg(speed) as average_speed from source
group by  team_name , full_name, location,date(date), n_gear
order by 1 ,2 , 3, 4, 5
)

select * from avg_speed_gear