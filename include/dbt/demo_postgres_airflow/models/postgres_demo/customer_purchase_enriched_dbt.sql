{{
    config(
        materialized = 'table',
)
}}


select c.id, c.name, r.name as region, p.product, c.qty 
    from {{ ref('customer_purchases') }} c 
    join {{ref('regions')}} r on r.id = c.region 
    join {{ref('products_data')}} p on p.id = c.product