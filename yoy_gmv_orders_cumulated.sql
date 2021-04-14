WITH first_request AS (
    SELECT
         date(created_at) as created_at,
         SUM(gmv) AS gmv,
         0 as gmv_last_year,
         count(DISTINCT order_id) AS orders,
         0 as order_last_year
     FROM `selency-data-gold.raw_tables.order_report` order_report
     WHERE order_status = 'paid'
     AND order_product_status != 'UNAVAILABLE'
     GROUP BY 1,3,5

     UNION ALL

     SELECT
         date_add(DATE(created_at), INTERVAL 1 YEAR) as created_at,
         0 as gmv,
         SUM(gmv) AS gmv_last_year,
         0 as orders,
         count(DISTINCT order_id) AS order_last_year
     FROM `selency-data-gold.raw_tables.order_report` order_report
     WHERE order_status = 'paid'
     AND order_product_status != 'UNAVAILABLE'
     GROUP BY 1,2,4
)
SELECT
    created_at,
    sum(gmv) as gmv,
    sum(gmv_last_year) as gmv_last_year,
    sum(orders) as orders,
    sum(order_last_year) as orders_last_year
FROM first_request
GROUP BY 1
ORDER BY 1;
