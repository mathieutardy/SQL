WITH issues_last_exact_date AS ( 
 SELECT DISTINCT
  issues_history.issues_id,
  LAST_VALUE(CASE WHEN issues_history.new_value = '' THEN issues_history.old_value ELSE issues_history.new_value END) OVER (PARTITION BY issues_history.issues_id ORDER BY issues_history.date ASC ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS last_exact_date
FROM `selency-data-gold.shipping.issues_history` issues_history
WHERE issues_history.updated_field = 'Exact date'
ORDER BY 1
 
), all_shippings AS (

SELECT DISTINCT

   DATE(order_report.created_at) AS created_at,
   pickup_date.pickup_date,
   delivery_date.delivery_date,
   order_report.order_number,
   order_report.order_product_id,
   order_report.product_sku,
   issues_delivery.issues_id AS delivery_issues_id,
   issues_pickup.issues_id AS pickup_issues_id,

   CASE WHEN issues_delivery.shipper_login IS NULL THEN 'UNASSIGNED' ELSE issues_delivery.shipper_login END AS shipper_delivery,
   CASE WHEN issues_pickup.shipper_login IS NULL THEN 'UNASSIGNED' ELSE issues_pickup.shipper_login END AS shipper_pickup,
   issues_delivery.shipping_status AS delivery_shipping_status,
   issues_pickup.shipping_status AS pickup_shipping_status,
   ROUND(DATE_DIFF(pickup_date.pickup_date, DATE(order_report.created_at), DAY)/7,1) AS pickup_time,
   ROUND(DATE_DIFF(delivery_date.delivery_date, DATE(order_report.created_at), DAY)/7,1) AS delivery_time,
    
   
FROM `selency-data-gold.shipping.issues` issues_pickup
INNER JOIN `selency-data-gold.shipping.issues` issues_delivery
ON (issues_pickup.ordernumber = issues_delivery.ordernumber  AND issues_pickup.sku = issues_delivery.sku  AND issues_pickup.shipping_type = 'pickup' AND issues_delivery.shipping_type = 'livraison')
LEFT JOIN `selency-data-gold.shipping.issues_details` issues_details_pickup
ON issues_pickup.issues_id = issues_details_pickup.issues_id
LEFT JOIN `selency-data-gold.shipping.issues_details` issues_details_delivery
ON issues_delivery.issues_id = issues_details_delivery.issues_id
LEFT JOIN 
(
SELECT
  issues.issues_id,
  CASE WHEN issues_last_exact_date.last_exact_date IS NULL THEN DATE_SUB(LAST_VALUE(DATE(issues_history.date)) OVER (PARTITION BY issues.issues_id ORDER BY issues_history.date ASC ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING),INTERVAL 3 DAY) ELSE DATE(TIMESTAMP(issues_last_exact_date.last_exact_date)) END AS pickup_date
FROM `selency-data-gold.shipping.issues` issues 
LEFT JOIN `selency-data-gold.shipping.issues_history` issues_history ON issues_history.issues_id = issues.issues_id
LEFT JOIN issues_last_exact_date ON issues_last_exact_date.issues_id = issues.issues_id
) pickup_date
ON issues_pickup.issues_id = pickup_date.issues_id
LEFT JOIN 
(
SELECT
  issues.issues_id,
  CASE WHEN issues_last_exact_date.last_exact_date IS NULL THEN DATE_SUB(LAST_VALUE(DATE(issues_history.date)) OVER (PARTITION BY issues.issues_id ORDER BY issues_history.date ASC ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING),INTERVAL 3 DAY) ELSE DATE(TIMESTAMP(issues_last_exact_date.last_exact_date)) END AS delivery_date
FROM `selency-data-gold.shipping.issues` issues 
LEFT JOIN `selency-data-gold.shipping.issues_history` issues_history ON issues_history.issues_id = issues.issues_id
LEFT JOIN issues_last_exact_date ON issues_last_exact_date.issues_id = issues.issues_id
) delivery_date
ON issues_delivery.issues_id = delivery_date.issues_id
INNER JOIN `selency-data-gold.raw_tables.order_report_hourly` order_report
ON (order_report.product_sku = issues_delivery.sku AND issues_delivery.ordernumber = order_report.order_number)
-- POST CANCEL
WHERE order_report.order_status = 'paid'
AND order_report.delivery_provider = 'BROCANTE_LAB'
AND order_report.order_product_status != 'UNAVAILABLE'
AND DATE_DIFF(delivery_date.delivery_date, DATE(order_report.created_at), DAY)/7 BETWEEN 0 AND 20
ORDER BY 1,5

), median_shipping_time AS (

SELECT DISTINCT

  delivery_date, 
  DATE_TRUNC(delivery_date,WEEK(SATURDAY)) AS start_week,
  DATE_TRUNC(delivery_date,MONTH) AS start_month,
  
  -- MEDIAN DELIVERY AND PICKUP
   PERCENTILE_CONT(pickup_time, 0.5) OVER(PARTITION BY delivery_date) AS median_pickup_daily,
   PERCENTILE_CONT(delivery_time, 0.5) OVER(PARTITION BY delivery_date) AS median_delivery_daily,
   
   PERCENTILE_CONT(pickup_time, 0.5) OVER(PARTITION BY DATE_TRUNC(delivery_date,WEEK(SATURDAY))) AS median_pickup_weekly,
   PERCENTILE_CONT(delivery_time, 0.5) OVER(PARTITION BY DATE_TRUNC(delivery_date,WEEK(SATURDAY))) AS median_delivery_weekly,
   
   PERCENTILE_CONT(pickup_time, 0.5) OVER(PARTITION BY DATE_TRUNC(delivery_date,MONTH)) AS median_pickup_monthly,
   PERCENTILE_CONT(delivery_time, 0.5) OVER(PARTITION BY DATE_TRUNC(delivery_date,MONTH)) AS median_delivery_monthly,

FROM all_shippings
WHERE delivery_shipping_status IN ('done','archived')
AND pickup_shipping_status IN ('done','archived')

)

SELECT
  
  -- DATES
  all_shippings.delivery_date,
  DATE_TRUNC(all_shippings.delivery_date, WEEK(SATURDAY)) AS start_week,
  DATE_TRUNC(all_shippings.delivery_date, MONTH) AS start_month,
  
  ----------------------------------------------------------------------------------------------------------------------------------------------------------------
  -- METRICS -----------------------------------------------------------------------------------------------------------------------------------------------------
  ----------------------------------------------------------------------------------------------------------------------------------------------------------------
    
  -- GENERAL
   COUNT(DISTINCT CASE WHEN  all_shippings.delivery_shipping_status IN ('done','archived') AND all_shippings.pickup_shipping_status IN ('done','archived') THEN all_shippings.order_product_id END ) AS nb_achieved_shippings,
   COUNT( CASE WHEN  all_shippings.delivery_shipping_status IN ('done','archived') AND all_shippings.pickup_shipping_status IN ('done','archived') THEN all_shippings.order_product_id END ) AS nb_achieved_shippings_not_distinct,
    COUNT(DISTINCT all_shippings.order_product_id) AS nb_shippings,
  
  -- DELIVERY/PICKUP TIME
  SUM( CASE WHEN  (all_shippings.delivery_shipping_status IN ('done','archived') AND all_shippings.pickup_shipping_status IN ('done','archived')) THEN 
all_shippings.pickup_time END) AS cumulated_pickup_time,
  SUM( CASE WHEN (all_shippings.delivery_shipping_status IN ('done','archived') AND all_shippings.pickup_shipping_status IN ('done','archived')) THEN
all_shippings.delivery_time END) AS cumulated_delivery_time,
  COUNT(DISTINCT CASE WHEN all_shippings.delivery_time >= 5 AND all_shippings.delivery_shipping_status IN ('done','archived') AND all_shippings.pickup_shipping_status IN ('done','archived') THEN all_shippings.order_product_id END) AS nb_deliveries_where_delivery_time_more_equal_5weeks,
  COUNT(DISTINCT CASE WHEN all_shippings.delivery_time >= 6 AND all_shippings.delivery_shipping_status IN ('done','archived') AND all_shippings.pickup_shipping_status IN ('done','archived') THEN all_shippings.order_product_id END) AS nb_deliveries_where_delivery_time_more_equal_6weeks,
  
  median_shipping_time.median_pickup_daily,
  median_shipping_time.median_pickup_weekly,
  median_shipping_time.median_pickup_monthly,
  median_shipping_time.median_delivery_daily,
  median_shipping_time.median_delivery_weekly,
  median_shipping_time.median_delivery_monthly,
   
   -- DELIVERY/PICKUP TIME BUCKETS
   COUNT (DISTINCT CASE WHEN all_shippings.delivery_time > 7 AND all_shippings.delivery_shipping_status IN ('done','archived') AND all_shippings.pickup_shipping_status IN ('done','archived') THEN all_shippings.order_product_id END) AS  nb_deliveries_where_delivery_time_more_7weeks,
   COUNT (DISTINCT CASE WHEN all_shippings.delivery_time > 6 AND all_shippings.delivery_time <= 7 AND all_shippings.delivery_shipping_status IN ('done','archived') AND all_shippings.pickup_shipping_status IN ('done','archived') THEN all_shippings.order_product_id END) AS  nb_deliveries_where_delivery_time_weeks_6_excluded_7_included,
   COUNT (DISTINCT CASE WHEN all_shippings.delivery_time > 5 AND all_shippings.delivery_time <= 6 AND all_shippings.delivery_shipping_status IN ('done','archived') AND all_shippings.pickup_shipping_status IN ('done','archived') THEN all_shippings.order_product_id END) AS  nb_deliveries_where_delivery_time_weeks_5_excluded_6_included,
   COUNT (DISTINCT CASE WHEN all_shippings.delivery_time > 4 AND all_shippings.delivery_time <= 5 AND all_shippings.delivery_shipping_status IN ('done','archived') AND all_shippings.pickup_shipping_status IN ('done','archived') THEN all_shippings.order_product_id END) AS  nb_deliveries_where_delivery_time_weeks_4_excluded_5_included,
   COUNT (DISTINCT CASE WHEN all_shippings.delivery_time > 3 AND all_shippings.delivery_time <= 4 AND all_shippings.delivery_shipping_status IN ('done','archived') AND all_shippings.pickup_shipping_status IN ('done','archived') THEN all_shippings.order_product_id END) AS  nb_deliveries_where_delivery_time_weeks_3_excluded_4_included,
   COUNT (DISTINCT CASE WHEN all_shippings.delivery_time <= 3 AND all_shippings.delivery_shipping_status IN ('done','archived') AND all_shippings.pickup_shipping_status IN ('done','archived') THEN all_shippings.order_product_id END) AS  nb_deliveries_where_delivery_time_less_equal_3_weeks,
   
   -- LATE DELIVERIES/PICKUPS
 COUNT (DISTINCT CASE WHEN all_shippings.pickup_time > 5 AND all_shippings.delivery_shipping_status IN ('done','archived') AND all_shippings.pickup_shipping_status IN ('done','archived') THEN all_shippings.order_product_id END) AS  nb_deliveries_where_pickup_time_more_5weeks,
   COUNT (DISTINCT CASE WHEN all_shippings.pickup_time > 4 AND all_shippings.pickup_time <= 5 AND all_shippings.delivery_shipping_status IN ('done','archived') AND all_shippings.pickup_shipping_status IN ('done','archived') THEN all_shippings.order_product_id END) AS  nb_pickup_where_pickup_time_weeks_4_excluded_5_included,
   COUNT (DISTINCT CASE WHEN all_shippings.pickup_time > 3 AND all_shippings.pickup_time <= 4 AND all_shippings.delivery_shipping_status IN ('done','archived') AND all_shippings.pickup_shipping_status IN ('done','archived') THEN all_shippings.order_product_id END) AS  nb_pickup_where_pickup_time_weeks_3_excluded_4_included,
   COUNT (DISTINCT CASE WHEN all_shippings.pickup_time > 2 AND all_shippings.pickup_time <= 3 AND all_shippings.delivery_shipping_status IN ('done','archived') AND all_shippings.pickup_shipping_status IN ('done','archived') THEN all_shippings.order_product_id END) AS  nb_pickup_where_pickup_time_weeks_2_excluded_3_included,
   COUNT (DISTINCT CASE WHEN all_shippings.pickup_time <= 2 AND all_shippings.delivery_shipping_status IN ('done','archived') AND all_shippings.pickup_shipping_status IN ('done','archived') THEN all_shippings.order_product_id END) AS  nb_pickup_where_pickup_time_less_equal_2_weeks,
   
   -- OTHER
   COUNT(DISTINCT CASE WHEN delivery_shipping_status = 'canceled' OR pickup_shipping_status= 'canceled' THEN all_shippings.order_product_id END) AS nb_canceled_items_shipped,
   COUNT(DISTINCT CASE WHEN delivery_shipping_status = 'broken' OR pickup_shipping_status = 'broken' THEN all_shippings.order_product_id END) AS nb_broken_items_shipped,   
   
FROM all_shippings
LEFT JOIN median_shipping_time 
ON all_shippings.delivery_date = median_shipping_time.delivery_date 
WHERE all_shippings.delivery_date >= '2019-01-01'
GROUP BY 1,2,3,11,12,13,14,15,16
ORDER BY 1