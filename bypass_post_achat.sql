WITH last_status_update AS (

  SELECT DISTINCT

    product_id,
    FIRST_VALUE(TIMESTAMP(date)) OVER (PARTITION BY product_id ORDER BY date DESC) AS last_status_update_date

  FROM `selency-data-bronze.mongodb.s_product_prod__products_history_flattened_results` products_history

) , remb AS ( 
  
  SELECT DISTINCT
    type,
    respo,
    CASE
      WHEN LOWER(refunds.respo) = 'client' THEN order_report.customer_id
      WHEN LOWER(refunds.respo) = 'vendeur' THEN order_report.seller_id
      ELSE NULL END AS user_id,
    refunds.order_number,
    refunds.sku,
    products_reposted.sku AS sku_reposted,
    order_report.gmv,
    products.status,
    last_status_update.last_status_update_date,
    order_report.created_at AS order_date,
    PARSE_DATE('%d/%m/%Y',date) AS remb_date,
    CASE
      WHEN products.status = 'sold_out' AND DATE_DIFF(DATE(last_status_update.last_status_update_date),DATE(order_report.created_at),DAY) < 1 AND products_reposted.sku IS NULL THEN TRUE
      WHEN products.status = 'removed_by_seller' AND DATE_DIFF(DATE(last_status_update.last_status_update_date),DATE(order_report.created_at),DAY) < 15 AND products_reposted.sku IS NULL THEN TRUE
      ELSE FALSE
      END AS bypass_boolean,
      products.product_id,
      
  FROM `selency-data-gold.operations.refunds_data_cs` refunds
  LEFT JOIN `selency-data-gold.raw_tables.order_report` order_report
  ON (refunds.order_number = order_report.order_number AND order_report.product_sku = refunds.sku AND order_report.order_status = 'paid'
AND order_report.order_product_status != 'UNAVAILABLE')
  LEFT JOIN `selency-data-gold.dim_tables.dim_product` products
  ON refunds.sku = products.sku
  LEFT JOIN last_status_update 
  ON last_status_update.product_id = products.product_id
  LEFT JOIN `selency-data-gold.dim_tables.dim_product` products_reposted
  ON (products.slug_fr = products_reposted.slug_fr AND products.sku != products_reposted.sku AND products.seller_id = products_reposted.seller_id AND products.created_at < products_reposted.created_at AND products.color_id = products_reposted.color_id AND products.price_value_eur = products_reposted.price_value_eur AND products.product_height = products_reposted.product_height AND products.product_width = products_reposted.product_width)
  WHERE refunds.respo IS NOT NULL
  AND LOWER(refunds.respo) NOT IN ('selency','7y9x5944')
  AND type IN ('Rétractation avant expédition','Livraison impossible','Rétractation / Indispo','Dégradation avant expédition')
  ORDER BY 5 DESC
  
), customer_orders AS (

  SELECT DISTINCT
    customer_id,
     COUNT(DISTINCT order_product_id) AS nb_products_bought
  FROM `selency-data-gold.raw_tables.order_report` order_report
  WHERE order_report.order_status = 'paid'
  AND order_report.order_product_status != 'UNAVAILABLE'
  GROUP BY 1
  ORDER BY 2 DESC

), seller_orders AS (

  SELECT DISTINCT
    seller_id,
    COUNT(DISTINCT order_product_id) AS nb_products_sold
  FROM `selency-data-gold.raw_tables.order_report` order_report
  WHERE order_report.order_status = 'paid'
  AND order_report.order_product_status != 'UNAVAILABLE'
  GROUP BY 1
  ORDER BY 2 DESC

), bypass AS (

  SELECT
    
    type,
    respo,
    user_id,
    seller_orders.nb_products_sold,
    customer_orders.nb_products_bought, 
    CASE
      WHEN respo = 'CLIENT' THEN nb_products_bought
      WHEN respo = 'VENDEUR' THEN nb_products_sold
      ELSE NULL END AS user_activity,
    COUNT(DISTINCT CASE WHEN bypass_boolean IS TRUE THEN CONCAT(order_number,sku) END) AS nb_remb_cases,
    CASE
      WHEN LOWER(respo) = 'client' THEN SAFE_DIVIDE(COUNT(DISTINCT CASE WHEN bypass_boolean IS TRUE THEN CONCAT(order_number,sku) END),MAX(nb_products_bought))
      WHEN LOWER(respo) = 'vendeur' THEN SAFE_DIVIDE(COUNT(DISTINCT CASE WHEN bypass_boolean IS TRUE THEN CONCAT(order_number,sku) END),MAX(nb_products_sold))
      ELSE NULL END AS ratio_activity_remb_cases,
      SUM(gmv) AS gmv

  FROM remb
  LEFT JOIN seller_orders
  ON seller_orders.seller_id = remb.user_id
  LEFT JOIN customer_orders
  ON customer_orders.customer_id = remb.user_id
  WHERE user_id IS NOT NULL
  GROUP BY 1,2,3,4,5,6
  ORDER BY 3

)


SELECT DISTINCT
  

  
  bypass.user_id,
  remb.product_id,
  CASE
    WHEN bypass.respo = 'CLIENT' THEN 'buyer'
    WHEN bypass.respo = 'VENDEUR' THEN 'seller'
    ELSE NULL END AS buyer_seller,
 
FROM bypass
LEFT JOIN remb
ON remb.user_id = bypass.user_id
WHERE user_activity >= 2 AND ratio_activity_remb_cases >= 0.3
