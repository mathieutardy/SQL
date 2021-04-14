WITH visitors AS (

  SELECT DISTINCT
    fullVisitorId,
    visitId,
    CASE
      WHEN exp.experimentVariant = '2' THEN 'Variant_leaf_and_cat'
      WHEN exp.experimentVariant = '1' THEN 'Variant_leaf_only'
      WHEN exp.experimentVariant = '0' THEN 'Reference'
    END AS user_type,
    hitNumber,
    hits.transaction.transactionId AS order_id,
    date,
    hits.type AS hit_type,
    visitStartTime,
    CASE
      WHEN (contentGroup.contentGroup5 LIKE 'Products'
           OR contentGroup.contentGroup5 LIKE 'Categories'
           OR contentGroup.contentGroup5 LIKE 'Search'
           OR contentGroup.contentGroup5 LIKE 'Selections'
           OR contentGroup.contentGroup5 LIKE 'Shops')
           AND contentGroup.contentGroup5 NOT LIKE 'Listing'
           AND totals.pageviews >= 2
           THEN 1
      ELSE 0 END AS boolean_relevant_session,
  FROM `selency-data-bronze.86655878.ga_sessions_*` ga
  CROSS JOIN UNNEST(hits) as hits
  CROSS JOIN UNNEST(hits.experiment) exp
  WHERE exp.experimentId = 'Zjv5kXWgQsuU7UMKPEeKwg'
  AND _TABLE_SUFFIX like 'intraday%'
  ORDER BY 1

), page_catalogue AS (

  SELECT DISTINCT
   date,
   fullVisitorId,
   visitId,
   hitNumber
  FROM `selency-data-bronze.86655878.ga_sessions_*` ga
  CROSS JOIN UNNEST(hits) as hits
  WHERE REGEXP_contains(page.pagePathLevel1, r'^/search|^/nouveaux-produits|^/chaises|/meubles|^/mobilier-de-jardin-terrasse|^/decorer|^/art|^/kids-enfant-vintage|^/selections|^/eclairer|^/furniture|^/lighting|^/decor|^/seating|^/new-products|^/garden-accessories|^/kids')
  AND _TABLE_SUFFIX like 'intraday%'

  ), page_produit AS (

   SELECT DISTINCT
    fullVisitorId,
    visitId,
    hitNumber
  FROM `selency-data-bronze.86655878.ga_sessions_*` ga
  CROSS JOIN UNNEST(hits) as hits
  WHERE hits.page.pagePathLevel1 IN ('/produit/', '/product/')
  AND hits.type = 'PAGE'
  AND _TABLE_SUFFIX like 'intraday%'

), ga_add_to_cart AS (

  SELECT DISTINCT
    fullVisitorId,
    visitId,
    hitNumber,
  FROM `selency-data-bronze.86655878.ga_sessions_*` ga
  CROSS JOIN UNNEST(hits) as hits
  WHERE  hits.eventInfo.eventAction = 'Add to cart'
  AND _TABLE_SUFFIX like 'intraday%'

), orders AS (

  SELECT
    order_id,
    fullVisitorId,
    visitId,
    hitNumber,
    user_type,
    ROW_NUMBER() OVER(PARTITION BY order_id order by visitStartTime) as _rank
  FROM visitors
  WHERE order_id IS NOT NULL

), ga_sessions AS (

  SELECT DISTINCT
    PARSE_DATE('%Y%m%d',  visitors.date) as created_at,
    visitors.user_type,

    -- SESSIONS
    COUNT(DISTINCT CASE WHEN visitors.boolean_relevant_session = 1 THEN CONCAT(visitors.fullVisitorId, visitors.visitId) END) AS relevant_sessions,
    COUNT(DISTINCT CONCAT(page_catalogue.fullVisitorId, page_catalogue.visitId)) AS sessions_one_catalogue_page,

    -- UNIQUE VIEWERS
    COUNT(DISTINCT page_catalogue.fullVisitorId) AS unique_viewers,
    COUNT(DISTINCT page_catalogue.fullVisitorId) AS unique_viewers_relevant,

    -- PAGES
    COUNT(CASE WHEN page_produit.fullVisitorId IS NOT NULL THEN page_produit.fullVisitorId END) AS nb_product_pages_after_catalogue_page,
    COUNT(page_catalogue.fullVisitorId) AS nb_catalogue_pages,

    -- ORDERS
    COUNT(DISTINCT CASE WHEN orders.order_id IS NOT NULL AND page_catalogue.hitNumber < orders.hitNumber THEN CONCAT(page_catalogue.fullVisitorId, page_catalogue.visitId) END) AS nb_sessions_with_order_after_catalogue_page,
    COUNT(DISTINCT CASE WHEN orders.order_id IS NOT NULL THEN CONCAT(visitors.fullVisitorId, visitors.visitId) END) AS nb_sessions_with_order,
    COUNT(DISTINCT CASE WHEN orders.order_id IS NOT NULL THEN CONCAT(page_catalogue.fullVisitorId, page_catalogue.visitId) END) AS nb_sessions_with_order_seen_page_catalogue,

    -- SESSIONS
    COUNT(DISTINCT CONCAT(page_catalogue.fullVisitorId, page_catalogue.visitId)) AS nb_sessions_with_at_least_one_catalogue_page,

    -- ADDING TO CART
    COUNT(DISTINCT CASE WHEN page_catalogue.hitNumber < ga_add_to_cart.hitNumber THEN CONCAT(page_catalogue.fullVisitorId, page_catalogue.visitId) END) AS nb_sessions_adding_cart_after_catalogue_page,
    COUNT(DISTINCT CASE WHEN ga_add_to_cart.hitNumber IS NOT NULL THEN CONCAT(visitors.fullVisitorId, visitors.visitId) END) AS nb_sessions_adding_cart,

  FROM visitors
  LEFT JOIN page_catalogue
  ON visitors.fullVisitorId = page_catalogue.fullVisitorId AND visitors.visitId = page_catalogue.visitId AND visitors.hitNumber = page_catalogue.hitNumber
  LEFT JOIN page_produit
  ON page_catalogue.fullVisitorId = page_produit.fullVisitorId AND page_catalogue.visitId = page_produit.visitId AND  page_catalogue.hitnumber = page_produit.hitnumber + 1
  LEFT JOIN orders
  ON visitors.fullVisitorId = orders.fullVisitorId AND visitors.visitId = orders.visitId AND orders._rank = 1
  LEFT JOIN ga_add_to_cart
  ON visitors.fullVisitorId = ga_add_to_cart.fullVisitorId AND visitors.visitId = ga_add_to_cart.visitId
  GROUP BY 1,2
  ORDER BY 1

), order_report AS (

  SELECT DISTINCT
    DATE(order_report.created_at) AS created_at,
    order_report.customer_id,
    order_report.order_id,
    order_report.product_sku,
    order_report.gmv,
    order_report.delivery_price,
    order_report.delivery_provider,
    order_report.order_status = 'paid' AND order_report.order_product_status != 'UNAVAILABLE' as is_post_cancel,
    order_report.order_status IN ('paid','canceled') as is_pre_cancel
  FROM `selency-data-gold.raw_tables.order_report_hourly` order_report
  LEFT JOIN `selency-data-gold.dim_tables.dim_product` products ON order_report.product_sku = products.sku
  WHERE DATE(order_report.created_at) >= (SELECT MIN(PARSE_DATE('%Y%m%d',  date)) FROM `selency-data-bronze.86655878.ga_sessions_*` WHERE _TABLE_SUFFIX like 'intraday%')
  AND products.is_retail IS FALSE
  ORDER BY 2
), order_after_catalogue_page AS (

 SELECT DISTINCT
    PARSE_DATE('%Y%m%d',  visitors.date) as created_at,
    CONCAT(visitors.fullVisitorId,visitors.visitId) AS session_id,
    visitors.user_type,
    orders.order_id,

  FROM visitors
  LEFT JOIN page_catalogue
  ON visitors.fullVisitorId = page_catalogue.fullVisitorId AND visitors.visitId = page_catalogue.visitId AND visitors.hitNumber = page_catalogue.hitNumber
  LEFT JOIN orders
  ON visitors.fullVisitorId = orders.fullVisitorId AND visitors.visitId = orders.visitId AND orders._rank = 1
  WHERE orders.hitNumber > page_catalogue.hitNumber
  ORDER BY 1

)

SELECT DISTINCT
  order_report.created_at,
  DATE_TRUNC(order_report.created_at, WEEK(SATURDAY)) AS start_week,
  DATE_TRUNC(order_report.created_at, MONTH) AS start_month,

  -- SPLIT A/B TEST
  orders.user_type AS user_type,
  ga_sessions.sessions_one_catalogue_page,
  ga_sessions.nb_product_pages_after_catalogue_page,
  ga_sessions.nb_catalogue_pages,
  ga_sessions.nb_sessions_with_order_after_catalogue_page,
  ga_sessions.nb_sessions_with_at_least_one_catalogue_page,
  ga_sessions.relevant_sessions,
  ga_sessions.nb_sessions_adding_cart_after_catalogue_page,
  ga_sessions.nb_sessions_with_order,
  ga_sessions.nb_sessions_adding_cart,
  ga_sessions.nb_sessions_with_order_seen_page_catalogue,

  -- GMV
  ROUND(SUM(CASE WHEN is_post_cancel AND order_after_catalogue_page.order_id IS NOT NULL THEN gmv END), 2) AS gmv_post_cancel_after_page_catalogue,
  ROUND(SUM(CASE WHEN is_post_cancel THEN gmv END), 2) AS gmv_post_cancel,

  -- GOOGLE ANALYTICS
  COUNT(DISTINCT CASE WHEN order_after_catalogue_page.order_id IS NOT NULL THEN order_report.customer_id END) AS total_buyers_after_page_catalogue,
  COUNT(DISTINCT order_report.customer_id) AS total_buyers,

  -- ORDERS
  COUNT(DISTINCT CASE WHEN  order_after_catalogue_page.order_id IS NOT NULL THEN order_report.order_id END) AS total_orders_after_page_catalogue,
  COUNT(DISTINCT order_report.order_id) AS total_orders,

  COUNT(DISTINCT CASE WHEN is_post_cancel AND order_after_catalogue_page.order_id IS NOT NULL THEN order_report.order_id END) AS orders_post_cancel_after_page_catalogue,
  COUNT(DISTINCT CASE WHEN is_pre_cancel AND order_after_catalogue_page.order_id IS NOT NULL THEN order_report.order_id END) AS orders_pre_cancel_after_page_catalogue,
  COUNT(DISTINCT CASE WHEN is_post_cancel THEN order_report.order_id END) AS orders_post_cancel,
  COUNT(DISTINCT CASE WHEN is_pre_cancel THEN order_report.order_id END) AS orders_pre_cancel,

  -- ITEMS SOLD
  COUNT(CASE WHEN is_post_cancel AND order_after_catalogue_page.order_id IS NOT NULL THEN order_report.product_sku END) AS items_sold_after_page_catalogue,
  COUNT(CASE WHEN is_post_cancel THEN order_report.product_sku END) AS items_sold,

  COUNT(CASE WHEN is_post_cancel AND order_report.delivery_provider = 'BROCANTE_LAB' AND order_after_catalogue_page.order_id IS NOT NULL THEN order_report.product_sku END) AS items_sold_by_selency_after_page_catalogue,
  COUNT(CASE WHEN is_post_cancel AND order_report.delivery_provider = 'BROCANTE_LAB' THEN order_report.product_sku END) AS items_sold_by_selency,

  -- AVERAGE SHIPPING PRICE
  ROUND(AVG(CASE WHEN is_post_cancel AND order_after_catalogue_page.order_id IS NOT NULL THEN order_report.delivery_price END), 2) AS avg_shipping_price_after_page_catalogue,
  ROUND(AVG(CASE WHEN is_post_cancel THEN order_report.delivery_price END), 2) AS avg_shipping_price,

  ROUND(AVG(CASE WHEN is_post_cancel AND order_report.delivery_provider = 'BROCANTE_LAB' AND order_after_catalogue_page.order_id IS NOT NULL THEN order_report.delivery_price END), 2) AS avg_shipping_price_by_selency_after_page_catalogue,
  ROUND(AVG(CASE WHEN is_post_cancel AND order_report.delivery_provider = 'BROCANTE_LAB' THEN order_report.delivery_price END), 2) AS avg_shipping_price_by_selency,

  -- MEDIAN SHIPPING PRICE
  APPROX_QUANTILES(CASE WHEN order_after_catalogue_page.order_id IS NOT NULL THEN order_report.delivery_price END, 100)[OFFSET(50)] AS median_shipping_price_after_page_catalogue,
  APPROX_QUANTILES(order_report.delivery_price, 100)[OFFSET(50)] AS median_shipping_price,

FROM order_report
LEFT JOIN orders
ON order_report.order_id = orders.order_id AND orders._rank = 1
LEFT JOIN order_after_catalogue_page
ON order_report.order_id = order_after_catalogue_page.order_id
LEFT JOIN ga_sessions
ON orders.user_type = ga_sessions.user_type AND order_report.created_at = ga_sessions.created_at
WHERE orders.user_type IS NOT NULL
AND ga_sessions.user_type IS NOT NULL
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14
ORDER BY 1,4