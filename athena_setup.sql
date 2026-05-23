-- ============================================================
-- 0. Create a dedicated database
-- ============================================================
CREATE DATABASE IF NOT EXISTS maxab;


-- ============================================================
-- 1. orders
-- ============================================================
DROP TABLE IF EXISTS maxab.orders;
CREATE EXTERNAL TABLE maxab.orders (
    order_id          STRING,
    customer_id       STRING,
    merchant_id       STRING,
    basket_value      DOUBLE,
    order_date        STRING,
    payment_method    STRING,
    payment_status    STRING,
    fulfilment_status STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    'separatorChar' = ',',
    'quoteChar'     = '"',
    'escapeChar'    = '\\'
)
STORED AS TEXTFILE
LOCATION 's3://maxab-assessment-data/raw/orders/'
TBLPROPERTIES (
    'skip.header.line.count' = '1',
    'classification'         = 'csv'
);


-- ============================================================
-- 2. order_items
-- ============================================================
DROP TABLE IF EXISTS maxab.order_items;
CREATE EXTERNAL TABLE maxab.order_items (
    order_id   STRING,
    sku_id     STRING,
    quantity   INT,
    unit_price DOUBLE,
    line_total DOUBLE
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    'separatorChar' = ',',
    'quoteChar'     = '"',
    'escapeChar'    = '\\'
)
STORED AS TEXTFILE
LOCATION 's3://maxab-assessment-data/raw/order_items/'
TBLPROPERTIES (
    'skip.header.line.count' = '1',
    'classification'         = 'csv'
);


-- ============================================================
-- 3. customers
-- ============================================================
DROP TABLE IF EXISTS maxab.customers;
CREATE EXTERNAL TABLE maxab.customers (
    customer_id  STRING,
    tenure_days  INT,
    total_orders INT,
    ltv          DOUBLE,
    region       STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    'separatorChar' = ',',
    'quoteChar'     = '"',
    'escapeChar'    = '\\'
)
STORED AS TEXTFILE
LOCATION 's3://maxab-assessment-data/raw/customers/'
TBLPROPERTIES (
    'skip.header.line.count' = '1',
    'classification'         = 'csv'
);


-- ============================================================
-- 4. fraud_flags
-- ============================================================
DROP TABLE IF EXISTS maxab.fraud_flags;
CREATE EXTERNAL TABLE maxab.fraud_flags (
    order_id    STRING,
    fraud_score DOUBLE,
    flag_reason STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    'separatorChar' = ',',
    'quoteChar'     = '"',
    'escapeChar'    = '\\'
)
STORED AS TEXTFILE
LOCATION 's3://maxab-assessment-data/raw/fraud_flags/'
TBLPROPERTIES (
    'skip.header.line.count' = '1',
    'classification'         = 'csv'
);


-- ============================================================
-- TEST QUERIES
-- ============================================================

-- 1. orders: row count, date range, basket distribution, status breakdown
SELECT
    COUNT(*)                                        AS total_orders,
    MIN(DATE(order_date))                           AS earliest_order,
    MAX(DATE(order_date))                           AS latest_order,
    ROUND(AVG(basket_value), 2)                     AS avg_basket_egp,
    ROUND(APPROX_PERCENTILE(basket_value, 0.5), 2)  AS median_basket_egp,
    ROUND(MAX(basket_value), 2)                     AS max_basket_egp,
    COUNT(DISTINCT customer_id)                     AS unique_customers,
    COUNT(DISTINCT merchant_id)                     AS unique_merchants
FROM maxab.orders;

-- 2. order_items: row count, items-per-order distribution, top SKUs by revenue
SELECT
    COUNT(*)                                        AS total_line_items,
    COUNT(DISTINCT order_id)                        AS orders_covered,
    ROUND(AVG(CAST(quantity AS DOUBLE)), 2)         AS avg_qty,
    ROUND(AVG(unit_price), 2)                       AS avg_unit_price_egp,
    ROUND(SUM(line_total), 2)                       AS total_gmv_egp
FROM maxab.order_items;

-- 3. customers: row count, region spread, LTV stats
SELECT
    COUNT(*)                                           AS total_customers,
    COUNT(DISTINCT region)                             AS regions,
    ROUND(AVG(ltv), 2)                                 AS avg_ltv_egp,
    ROUND(AVG(CAST(tenure_days AS DOUBLE)), 0)         AS avg_tenure_days,
    ROUND(AVG(CAST(total_orders AS DOUBLE)), 1)        AS avg_orders_per_customer
FROM maxab.customers;

-- 4. fraud_flags: row count, fraud rate, score distribution, top reasons
SELECT
    COUNT(*)                                                          AS flagged_orders,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM maxab.orders), 2) AS pct_of_orders,
    ROUND(AVG(fraud_score), 4)                                        AS avg_fraud_score,
    ROUND(APPROX_PERCENTILE(fraud_score, 0.5), 4)                     AS median_fraud_score,
    ROUND(MAX(fraud_score), 4)                                        AS max_fraud_score
FROM maxab.fraud_flags;

-- 5. Verify basket_value = sum of line items (should return 0 mismatches)
SELECT COUNT(*) AS mismatches
FROM maxab.orders o
JOIN (
    SELECT order_id, ROUND(SUM(line_total), 2) AS computed_basket
    FROM maxab.order_items
    GROUP BY order_id
) li ON li.order_id = o.order_id
WHERE ABS(o.basket_value - li.computed_basket) > 0.01;

-- 6. Top 10 SKUs by total revenue
SELECT
    sku_id,
    COUNT(DISTINCT order_id)                AS orders,
    SUM(CAST(quantity AS BIGINT))           AS units_sold,
    ROUND(SUM(line_total), 2)               AS total_revenue_egp
FROM maxab.order_items
GROUP BY sku_id
ORDER BY total_revenue_egp DESC
LIMIT 10;

-- 7. High-fraud orders with customer context and line-item count
SELECT
    o.order_id,
    o.customer_id,
    o.basket_value,
    o.payment_status,
    o.fulfilment_status,
    c.region,
    c.tenure_days,
    f.fraud_score,
    f.flag_reason,
    li.line_items
FROM maxab.orders o
JOIN maxab.fraud_flags f ON f.order_id = o.order_id
JOIN maxab.customers   c ON c.customer_id = o.customer_id
JOIN (
    SELECT order_id, COUNT(*) AS line_items
    FROM maxab.order_items
    GROUP BY order_id
) li ON li.order_id = o.order_id
WHERE f.fraud_score > 0.80
ORDER BY f.fraud_score DESC
LIMIT 25;
