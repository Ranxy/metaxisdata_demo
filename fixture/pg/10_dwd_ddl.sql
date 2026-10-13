-- dwd (PostgreSQL): the complex-transform layer.
-- Views are analyzed by the PostgreSQL lineage analyzer from their stored
-- definitions; the two tables are populated by Airflow SQL operators, whose
-- OpenLineage events carry the INSERT ... SELECT column lineage.
-- Run inside database warehouse (schemas dwd + ods).
CREATE SCHEMA IF NOT EXISTS dwd;
SET search_path TO dwd, ods;

DROP MATERIALIZED VIEW IF EXISTS mv_daily_sales CASCADE;
DROP VIEW IF EXISTS v_customer_360 CASCADE;
DROP VIEW IF EXISTS v_customer_payments CASCADE;
DROP VIEW IF EXISTS v_product_performance CASCADE;
DROP VIEW IF EXISTS v_order_line CASCADE;
DROP VIEW IF EXISTS v_order_base CASCADE;
DROP VIEW IF EXISTS v_experimental_pg_features CASCADE;
DROP TABLE IF EXISTS dwd_order_fact CASCADE;
DROP TABLE IF EXISTS dwd_customer_360 CASCADE;

-- 1) Order header enrichment: joins customers/regions/sales_reps, CASE bucketing,
--    arithmetic, COALESCE/NULLIF, and a derived month key.
CREATE VIEW v_order_base AS
SELECT o.order_id,
       o.order_no,
       o.customer_id,
       c.customer_code,
       c.full_name,
       c.country,
       r.country_name,
       r.region,
       r.sub_region,
       r.is_emerging,
       o.sales_rep_id,
       sr.rep_name,
       sr.team AS rep_team,
       o.order_ts,
       o.order_date,
       date_trunc('month', o.order_ts)::date AS order_month,
       o.status,
       CASE
         WHEN o.status = 'completed' THEN 'booked'
         WHEN o.status = 'shipped' THEN 'booked'
         WHEN o.status = 'returned' THEN 'reversed'
         WHEN o.status = 'cancelled' THEN 'lost'
         ELSE 'open'
       END AS status_group,
       o.channel,
       o.currency,
       o.item_count,
       o.gross_amount,
       o.discount_amount,
       o.shipping_fee,
       o.tax_amount,
       o.net_amount,
       COALESCE(o.coupon_code, 'none') AS coupon_code,
       CASE
         WHEN o.gross_amount > 0 THEN round(o.discount_amount / o.gross_amount * 100, 2)
         ELSE 0
       END AS discount_rate_pct,
       CASE WHEN o.discount_amount > 0 THEN true ELSE false END AS is_discounted,
       COALESCE(c.credit_limit, 0) AS credit_limit
FROM orders o
JOIN customers c ON c.customer_id = o.customer_id
JOIN regions r ON r.country = c.country
LEFT JOIN sales_reps sr ON sr.rep_id = o.sales_rep_id;

-- 2) Order line enrichment: product attributes, margin math, and window
--    functions over each order (rank + share of the order).
CREATE VIEW v_order_line AS
SELECT i.item_id,
       i.order_id,
       o.order_date,
       i.product_id,
       p.sku,
       p.product_name,
       p.category,
       p.sub_category,
       p.brand,
       i.quantity,
       i.unit_price,
       i.discount_pct,
       i.line_amount,
       round(i.quantity * i.unit_price, 2) AS line_gross,
       round(i.quantity * i.unit_price - i.line_amount, 2) AS line_discount,
       i.warehouse_code,
       round(i.line_amount - i.quantity * p.unit_cost, 2) AS gross_margin,
       CASE
         WHEN i.line_amount > 0 THEN round((i.line_amount - i.quantity * p.unit_cost) / i.line_amount * 100, 2)
         ELSE NULL
       END AS margin_pct,
       row_number() OVER (PARTITION BY i.order_id ORDER BY i.line_amount DESC, i.item_id) AS line_rank,
       sum(i.line_amount) OVER (PARTITION BY i.order_id) AS order_line_total,
       round(i.line_amount / NULLIF(sum(i.line_amount) OVER (PARTITION BY i.order_id), 0) * 100, 2) AS order_line_share_pct,
       CASE
         WHEN p.unit_price >= 500 THEN 'premium'
         WHEN p.unit_price >= 100 THEN 'mid'
         ELSE 'value'
       END AS price_band
FROM order_items i
JOIN products p ON p.product_id = i.product_id
JOIN orders o ON o.order_id = i.order_id;

-- 2b) Dominant category per order (DISTINCT ON is PostgreSQL-specific syntax).
CREATE VIEW v_order_top_category AS
SELECT DISTINCT ON (l.order_id)
       l.order_id,
       l.category AS top_category,
       l.line_amount AS top_line_amount
FROM v_order_line l
ORDER BY l.order_id, l.line_amount DESC, l.item_id;

-- 3) Payment/refund reconciliation per order.
CREATE VIEW v_customer_payments AS
SELECT o.customer_id,
       p.order_id,
       count(*) FILTER (WHERE p.status = 'captured') AS captured_payment_count,
       sum(CASE WHEN p.status = 'captured' THEN p.amount ELSE 0 END) AS paid_amount,
       sum(CASE WHEN p.status = 'failed' THEN 1 ELSE 0 END) AS failed_payment_count,
       COALESCE(sum(rf.amount), 0) AS refund_amount,
       max(p.payment_ts) AS last_payment_ts,
       COALESCE(sum(CASE WHEN p.status = 'captured' THEN p.fee ELSE 0 END), 0) AS fee_amount
FROM payments p
JOIN orders o ON o.order_id = p.order_id
LEFT JOIN refunds rf ON rf.payment_id = p.payment_id
GROUP BY o.customer_id, p.order_id;

-- 4) Customer 360: CTE-fed aggregates, ratio math, CASE segmentation and a
--    window rank across all customers.
CREATE VIEW v_customer_360 AS
WITH order_agg AS (
  SELECT b.customer_id,
         count(*) AS order_count,
         count(*) FILTER (WHERE b.status_group = 'booked') AS booked_order_count,
         count(*) FILTER (WHERE b.status = 'cancelled') AS cancelled_order_count,
         min(b.order_date) AS first_order_date,
         max(b.order_date) AS last_order_date,
         sum(b.gross_amount) AS lifetime_gross,
         sum(b.discount_amount) AS lifetime_discount,
         sum(b.net_amount) AS lifetime_net,
         round(avg(b.net_amount), 2) AS avg_order_value,
         sum(b.item_count) AS lifetime_units
  FROM v_order_base b
  WHERE b.status <> 'cancelled'
  GROUP BY b.customer_id
),
pay_agg AS (
  SELECT customer_id,
         sum(paid_amount) AS paid_amount,
         sum(refund_amount) AS refund_amount,
         sum(captured_payment_count) AS captured_payment_count,
         sum(failed_payment_count) AS failed_payment_count
  FROM v_customer_payments
  GROUP BY customer_id
)
SELECT c.customer_id,
       c.customer_code,
       c.full_name,
       c.country,
       r.country_name,
       r.region,
       r.sub_region,
       c.city,
       c.status,
       c.loyalty_tier,
       c.signup_date,
       (CURRENT_DATE - c.signup_date) AS tenure_days,
       oa.order_count,
       oa.booked_order_count,
       oa.cancelled_order_count,
       oa.first_order_date,
       oa.last_order_date,
       oa.lifetime_gross,
       oa.lifetime_discount,
       oa.lifetime_net,
       oa.avg_order_value,
       oa.lifetime_units,
       COALESCE(pa.paid_amount, 0) AS paid_amount,
       COALESCE(pa.refund_amount, 0) AS refund_amount,
       COALESCE(pa.paid_amount, 0) - COALESCE(pa.refund_amount, 0) AS net_collected,
       COALESCE(oa.lifetime_net, 0) - (COALESCE(pa.paid_amount, 0) - COALESCE(pa.refund_amount, 0)) AS balance_due,
       CASE
         WHEN COALESCE(oa.lifetime_net, 0) > 0
           THEN round((COALESCE(pa.paid_amount, 0) - COALESCE(pa.refund_amount, 0)) / oa.lifetime_net, 4)
         ELSE NULL
       END AS collection_rate,
       CASE
         WHEN COALESCE(oa.lifetime_net, 0) >= 20000 THEN 'platinum'
         WHEN COALESCE(oa.lifetime_net, 0) >= 8000 THEN 'gold'
         WHEN COALESCE(oa.lifetime_net, 0) >= 2000 THEN 'silver'
         WHEN oa.order_count IS NULL THEN 'prospect'
         ELSE 'bronze'
       END AS customer_segment,
       rank() OVER (ORDER BY COALESCE(oa.lifetime_net, 0) DESC) AS value_rank,
       CASE WHEN COALESCE(oa.lifetime_net, 0) >= 8000 THEN true ELSE false END AS is_vip
FROM customers c
JOIN regions r ON r.country = c.country
LEFT JOIN order_agg oa ON oa.customer_id = c.customer_id
LEFT JOIN pay_agg pa ON pa.customer_id = c.customer_id;

-- 5) Product performance: window share of revenue plus a per-category rank.
CREATE VIEW v_product_performance AS
WITH product_agg AS (
  SELECT l.product_id,
         l.sku,
         l.category,
         count(DISTINCT l.order_id) AS order_count,
         sum(l.quantity) AS units_sold,
         sum(l.line_amount) AS revenue,
         sum(l.line_amount - l.quantity * p.unit_cost) AS gross_margin,
         round(avg(l.discount_pct), 2) AS avg_discount_pct
  FROM v_order_line l
  JOIN products p ON p.product_id = l.product_id
  GROUP BY l.product_id, l.sku, l.category
)
SELECT a.product_id,
       a.sku,
       p.product_name,
       a.category,
       p.sub_category,
       p.brand,
       p.unit_price,
       p.unit_cost,
       a.order_count,
       a.units_sold,
       a.revenue,
       a.gross_margin,
       CASE WHEN a.revenue > 0 THEN round(a.gross_margin / a.revenue * 100, 2) ELSE NULL END AS margin_pct,
       round(a.revenue / NULLIF(sum(a.revenue) OVER (), 0) * 100, 4) AS revenue_share_pct,
       rank() OVER (PARTITION BY a.category ORDER BY a.revenue DESC) AS category_rank,
       a.avg_discount_pct,
       CASE WHEN rank() OVER (PARTITION BY a.category ORDER BY a.revenue DESC) <= 3 THEN true ELSE false END AS is_top_seller
FROM product_agg a
JOIN products p ON p.product_id = a.product_id;

-- 6) Daily sales rollup, materialized so DataX can ship it to StarRocks.
CREATE MATERIALIZED VIEW mv_daily_sales AS
SELECT l.order_date AS sales_date,
       b.channel,
       b.region,
       b.currency,
       count(DISTINCT b.order_id) AS order_count,
       count(DISTINCT b.customer_id) AS customer_count,
       sum(l.quantity) AS units,
       sum(l.line_gross) AS gross_amount,
       sum(l.line_discount) AS discount_amount,
       sum(l.line_amount) AS net_line_amount,
       round(avg(l.line_amount), 2) AS avg_line_amount
FROM v_order_line l
JOIN v_order_base b ON b.order_id = l.order_id
GROUP BY l.order_date, b.channel, b.region, b.currency
WITH DATA;

CREATE INDEX IF NOT EXISTS idx_mv_daily_sales_date ON mv_daily_sales (sales_date);

-- 7) Order fact, loaded by an Airflow INSERT ... SELECT (OpenLineage SQL lineage).
CREATE TABLE IF NOT EXISTS dwd_order_fact (
  order_id          BIGINT        NOT NULL,
  order_no          VARCHAR(32)   NOT NULL,
  customer_id       BIGINT        NOT NULL,
  customer_code     VARCHAR(20)   NOT NULL,
  customer_name     VARCHAR(100)  NOT NULL,
  country           CHAR(2)       NOT NULL,
  region            VARCHAR(32)   NOT NULL,
  sub_region        VARCHAR(32)   NOT NULL,
  sales_rep_id      INTEGER       NULL,
  rep_name          VARCHAR(64)   NULL,
  rep_team          VARCHAR(32)   NULL,
  order_date        DATE          NOT NULL,
  order_ts          TIMESTAMP     NOT NULL,
  status            VARCHAR(20)   NOT NULL,
  status_group      VARCHAR(16)   NOT NULL,
  channel           VARCHAR(20)   NOT NULL,
  currency          CHAR(3)       NOT NULL,
  item_count        INTEGER       NOT NULL,
  gross_amount      NUMERIC(14,2) NOT NULL,
  discount_amount   NUMERIC(14,2) NOT NULL,
  shipping_fee      NUMERIC(10,2) NOT NULL,
  tax_amount        NUMERIC(12,2) NOT NULL,
  net_amount        NUMERIC(14,2) NOT NULL,
  line_count        INTEGER       NOT NULL,
  units             INTEGER       NOT NULL,
  line_revenue      NUMERIC(14,2) NOT NULL,
  top_category      VARCHAR(64)   NULL,
  paid_amount       NUMERIC(14,2) NOT NULL,
  refund_amount     NUMERIC(14,2) NOT NULL,
  balance_due       NUMERIC(14,2) NOT NULL,
  payment_coverage  NUMERIC(9,4)  NULL,
  customer_segment  VARCHAR(16)   NOT NULL,
  month_net_rank    INTEGER       NOT NULL,
  loaded_at         TIMESTAMP     NOT NULL DEFAULT now(),
  PRIMARY KEY (order_id)
);

-- 8) Customer 360 snapshot, also loaded by an Airflow INSERT ... SELECT.
CREATE TABLE IF NOT EXISTS dwd_customer_360 (
  customer_id          BIGINT        NOT NULL,
  customer_code        VARCHAR(20)   NOT NULL,
  customer_name        VARCHAR(100)  NOT NULL,
  country              CHAR(2)       NOT NULL,
  region               VARCHAR(32)   NOT NULL,
  sub_region           VARCHAR(32)   NOT NULL,
  city                 VARCHAR(64)   NOT NULL,
  status               VARCHAR(16)   NOT NULL,
  loyalty_tier         VARCHAR(16)   NOT NULL,
  tenure_days          INTEGER       NOT NULL,
  order_count          INTEGER       NOT NULL,
  lifetime_gross       NUMERIC(14,2) NOT NULL,
  lifetime_net         NUMERIC(14,2) NOT NULL,
  avg_order_value      NUMERIC(14,2) NULL,
  lifetime_units       INTEGER       NOT NULL,
  paid_amount          NUMERIC(14,2) NOT NULL,
  refund_amount        NUMERIC(14,2) NOT NULL,
  net_collected        NUMERIC(14,2) NOT NULL,
  balance_due          NUMERIC(14,2) NOT NULL,
  collection_rate      NUMERIC(9,4)  NULL,
  customer_segment     VARCHAR(16)   NOT NULL,
  value_rank           INTEGER       NOT NULL,
  loaded_at            TIMESTAMP     NOT NULL DEFAULT now(),
  PRIMARY KEY (customer_id)
);
