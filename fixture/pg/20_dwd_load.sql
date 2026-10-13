-- dwd load: INSERT ... SELECT statements executed by Airflow. The
-- OpenLineage SQL extractor turns each of these into column-level lineage
-- events, which is how table lineage crosses from the analysed views into the
-- plain fact tables.
-- Run inside database warehouse (schemas dwd + ods).
CREATE SCHEMA IF NOT EXISTS dwd;
SET search_path TO dwd, ods;

TRUNCATE TABLE dwd.dwd_order_fact;
INSERT INTO dwd.dwd_order_fact (
  order_id, order_no, customer_id, customer_code, customer_name, country, region,
  sub_region, sales_rep_id, rep_name, rep_team, order_date, order_ts, status,
  status_group, channel, currency, item_count, gross_amount, discount_amount,
  shipping_fee, tax_amount, net_amount, line_count, units, line_revenue,
  top_category, paid_amount, refund_amount, balance_due, payment_coverage,
  customer_segment, month_net_rank
)
WITH line_agg AS (
  SELECT l.order_id,
         count(*) AS line_count,
         sum(l.quantity) AS units,
         sum(l.line_amount) AS line_revenue
  FROM dwd.v_order_line l
  GROUP BY l.order_id
)
SELECT b.order_id,
       b.order_no,
       b.customer_id,
       b.customer_code,
       b.full_name,
       b.country,
       b.region,
       b.sub_region,
       b.sales_rep_id,
       b.rep_name,
       b.rep_team,
       b.order_date,
       b.order_ts,
       b.status,
       b.status_group,
       b.channel,
       b.currency,
       b.item_count,
       b.gross_amount,
       b.discount_amount,
       b.shipping_fee,
       b.tax_amount,
       b.net_amount,
       COALESCE(la.line_count, 0),
       COALESCE(la.units, 0),
       COALESCE(la.line_revenue, 0),
       tc.top_category,
       COALESCE(pay.paid_amount, 0),
       COALESCE(pay.refund_amount, 0),
       b.net_amount - (COALESCE(pay.paid_amount, 0) - COALESCE(pay.refund_amount, 0)),
       CASE
         WHEN b.net_amount > 0
           THEN round((COALESCE(pay.paid_amount, 0) - COALESCE(pay.refund_amount, 0)) / b.net_amount, 4)
         ELSE NULL
       END,
       COALESCE(c360.customer_segment, 'prospect'),
       rank() OVER (PARTITION BY date_trunc('month', b.order_date) ORDER BY b.net_amount DESC)
FROM dwd.v_order_base b
LEFT JOIN line_agg la ON la.order_id = b.order_id
LEFT JOIN dwd.v_order_top_category tc ON tc.order_id = b.order_id
LEFT JOIN dwd.v_customer_payments pay ON pay.order_id = b.order_id
LEFT JOIN dwd.v_customer_360 c360 ON c360.customer_id = b.customer_id;

TRUNCATE TABLE dwd.dwd_customer_360;
INSERT INTO dwd.dwd_customer_360 (
  customer_id, customer_code, customer_name, country, region, sub_region, city,
  status, loyalty_tier, tenure_days, order_count, lifetime_gross, lifetime_net,
  avg_order_value, lifetime_units, paid_amount, refund_amount, net_collected,
  balance_due, collection_rate, customer_segment, value_rank
)
SELECT c.customer_id,
       c.customer_code,
       c.full_name,
       c.country,
       c.region,
       c.sub_region,
       c.city,
       c.status,
       c.loyalty_tier,
       c.tenure_days,
       COALESCE(c.order_count, 0),
       COALESCE(c.lifetime_gross, 0),
       COALESCE(c.lifetime_net, 0),
       c.avg_order_value,
       COALESCE(c.lifetime_units, 0),
       c.paid_amount,
       c.refund_amount,
       c.net_collected,
       c.balance_due,
       c.collection_rate,
       c.customer_segment,
       c.value_rank
FROM dwd.v_customer_360 c;

REFRESH MATERIALIZED VIEW dwd.mv_daily_sales;
