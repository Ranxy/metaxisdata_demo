-- ads load: cross-database INSERT ... SELECT from dwd.
-- Run inside database warehouse (schema ads).
CREATE SCHEMA IF NOT EXISTS ads;
SET search_path TO ads, dwd;

TRUNCATE TABLE ads.ads_daily_sales;
INSERT INTO ads.ads_daily_sales (
  sales_date, channel, region, currency, order_count, customer_count, units,
  gross_amount, discount_amount, net_line_amount, avg_line_amount,
  order_share_pct, revenue_band
)
SELECT m.sales_date,
       m.channel,
       m.region,
       m.currency,
       m.order_count,
       m.customer_count,
       m.units,
       m.gross_amount,
       m.discount_amount,
       m.net_line_amount,
       m.avg_line_amount,
       round(m.order_count / NULLIF(sum(m.order_count) OVER (PARTITION BY m.sales_date), 0) * 100, 4),
       CASE
         WHEN m.net_line_amount >= 10000 THEN 'high'
         WHEN m.net_line_amount >= 2000 THEN 'medium'
         ELSE 'low'
       END
FROM dwd.mv_daily_sales m;

TRUNCATE TABLE ads.ads_customer_segment;
INSERT INTO ads.ads_customer_segment (
  customer_segment, country, region, customer_count, total_net, avg_net,
  avg_orders, vip_count, segment_share_pct, top_customer_id, top_customer_name
)
WITH base AS (
  SELECT c.customer_id,
         c.full_name,
         c.customer_segment,
         c.country,
         c.region,
         c.lifetime_net,
         c.order_count,
         c.is_vip
  FROM dwd.v_customer_360 c
),
agg AS (
  SELECT b.customer_segment,
         b.country,
         b.region,
         count(*) AS customer_count,
         COALESCE(sum(b.lifetime_net), 0) AS total_net,
         COALESCE(round(avg(b.lifetime_net), 2), 0) AS avg_net,
         COALESCE(round(avg(b.order_count), 2), 0) AS avg_orders,
         count(*) FILTER (WHERE b.is_vip) AS vip_count
  FROM base b
  GROUP BY b.customer_segment, b.country, b.region
)
SELECT a.customer_segment,
       a.country,
       a.region,
       a.customer_count,
       a.total_net,
       a.avg_net,
       a.avg_orders,
       a.vip_count,
       round(a.total_net / NULLIF(sum(a.total_net) OVER (), 0) * 100, 4),
       (SELECT b.customer_id
          FROM base b
         WHERE b.customer_segment = a.customer_segment
           AND b.country = a.country
           AND b.region = a.region
         ORDER BY b.lifetime_net DESC
         LIMIT 1),
       (SELECT b.full_name
          FROM base b
         WHERE b.customer_segment = a.customer_segment
           AND b.country = a.country
           AND b.region = a.region
         ORDER BY b.lifetime_net DESC
         LIMIT 1)
FROM agg a;
