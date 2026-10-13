-- StarRocks ads: analysable StarRocks objects on top of the landed tables.
-- These are what the StarRocks lineage analyzer is expected to resolve.
-- Apply with: mysql -uroot -h127.0.0.1 -P9030 < 20_ads_ddl.sql

CREATE DATABASE IF NOT EXISTS ads;

DROP MATERIALIZED VIEW IF EXISTS ads.mv_sr_region_daily;
DROP VIEW IF EXISTS ads.v_sr_sales_band;
DROP VIEW IF EXISTS ads.v_sr_channel_region;

-- View with a join, a CASE band and an aggregate.
CREATE VIEW ads.v_sr_channel_region AS
SELECT f.region,
       f.channel,
       f.currency,
       count(DISTINCT f.order_id) AS order_count,
       sum(f.net_amount) AS net_amount,
       sum(f.line_revenue) AS line_revenue,
       CASE
         WHEN sum(f.net_amount) >= 3000000 THEN 'A'
         WHEN sum(f.net_amount) >= 1000000 THEN 'B'
         ELSE 'C'
       END AS revenue_tier
FROM ods.dwd_order_fact f
GROUP BY f.region, f.channel, f.currency;

-- View over a landed fact plus the serving table.
CREATE VIEW ads.v_sr_sales_band AS
SELECT d.sales_date,
       d.region,
       d.channel,
       d.net_line_amount,
       d.order_share_pct,
       CASE WHEN d.order_share_pct >= 50 THEN 'dominant' ELSE 'minor' END AS share_band,
       c.customer_segment,
       c.total_net AS segment_total_net
FROM ods.ads_daily_sales d
LEFT JOIN ods.ads_customer_segment c
       ON c.region = d.region;

-- Async materialized view (single-source aggregate).
CREATE MATERIALIZED VIEW ads.mv_sr_region_daily
DISTRIBUTED BY HASH(order_date) BUCKETS 3
REFRESH ASYNC
PROPERTIES ("replication_num" = "1")
AS
SELECT order_date,
       region,
       channel,
       count(*) AS order_count,
       sum(net_amount) AS net_amount,
       sum(line_revenue) AS line_revenue
FROM ods.dwd_order_fact
GROUP BY order_date, region, channel;
