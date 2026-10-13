-- Edge-case PostgreSQL definitions. They are deliberately outside the main
-- chain: each one uses a construct the analyzer may not cover, so a parse
-- failure here is an isolated finding rather than a broken chain.
-- Run inside database warehouse (schemas dwd + ods).
CREATE SCHEMA IF NOT EXISTS dwd;
SET search_path TO dwd, ods;

DROP VIEW IF EXISTS v_experimental_arrays CASCADE;
DROP VIEW IF EXISTS v_experimental_series CASCADE;
DROP VIEW IF EXISTS v_experimental_lateral CASCADE;
DROP VIEW IF EXISTS v_experimental_grouping_sets CASCADE;
DROP VIEW IF EXISTS v_experimental_window_filter CASCADE;

-- GROUPING SETS / GROUPING()
CREATE VIEW v_experimental_grouping_sets AS
SELECT region,
       channel,
       count(*) AS order_count,
       sum(net_amount) AS net_amount,
       grouping(region) AS is_region_total,
       grouping(channel) AS is_channel_total
FROM v_order_base
GROUP BY GROUPING SETS ((region), (channel), ());

-- LATERAL join with ORDER BY ... LIMIT inside
CREATE VIEW v_experimental_lateral AS
SELECT b.order_id,
       b.region,
       top.item_id,
       top.product_name,
       top.line_amount
FROM v_order_base b
CROSS JOIN LATERAL (
  SELECT l.item_id, l.product_name, l.line_amount
  FROM v_order_line l
  WHERE l.order_id = b.order_id
  ORDER BY l.line_amount DESC
  LIMIT 1
) AS top;

-- generate_series + LEFT JOIN
CREATE VIEW v_experimental_series AS
SELECT d.day::date AS sales_date,
       count(o.order_id) AS order_count,
       COALESCE(sum(o.net_amount), 0) AS net_amount
FROM generate_series(date '2025-01-01', date '2026-03-31', interval '1 day') AS d(day)
LEFT JOIN orders o ON o.order_date = d.day::date
GROUP BY d.day;

-- Window function with FILTER, and array/json aggregation
CREATE VIEW v_experimental_arrays AS
SELECT b.customer_id,
       count(*) AS order_count,
       array_agg(b.order_no ORDER BY b.order_ts) AS order_numbers,
       count(*) FILTER (WHERE b.status_group = 'booked') AS booked_count,
       sum(b.net_amount) FILTER (WHERE b.channel = 'web') AS web_net_amount,
       jsonb_build_object('region', max(b.region), 'orders', count(*)) AS summary
FROM v_order_base b
GROUP BY b.customer_id;

-- FILTER on a window aggregate
CREATE VIEW v_experimental_window_filter AS
SELECT b.order_id,
       b.region,
       b.net_amount,
       sum(b.net_amount) FILTER (WHERE b.status_group = 'booked')
         OVER (PARTITION BY b.region) AS booked_region_net,
       percent_rank() OVER (PARTITION BY b.region ORDER BY b.net_amount) AS net_percent_rank
FROM v_order_base b;
