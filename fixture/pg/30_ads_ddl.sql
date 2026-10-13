-- ads (PostgreSQL): serving layer. Tables are loaded by Airflow
-- INSERT ... SELECT statements and are the source of the second DataX hop.
-- Run inside database warehouse (schema ads).
CREATE SCHEMA IF NOT EXISTS ads;
SET search_path TO ads, dwd;

DROP VIEW IF EXISTS v_ads_region_summary CASCADE;
DROP TABLE IF EXISTS ads_customer_segment CASCADE;
DROP TABLE IF EXISTS ads_daily_sales CASCADE;

CREATE TABLE IF NOT EXISTS ads_daily_sales (
  sales_date       DATE          NOT NULL,
  channel          VARCHAR(20)   NOT NULL,
  region           VARCHAR(32)   NOT NULL,
  currency         CHAR(3)       NOT NULL,
  order_count      BIGINT        NOT NULL,
  customer_count   BIGINT        NOT NULL,
  units            BIGINT        NOT NULL,
  gross_amount     NUMERIC(16,2) NOT NULL,
  discount_amount  NUMERIC(16,2) NOT NULL,
  net_line_amount  NUMERIC(16,2) NOT NULL,
  avg_line_amount  NUMERIC(14,2) NULL,
  order_share_pct  NUMERIC(9,4)  NULL,
  revenue_band     VARCHAR(16)   NOT NULL,
  loaded_at        TIMESTAMP     NOT NULL DEFAULT now(),
  PRIMARY KEY (sales_date, channel, region, currency)
);

CREATE TABLE IF NOT EXISTS ads_customer_segment (
  customer_segment  VARCHAR(16)   NOT NULL,
  country           CHAR(2)       NOT NULL,
  region            VARCHAR(32)   NOT NULL,
  customer_count    BIGINT        NOT NULL,
  total_net         NUMERIC(16,2) NOT NULL,
  avg_net           NUMERIC(14,2) NULL,
  avg_orders        NUMERIC(10,2) NULL,
  vip_count         BIGINT        NOT NULL,
  segment_share_pct NUMERIC(9,4)  NULL,
  top_customer_id   BIGINT        NULL,
  top_customer_name VARCHAR(100)  NULL,
  loaded_at         TIMESTAMP     NOT NULL DEFAULT now(),
  PRIMARY KEY (customer_segment, country, region)
);

-- A view on top of the serving table, so the ads database has its own
-- analysable definition too.
CREATE VIEW v_ads_region_summary AS
SELECT d.region,
       d.currency,
       sum(d.order_count) AS order_count,
       sum(d.units) AS units,
       sum(d.gross_amount) AS gross_amount,
       sum(d.net_line_amount) AS net_line_amount,
       round(avg(d.avg_line_amount), 2) AS avg_line_amount,
       CASE
         WHEN sum(d.net_line_amount) >= 500000 THEN 'tier-1'
         WHEN sum(d.net_line_amount) >= 100000 THEN 'tier-2'
         ELSE 'tier-3'
       END AS region_tier
FROM ads_daily_sales d
GROUP BY d.region, d.currency;
