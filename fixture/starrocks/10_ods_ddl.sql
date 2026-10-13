-- StarRocks ods: landing tables for the PostgreSQL -> DataX hop. They must
-- exist before DataX runs (starrockswriter is a Stream Load into an existing
-- table).
-- Apply with: mysql -uroot -h127.0.0.1 -P9030 < 10_ods_ddl.sql

CREATE DATABASE IF NOT EXISTS ods;

DROP TABLE IF EXISTS ods.ads_customer_segment;
DROP TABLE IF EXISTS ods.ads_daily_sales;
DROP TABLE IF EXISTS ods.mv_daily_sales;
DROP TABLE IF EXISTS ods.dwd_customer_360;
DROP TABLE IF EXISTS ods.dwd_order_fact;

CREATE TABLE ods.dwd_order_fact (
  order_id         BIGINT        NOT NULL,
  order_no         VARCHAR(32)   NOT NULL,
  customer_id      BIGINT        NOT NULL,
  customer_code    VARCHAR(20)   NOT NULL,
  customer_name    VARCHAR(100)  NOT NULL,
  country          VARCHAR(2)    NOT NULL,
  region           VARCHAR(32)   NOT NULL,
  sub_region       VARCHAR(32)   NOT NULL,
  sales_rep_id     INT           NULL,
  rep_name         VARCHAR(64)   NULL,
  rep_team         VARCHAR(32)   NULL,
  order_date       DATE          NOT NULL,
  order_ts         DATETIME      NOT NULL,
  status           VARCHAR(20)   NOT NULL,
  status_group     VARCHAR(16)   NOT NULL,
  channel          VARCHAR(20)   NOT NULL,
  currency         VARCHAR(3)    NOT NULL,
  item_count       INT           NOT NULL,
  gross_amount     DECIMAL(14,2) NOT NULL,
  discount_amount  DECIMAL(14,2) NOT NULL,
  shipping_fee     DECIMAL(10,2) NOT NULL,
  tax_amount       DECIMAL(12,2) NOT NULL,
  net_amount       DECIMAL(14,2) NOT NULL,
  line_count       INT           NOT NULL,
  units            INT           NOT NULL,
  line_revenue     DECIMAL(14,2) NOT NULL,
  top_category     VARCHAR(64)   NULL,
  paid_amount      DECIMAL(14,2) NOT NULL,
  refund_amount    DECIMAL(14,2) NOT NULL,
  balance_due      DECIMAL(14,2) NOT NULL,
  payment_coverage DECIMAL(9,4)  NULL,
  customer_segment VARCHAR(16)   NOT NULL,
  month_net_rank   INT           NOT NULL,
  loaded_at        DATETIME      NOT NULL
)
DUPLICATE KEY(order_id)
DISTRIBUTED BY HASH(order_id) BUCKETS 3
PROPERTIES ("replication_num" = "1");

CREATE TABLE ods.dwd_customer_360 (
  customer_id      BIGINT        NOT NULL,
  customer_code    VARCHAR(20)   NOT NULL,
  customer_name    VARCHAR(100)  NOT NULL,
  country          VARCHAR(2)    NOT NULL,
  region           VARCHAR(32)   NOT NULL,
  sub_region       VARCHAR(32)   NOT NULL,
  city             VARCHAR(64)   NOT NULL,
  status           VARCHAR(16)   NOT NULL,
  loyalty_tier     VARCHAR(16)   NOT NULL,
  tenure_days      INT           NOT NULL,
  order_count      INT           NOT NULL,
  lifetime_gross   DECIMAL(14,2) NOT NULL,
  lifetime_net     DECIMAL(14,2) NOT NULL,
  avg_order_value  DECIMAL(14,2) NULL,
  lifetime_units   INT           NOT NULL,
  paid_amount      DECIMAL(14,2) NOT NULL,
  refund_amount    DECIMAL(14,2) NOT NULL,
  net_collected    DECIMAL(14,2) NOT NULL,
  balance_due      DECIMAL(14,2) NOT NULL,
  collection_rate  DECIMAL(9,4)  NULL,
  customer_segment VARCHAR(16)   NOT NULL,
  value_rank       INT           NOT NULL,
  loaded_at        DATETIME      NOT NULL
)
DUPLICATE KEY(customer_id)
DISTRIBUTED BY HASH(customer_id) BUCKETS 3
PROPERTIES ("replication_num" = "1");

CREATE TABLE ods.mv_daily_sales (
  sales_date      DATE          NOT NULL,
  channel         VARCHAR(20)   NOT NULL,
  region          VARCHAR(32)   NOT NULL,
  currency        VARCHAR(3)    NOT NULL,
  order_count     BIGINT        NOT NULL,
  customer_count  BIGINT        NOT NULL,
  units           BIGINT        NOT NULL,
  gross_amount    DECIMAL(16,2) NOT NULL,
  discount_amount DECIMAL(16,2) NOT NULL,
  net_line_amount DECIMAL(16,2) NOT NULL,
  avg_line_amount DECIMAL(14,2) NULL
)
DUPLICATE KEY(sales_date, channel, region, currency)
DISTRIBUTED BY HASH(sales_date) BUCKETS 3
PROPERTIES ("replication_num" = "1");

CREATE TABLE ods.ads_daily_sales (
  sales_date      DATE          NOT NULL,
  channel         VARCHAR(20)   NOT NULL,
  region          VARCHAR(32)   NOT NULL,
  currency        VARCHAR(3)    NOT NULL,
  order_count     BIGINT        NOT NULL,
  customer_count  BIGINT        NOT NULL,
  units           BIGINT        NOT NULL,
  gross_amount    DECIMAL(16,2) NOT NULL,
  discount_amount DECIMAL(16,2) NOT NULL,
  net_line_amount DECIMAL(16,2) NOT NULL,
  avg_line_amount DECIMAL(14,2) NULL,
  order_share_pct DECIMAL(9,4)  NULL,
  revenue_band    VARCHAR(16)   NOT NULL,
  loaded_at       DATETIME      NOT NULL
)
DUPLICATE KEY(sales_date, channel, region, currency)
DISTRIBUTED BY HASH(sales_date) BUCKETS 3
PROPERTIES ("replication_num" = "1");

CREATE TABLE ods.ads_customer_segment (
  customer_segment  VARCHAR(16)   NOT NULL,
  country           VARCHAR(2)    NOT NULL,
  region            VARCHAR(32)   NOT NULL,
  customer_count    BIGINT        NOT NULL,
  total_net         DECIMAL(16,2) NOT NULL,
  avg_net           DECIMAL(14,2) NULL,
  avg_orders        DECIMAL(10,2) NULL,
  vip_count         BIGINT        NOT NULL,
  segment_share_pct DECIMAL(9,4)  NULL,
  top_customer_id   BIGINT        NULL,
  top_customer_name VARCHAR(100)  NULL,
  loaded_at         DATETIME      NOT NULL
)
DUPLICATE KEY(customer_segment, country, region)
DISTRIBUTED BY HASH(customer_segment) BUCKETS 3
PROPERTIES ("replication_num" = "1");
