-- ods (PostgreSQL): landing tables for the MySQL -> DataX hop.
-- Types mirror what DataX's mysqlreader produces (MySQL TINYINT arrives as a
-- Java Long, so the target column is INTEGER rather than BOOLEAN/SMALLINT).
-- Run inside database warehouse (schema ods).

CREATE SCHEMA IF NOT EXISTS ods;
SET search_path TO ods;

CREATE TABLE IF NOT EXISTS ods.regions (
  country      CHAR(2)       NOT NULL,
  country_name VARCHAR(64)   NOT NULL,
  region       VARCHAR(32)   NOT NULL,
  sub_region   VARCHAR(32)   NOT NULL,
  is_emerging  INTEGER       NOT NULL DEFAULT 0,
  PRIMARY KEY (country)
);

CREATE TABLE IF NOT EXISTS ods.sales_reps (
  rep_id     INTEGER      NOT NULL,
  rep_name   VARCHAR(64)  NOT NULL,
  team       VARCHAR(32)  NOT NULL,
  manager_id INTEGER      NULL,
  hired_on   DATE         NOT NULL,
  PRIMARY KEY (rep_id)
);

CREATE TABLE IF NOT EXISTS ods.customers (
  customer_id  BIGINT        NOT NULL,
  customer_code VARCHAR(20)   NOT NULL,
  full_name    VARCHAR(100)  NOT NULL,
  email        VARCHAR(200)  NULL,
  phone        VARCHAR(32)   NULL,
  city         VARCHAR(64)   NOT NULL,
  country      CHAR(2)       NOT NULL,
  signup_date  DATE          NOT NULL,
  status       VARCHAR(16)   NOT NULL,
  loyalty_tier VARCHAR(16)   NOT NULL,
  credit_limit NUMERIC(12,2) NOT NULL DEFAULT 0,
  created_at   TIMESTAMP     NOT NULL,
  updated_at   TIMESTAMP     NULL,
  PRIMARY KEY (customer_id)
);

CREATE TABLE IF NOT EXISTS ods.products (
  product_id   BIGINT        NOT NULL,
  sku          VARCHAR(40)   NOT NULL,
  product_name VARCHAR(200)  NOT NULL,
  category     VARCHAR(64)   NOT NULL,
  sub_category VARCHAR(64)   NULL,
  brand        VARCHAR(64)   NOT NULL,
  unit_price   NUMERIC(12,2) NOT NULL,
  unit_cost    NUMERIC(12,2) NOT NULL,
  weight_kg    NUMERIC(8,3)  NULL,
  is_active    INTEGER       NOT NULL DEFAULT 1,
  launched_on  DATE          NULL,
  PRIMARY KEY (product_id)
);

CREATE TABLE IF NOT EXISTS ods.orders (
  order_id        BIGINT        NOT NULL,
  order_no        VARCHAR(32)   NOT NULL,
  customer_id     BIGINT        NOT NULL,
  sales_rep_id    INTEGER       NULL,
  order_ts        TIMESTAMP     NOT NULL,
  order_date      DATE          NOT NULL,
  status          VARCHAR(20)   NOT NULL,
  channel         VARCHAR(20)   NOT NULL,
  currency        CHAR(3)       NOT NULL,
  item_count      INTEGER       NOT NULL,
  gross_amount    NUMERIC(14,2) NOT NULL,
  discount_amount NUMERIC(14,2) NOT NULL DEFAULT 0,
  shipping_fee    NUMERIC(10,2) NOT NULL DEFAULT 0,
  tax_amount      NUMERIC(12,2) NOT NULL DEFAULT 0,
  net_amount      NUMERIC(14,2) NOT NULL,
  coupon_code     VARCHAR(32)   NULL,
  PRIMARY KEY (order_id)
);

CREATE TABLE IF NOT EXISTS ods.order_items (
  item_id        BIGINT        NOT NULL,
  order_id       BIGINT        NOT NULL,
  product_id     BIGINT        NOT NULL,
  quantity       INTEGER       NOT NULL,
  unit_price     NUMERIC(12,2) NOT NULL,
  discount_pct   NUMERIC(5,2)  NOT NULL DEFAULT 0,
  line_amount    NUMERIC(14,2) NOT NULL,
  warehouse_code VARCHAR(16)   NOT NULL,
  PRIMARY KEY (item_id)
);

CREATE TABLE IF NOT EXISTS ods.payments (
  payment_id  BIGINT        NOT NULL,
  order_id    BIGINT        NOT NULL,
  payment_ts  TIMESTAMP     NOT NULL,
  method      VARCHAR(24)   NOT NULL,
  amount      NUMERIC(14,2) NOT NULL,
  fee         NUMERIC(10,2) NOT NULL DEFAULT 0,
  currency    CHAR(3)       NOT NULL,
  status      VARCHAR(16)   NOT NULL,
  gateway_ref VARCHAR(64)   NULL,
  PRIMARY KEY (payment_id)
);

CREATE TABLE IF NOT EXISTS ods.refunds (
  refund_id   BIGINT        NOT NULL,
  payment_id  BIGINT        NOT NULL,
  order_id    BIGINT        NOT NULL,
  refund_ts   TIMESTAMP     NOT NULL,
  amount      NUMERIC(14,2) NOT NULL,
  reason_code VARCHAR(24)   NOT NULL,
  status      VARCHAR(16)   NOT NULL,
  PRIMARY KEY (refund_id)
);
