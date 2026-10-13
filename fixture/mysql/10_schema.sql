-- shop: MySQL source layer for the MySQL -> DataX -> PostgreSQL -> DataX -> StarRocks
-- lineage integration test. Column names are kept identical across the DataX hops so
-- that the OpenLineage schema facet infers column-level lineage.

CREATE DATABASE IF NOT EXISTS shop DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci;
USE shop;

DROP TABLE IF EXISTS refunds;
DROP TABLE IF EXISTS payments;
DROP TABLE IF EXISTS order_items;
DROP TABLE IF EXISTS orders;
DROP TABLE IF EXISTS products;
DROP TABLE IF EXISTS customers;
DROP TABLE IF EXISTS sales_reps;
DROP TABLE IF EXISTS regions;

CREATE TABLE regions (
  country      CHAR(2)       NOT NULL,
  country_name VARCHAR(64)   NOT NULL,
  region       VARCHAR(32)   NOT NULL,
  sub_region   VARCHAR(32)   NOT NULL,
  is_emerging  TINYINT(1)    NOT NULL DEFAULT 0,
  PRIMARY KEY (country)
) ENGINE = InnoDB;

CREATE TABLE sales_reps (
  rep_id     INT          NOT NULL,
  rep_name   VARCHAR(64)  NOT NULL,
  team       VARCHAR(32)  NOT NULL,
  manager_id INT          NULL,
  hired_on   DATE         NOT NULL,
  PRIMARY KEY (rep_id)
) ENGINE = InnoDB;

CREATE TABLE customers (
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
  credit_limit DECIMAL(12,2) NOT NULL DEFAULT 0,
  created_at   DATETIME      NOT NULL,
  updated_at   DATETIME      NULL,
  PRIMARY KEY (customer_id),
  UNIQUE KEY uk_customers_code (customer_code),
  KEY idx_customers_country (country)
) ENGINE = InnoDB;

CREATE TABLE products (
  product_id   BIGINT        NOT NULL,
  sku          VARCHAR(40)   NOT NULL,
  product_name VARCHAR(200)  NOT NULL,
  category     VARCHAR(64)   NOT NULL,
  sub_category VARCHAR(64)   NULL,
  brand        VARCHAR(64)   NOT NULL,
  unit_price   DECIMAL(12,2) NOT NULL,
  unit_cost    DECIMAL(12,2) NOT NULL,
  weight_kg    DECIMAL(8,3)  NULL,
  is_active    TINYINT(1)    NOT NULL DEFAULT 1,
  launched_on  DATE          NULL,
  PRIMARY KEY (product_id),
  UNIQUE KEY uk_products_sku (sku),
  KEY idx_products_category (category)
) ENGINE = InnoDB;

CREATE TABLE orders (
  order_id        BIGINT        NOT NULL,
  order_no        VARCHAR(32)   NOT NULL,
  customer_id     BIGINT        NOT NULL,
  sales_rep_id    INT           NULL,
  order_ts        DATETIME      NOT NULL,
  order_date      DATE          NOT NULL,
  status          VARCHAR(20)   NOT NULL,
  channel         VARCHAR(20)   NOT NULL,
  currency        CHAR(3)       NOT NULL,
  item_count      INT           NOT NULL,
  gross_amount    DECIMAL(14,2) NOT NULL,
  discount_amount DECIMAL(14,2) NOT NULL DEFAULT 0,
  shipping_fee    DECIMAL(10,2) NOT NULL DEFAULT 0,
  tax_amount      DECIMAL(12,2) NOT NULL DEFAULT 0,
  net_amount      DECIMAL(14,2) NOT NULL,
  coupon_code     VARCHAR(32)   NULL,
  PRIMARY KEY (order_id),
  UNIQUE KEY uk_orders_no (order_no),
  KEY idx_orders_customer (customer_id),
  KEY idx_orders_date (order_date)
) ENGINE = InnoDB;

CREATE TABLE order_items (
  item_id        BIGINT        NOT NULL,
  order_id       BIGINT        NOT NULL,
  product_id     BIGINT        NOT NULL,
  quantity       INT           NOT NULL,
  unit_price     DECIMAL(12,2) NOT NULL,
  discount_pct   DECIMAL(5,2)  NOT NULL DEFAULT 0,
  line_amount    DECIMAL(14,2) NOT NULL,
  warehouse_code VARCHAR(16)   NOT NULL,
  PRIMARY KEY (item_id),
  KEY idx_items_order (order_id),
  KEY idx_items_product (product_id)
) ENGINE = InnoDB;

CREATE TABLE payments (
  payment_id  BIGINT        NOT NULL,
  order_id    BIGINT        NOT NULL,
  payment_ts  DATETIME      NOT NULL,
  method      VARCHAR(24)   NOT NULL,
  amount      DECIMAL(14,2) NOT NULL,
  fee         DECIMAL(10,2) NOT NULL DEFAULT 0,
  currency    CHAR(3)       NOT NULL,
  status      VARCHAR(16)   NOT NULL,
  gateway_ref VARCHAR(64)   NULL,
  PRIMARY KEY (payment_id),
  KEY idx_payments_order (order_id)
) ENGINE = InnoDB;

CREATE TABLE refunds (
  refund_id   BIGINT        NOT NULL,
  payment_id  BIGINT        NOT NULL,
  order_id    BIGINT        NOT NULL,
  refund_ts   DATETIME      NOT NULL,
  amount      DECIMAL(14,2) NOT NULL,
  reason_code VARCHAR(24)   NOT NULL,
  status      VARCHAR(16)   NOT NULL,
  PRIMARY KEY (refund_id),
  KEY idx_refunds_payment (payment_id)
) ENGINE = InnoDB;
