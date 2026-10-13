# metaxisdata demo（中文）

一个开箱即用的 **metaxisdata** 演示环境。metaxisdata 是自托管的数据治理与元数据平台。

一条命令就能拉起一个小型数据平台 —— MySQL、PostgreSQL、StarRocks —— 加上一条 Airflow 管道和平台本体，
数据与血缘都已经灌好，你只需要看。

- English: [README.md](README.md)

---

## 启动

需要 **Docker**（含 Compose v2）、约 **10GB 空闲内存**、**8GB 磁盘**，以及 **8083 与 8080 端口空闲**。

```bash
git clone <本仓库> && cd metaxisdata-demo
make up
```

这就是全部步骤。它会构建一个镜像、拉起全部组件、灌入数据、跑完管道并把平台喂满。
首次运行约 5–10 分钟，之后再启动会快很多。

就绪时最后一行会打印：

```
== smoke: the demo is populated
```

## 打开并登录

| 打开 | 登录 |
| --- | --- |
| **<http://localhost:8083>** —— metaxisdata | `admin@demo.local` / `Demo-admin-1234!` |
| **<http://localhost:8080>** —— Airflow，管道界面 | `admin` / `admin` |

这些只是本地演示用的一次性密码，这里的一切都不是生产形态。

## 你看到的是什么

```
  MySQL               PostgreSQL             StarRocks            Airflow
  mysql-shop    →     postgres-warehouse  →  starrocks-serving    :8080
  商城的订单等         数仓分层：             服务层：              跑四个管道步骤
                     ods → dwd → ads        建在上面的视图
      │                     │                      │                  │
      └─────────────────────┴──────────────────────┘                  │
                            │                                         │
                            ▼                                         ▼
                  metaxisdata  :8083  ←───────────────────────────────┘
                采集全部 schema，并画出数据血缘
```

![血缘图：选中一张表后，可以看到它的详情、上下游对象数量，以及每条关系来自哪种分析](assets/lineage-columns.png)

---

## 走一遍演示

### 1. 一套注册表，三种不同的数据库

**Data → Instances**：三个数据库已注册并采集完成，分别是 `mysql-shop`（MySQL）、
`postgres-warehouse`（PostgreSQL）、`starrocks-serving`（StarRocks）。

![实例管理：注册的三个数据库、各自的引擎与最近一次同步时间](assets/instances.png)

打开 **Metadata**，选 MySQL 实例里的 `shop.orders`：列、类型、索引、DDL、大小都在。然后执行

```bash
make evolve
```

它会给这张表加一列并重新同步。回到同一张表打开 **history**：这次变更被记录了下来，带 diff，摘要为
`+1 column`。

![元数据浏览中的 orders 表：列、索引、行数与大小](assets/metadata-table.png)

### 2. 由 SQL 推导出的血缘

打开 **Metadata → postgres-warehouse → `warehouse.dwd.v_order_line`**，再点 **Lineage**。

数仓的视图与物化视图都由管道创建；平台读取它们的定义并自行推导出这张图。向上可以追到
`warehouse.ods.order_items`，向下经过 `warehouse.dwd.mv_daily_sales` 到 `warehouse.ads.ads_daily_sales`。
边带有语义标注 —— `DIRECT`、`AGGREGATE`、`JOIN`、`GROUP`；展开表可以看到逐列的链路。

### 3. 一路跟到链路尽头

打开 **Metadata → `shop.orders`**（MySQL 那个），把 **Lineage** 向下展开。链路穿过三个引擎：

```
shop.orders → warehouse.ods.orders → v_order_base / v_order_line
  → mv_daily_sales / dwd_* → StarRocks ods.dwd_order_fact → ads.v_sr_* / ads.mv_sr_region_daily
```

### 4. 同一个故事，由 OpenLineage 讲述

打开 **OpenLineage → Jobs**，选 `build_warehouse_layers` 的某个 run。可以看到这个 task 读了哪些数据集、
写了哪些数据集，一条直达 task 日志的 **Airflow** 链接，以及产生这段血缘的 SQL。

物化视图那个 task 上有一条「语句无法解析」的告警 —— 这是 Airflow 自身 SQL 解析器的限制，平台选择把它显示出来，
而不是悄悄丢掉这段血缘。

![OpenLineage run：读写的数据集、跳转 Airflow 任务日志的链接，以及那条无法解析的语句](assets/openlineage-run.png)

### 5. 不是视图的语句

**Manual SQL** 放的是有人写在 wiki 里而不是建成视图的语句。**Application SQL** 放的是应用上报的、
它实际执行过的语句，因此可以看到哪个应用在读哪张表。两者 demo 里都已经有，并且都出现在血缘图中。

### 6. 人与权限

**Settings** 下有 **Users**、**Groups**、**Roles** 和 **Audit Log** —— 到目前为止的每个操作（包括你自己的）
都记录在里面。侧边栏的 **Notifications** 收集平台想告知管理员的事情。

### 7. 一个来自外部的生产者

平台接受任何说 OpenLineage 协议的生产者发来的事件。这里的生产者在模拟一个 Spark 作业，用的命名空间不属于任何数据库：

```bash
make ol-demo
```

第一条事件会以 `external:spark-prod:…` 数据集落地，管理员同时收到「命名空间未映射」的通知。随后脚本注册命名空间映射
并重发同一事件：数据集解析到 `starrocks-serving`，两个真实表之间出现了这条血缘边。

---

## 常用命令

| 命令 | 作用 |
| --- | --- |
| `make up` | 启动全部组件，并等到演示环境就绪 |
| `make smoke` | 检查演示环境是否已填充 |
| `make logs` | 跟踪日志 |
| `make down` | 停止，保留数据 |
| `make reset` | 停止并删除数据；之后 `make up` 就是全新环境 |
| `make evolve` | 加一列并重新同步，用来观察元数据历史 |
| `make ol-demo` | 运行外部生产者，分别演示「有/无命名空间映射」 |

## 遇到问题

| 现象 | 处理 |
| --- | --- |
| `make up` 最后失败 | 再跑 `make smoke`，它会指出缺了什么；`make logs` 看整栈。 |
| 中途停住，或 StarRocks 那一步失败 | StarRocks 自己要吃约 4GB 内存。给 Docker 更多内存后重新 `make up`。 |
| 端口被占用 | 在 `.env` 里改 `METAXISDATA_PORT` 或 `AIRFLOW_PORT`，再 `make up`。 |
| 第一次很慢 | 正在拉取约 5GB 镜像；之后再启动只需几秒。 |

## 更多

- [README.md](README.md) — 英文版同样内容
- [.agent/docs/design.zh.md](.agent/docs/design.zh.md) — 这个 demo 是怎么搭起来的（面向维护者）
