# metaxisdata demo（中文）

一个自包含的 [metaxisdata](https://github.com/Ranxy/metaxisdata) 演示环境。**一条 `docker compose up`
拉起一个小型数据平台（MySQL → PostgreSQL → StarRocks），在上面跑一条 Airflow 管道，把平台喂满，
交给你一个可以直接探索的环境。**

这里的一切都不是生产形态，所有凭据都是本地一次性值。

- 走查文档：见下文 · English: [README.md](README.md)
- 设计取舍：[docs/design.zh.md](docs/design.zh.md) · [English](docs/design.md)

---

## 你会得到什么

```
                        ┌──────────────────────────────────────────────┐
   浏览器 ─────────────▶│  metaxisdata :8083（server + 内嵌前端）        │
  localhost:8083        └───────┬────────────────────────┬─────────────┘
                                │ 元数据采集              │ POST /api/v1/lineage
        ┌───────────────┬───────┴────────┬───────────────┴──────────┐
        ▼               ▼                ▼                          ▼
 ┌─────────────┐ ┌────────────────┐ ┌──────────────────┐  ┌──────────────────┐
 │ mysql-shop  │ │postgres-       │ │ starrocks-serving│  │ metadata-db      │
 │ MySQL 8     │ │warehouse       │ │ StarRocks        │  │ 平台自身存储       │
 │ 库: shop    │ │ 库: warehouse  │ │ 库: ods / ads    │  └──────────────────┘
 │ 8 表 3 视图 │ │ ods/dwd/ads    │ │ 5 表             │
 │             │ │ 12 视图 1 物化 │ │ 2 视图 1 物化    │
 └──────▲──────┘ └───────▲────────┘ └────────▲─────────┘
        │                │                   │
        │        ┌───────┴───────────────────┴────────┐
        └────────┤  airflow :8080（standalone）         │
                 │  ingest_shop_to_warehouse          │  SQL facet ──┐
                 │  build_warehouse_layers            │  列级 facet ──┤
                 │  publish_to_serving                │               │
                 │  build_serving_aggregates          │               ▼
                 └────────────────────────────────────┘
   一次性： warehouse-init → serving-init → bootstrap → etl-runner → smoke
```

所有准备工作都自动完成：数据库灌好数据、平台注册并采集三个数据源、管道跑完、走查文档提到的样例也都建好。

## 环境要求

- Docker + Compose v2，启用 BuildKit
- **约 10GB 可用内存**、**约 8GB 磁盘**（StarRocks 占大头）
- 宿主端口 **8083**（平台）与 **8080**（Airflow）空闲。**demo 数据库默认不发布端口**，因此不会和本机已有服务冲突。

## 快速开始

```bash
git clone <本仓库> && cd metaxisdata-demo
make up
```

`make up` 会在首次运行时把 `.env.example` 复制成 `.env`，构建 Airflow 镜像，拉起全部组件，等管道跑完，
最后执行一次「环境是否已填充」的检查。冷启动需要拉取约 5GB 镜像，约 5–10 分钟；之后再启动会快很多。

成功时最后几行会列出检查结果。然后打开 **<http://localhost:8083>** 登录：

| 位置 | 账号 |
| --- | --- |
| metaxisdata（http://localhost:8083） | `admin@demo.local` / `Demo-admin-1234!` |
| Airflow（http://localhost:8080） | `admin` / `admin` |

```bash
make smoke      # 环境是否已填充？（简短报告，未填充则非零退出）
make logs       # 跟踪日志
make down       # 停止，保留数据
make reset      # 停止并删除数据卷；之后 `make up` 就是全新环境
```

---

## 走查

### 1. 三种不同引擎的元数据采集

**Data → Instances**：应看到 `mysql-shop`（MySQL）、`postgres-warehouse`（PostgreSQL）、
`starrocks-serving`（StarRocks），且都有较新的同步时间 —— 同一套注册表采集三种引擎。

打开 **Metadata**，选 MySQL 实例里的 `shop.orders`，查看列、索引、DDL 与大小。然后执行：

```bash
make evolve
```

它会给 `shop.orders` 加一列并重新同步。回到同一张表的 **history**，会多出一条摘要为 `+1 column`
的记录并带 diff —— 这正是维护元数据注册表的意义。

### 2. 由 SQL 推导的血缘

**Metadata → postgres-warehouse → `warehouse.dwd.v_order_line` → Lineage。**

数仓的视图、物化视图与表都由管道创建。平台读取它们的定义并自行推导图：上游能追到
`warehouse.ods.order_items`，下游经 `warehouse.dwd.mv_daily_sales` 到 `warehouse.ads.ads_daily_sales`。
边带类型标注（`DIRECT`、`AGGREGATE`、`JOIN`、`GROUP`），展开节点可看列级链路。

fixture 的 SQL 是刻意复杂的（多 CTE `INSERT … SELECT`、`GROUPING SETS`、`LATERAL`、窗口上的 `FILTER`、
`generate_series`、数组与 jsonb），所以这张图是在压分析器，而不是玩具 schema。

### 3. 端到端五跳

**Metadata → `shop.orders`（MySQL）→ Lineage**，然后逐层展开。链路跨三个引擎：

```
shop.orders → warehouse.ods.orders → v_order_base / v_order_line
  → mv_daily_sales / dwd_* → StarRocks ods.dwd_order_fact → ads.v_sr_* / ads.mv_sr_region_daily
```

约 400 条列级边，且没有任何节点落在三个实例之外：如果管道访问数据库时用的主机名与实例注册的不一致，
第一跳就会变成 *external* 数据集。

### 4. 由 OpenLineage 推导的血缘

**OpenLineage → Overview / Jobs / Datasets / Events**。管道的每个 task 都会上报一个 OpenLineage run；
`INSERT … SELECT` 的 run 带 SQL facet，跨库搬运的 run 带列级 facet。

打开 `build_warehouse_layers` 的某个 run，注意三件事：

- 它读写了哪些数据集，并且被解析到了注册表里的真实对象，而不是留下不可读的字符串；
- **Airflow 深链**，可直接跳回 `localhost:8080` 的 task 日志；
- 物化视图 task 上的 **extraction warning**：`REFRESH MATERIALIZED VIEW` 是 Airflow 自己的 SQL 解析器
  无法解析的语句。平台把它显性化，而不是悄悄丢掉这段血缘 —— 这条告警是预期行为，不是故障。

### 5. 哪些应用在读哪些表

**Application SQL** 列出的语句来自应用上报，而不是从数据库推断：demo 注册了一个应用、给它签了 key，
并上报它实际执行的参数化 SQL。**Manual SQL** 放的是「有人写在 wiki 里而不是建成视图」的语句 ——
两种情况注册表都会分析。

两者都已为你建好，也都出现在血缘图里。

### 6. 治理

**Settings → Users / Groups / Roles / IAM / Audit Log**，以及侧边栏的 **Notifications**。demo 预置了
第二个用户（`analyst@demo.local` / `Analyst-1234!`）、一个组和一个自定义角色；审计日志里已经有引导过程
与这次走查的全部操作 —— 连「查看审计日志」本身都会被审计。

### 7. 一个不是 Airflow 的生产者

平台接受任何说 OpenLineage 协议的生产者发来的事件。这里的生产者在模拟一个 Spark 作业，用的命名空间
不属于任何实例：

```bash
make ol-demo
```

第一条事件会落成 `external:spark-prod:…` 数据集，同时管理员收到「命名空间未映射」的通知。随后脚本创建
命名空间映射并重发同一事件，数据集就解析到 `starrocks-serving`，图上出现一条连接两个真实对象的边。
把别的团队的生产者接进注册表靠的是这条映射，而不是改代码。

---

## 不包含的内容

把 demo 的边界写清楚：

- **Explain SQL** 需要 LLM provider。在 *Settings → LLM Providers* 里加一个（任何 OpenAI 兼容端点都可以，
  包括本地跑的），页面即可用。
- **SQL Server 与 MariaDB** 平台支持，但本 demo 没有启动它们；加进来意味着两个额外服务，并按
  `scripts/bootstrap.py` 的方式注册。
- **MCP** 与 `mxd` CLI 在平台仓库里，且需要外部可访问的地址，见
  [docs/mcp.md](https://github.com/Ranxy/metaxisdata/blob/main/docs/mcp.md)。
- **DataX** 没有使用：它的上游二进制没有可靠的公开来源。demo 用一个自带的 operator 搬运数据
  （`airflow/plugins/sql_copy_operator.py`），DataX 那条路径的参考仍是平台的 `test/e2e_etl`。

## 故障排查

| 现象 | 处理 |
| --- | --- |
| `make up` 最后失败 | 再跑一次 `make smoke`，它会指出哪一项没过。`make logs` 看整栈，`docker compose logs etl-runner` 看管道。 |
| StarRocks 反复重启，或管道最后一跳失败 | StarRocks 自己要吃约 4GB 内存。给 Docker 更多内存，或去掉 `starrocks-serving`、`serving-init`、`publish_to_serving`、`build_serving_aggregates` 四个服务。 |
| 端口被占用 | 在 `.env` 里改 `METAXISDATA_PORT` / `AIRFLOW_PORT`。 |
| 管道跑了但平台没有新内容 | 同步发生在管道末尾，所以中途失败的 run 会让注册表落后。重跑：`docker compose up -d etl-runner && docker compose wait etl-runner`。 |
| Apple Silicon | StarRocks 镜像只有 `amd64`，会走模拟：能跑，但慢。 |

## 凭据说明

仓库里的一切都是本地一次性值：数据库 `demo` / `demo`，平台 `admin@demo.local` / `Demo-admin-1234!`，
Airflow `admin` / `admin`。请不要在本 demo 之外使用。

运行时生成的 key（OpenLineage 摄入 key、应用 key）都落在 `state/` 这个 Docker 卷里，绝不进仓库；
`make check-secrets` 会在发现真实凭据时直接失败。

## 目录结构

| 路径 | 说明 |
| --- | --- |
| `docker-compose.yml` | 整栈定义 |
| `fixture/` | 从平台端到端血缘测试 vendoring 来的 SQL |
| `airflow/dags/`、`airflow/plugins/` | 四个管道 DAG 与跨库搬运 operator |
| `scripts/` | 引导、样例、smoke 检查、schema 变更、OpenLineage 生产者 |
| `docs/design.zh.md` | 设计方案与取舍 |
