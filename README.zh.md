# metaxisdata demo（中文）

一个自包含的 [metaxisdata](https://github.com/Ranxy/metaxisdata) 演示环境：**一条 `docker compose up -d`
拉起一个小型数据平台（MySQL → PostgreSQL → StarRocks），跑一条 Airflow 管道，把平台喂满，让你打开就能探索。**

这里的一切都不是生产形态，所有凭据都是本地一次性值。

- 方案与决策：[docs/design.md](docs/design.md)（英文）· [docs/design.zh.md](docs/design.zh.md)（中文）
- English: [README.md](README.md)

## 能看到什么

| 想看的 | 去哪 |
| --- | --- |
| 从不同引擎采集元数据 —— MySQL、PostgreSQL、StarRocks | **Data → Instances**，再进元数据浏览 |
| 由 SQL 推导的血缘 —— 视图与物化视图 | `warehouse.dwd.v_order_line` 的 **Lineage** |
| 由 OpenLineage 推导的血缘 —— run / job / dataset / event、SQL facet、Airflow 深链 | **OpenLineage** |
| 端到端链路 —— 从 `shop.orders` 到 StarRocks 服务层视图，共五跳 | `shop.orders` 的 **Lineage** |
| 治理 —— 用户、组、角色、审计日志、通知 | **Settings** |

## 组成

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
 └──────▲──────┘ └───────▲────────┘ └────────▲─────────┘
        │                │                   │
        │        ┌───────┴───────────────────┴────────┐
        └────────┤  airflow :8080（standalone）         │
                 │  ingest_shop_to_warehouse          │
                 │  build_warehouse_layers            │
                 │  publish_to_serving                │
                 │  build_serving_aggregates          │
                 └────────────────────────────────────┘
```

整套初始化由一次性容器完成：`warehouse-init`（建 schema）→ `bootstrap`（平台账号、实例、同步、摄入 key、
样例 SQL）→ `etl-runner`（跑四个 DAG）→ `smoke`。

## 环境要求

- Docker + Compose v2，启用 BuildKit
- 默认栈需要约 10GB 可用内存、约 8GB 磁盘（StarRocks 占大头）
- 宿主端口 `8083` 与 `8080` 空闲。**默认不发布数据库端口**，避免与本机已有服务冲突。

## 快速开始

```bash
cp .env.example .env
make up          # 构建 Airflow 镜像、拉起全部组件、灌数据、跑管道
```

然后打开 <http://localhost:8083>，用 `make up` 打印的 demo 管理员账号登录（见 `.env.example`）。

想直接看管道本身：Airflow 在 <http://localhost:8080>。

```bash
make smoke       # 演示环境是否已填充？（简短报告）
make logs        # 跟踪日志
make reset       # 连同数据卷一起清空并重来
```

## 可选 profile

```bash
make up PROFILE=extra   # 增加 SQL Server 与 MariaDB，多两种采集引擎
make up PROFILE=llm     # 增加本地 OpenAI 兼容端点，Explain SQL 可离线使用
make up PROFILE=mcp     # 增加 mxd CLI：设备登录、MCP 工具、OAuth 同意页
make up PROFILE=ports   # 把 demo 数据库发布到 1xxxx 端口，便于本地客户端查看
```

## 当前状态

正在 step by step 实现，每个 step 一次提交；计划见 [docs/design.zh.md §11](docs/design.zh.md)。
目前完成 Step 0（仓库骨架与文档）与 Step 1（平台骨架）；完整走查文档随 Step 6 落地。

## 凭据说明

仓库里的一切都是本地一次性值，详见 [docs/design.zh.md §7.3](docs/design.zh.md)。运行时生成的 key
（OpenLineage 摄入 key、应用 key）都在 `state/` 卷里，该目录已 gitignore。请不要在本 demo 之外使用这些凭据。
