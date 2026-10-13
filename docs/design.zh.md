# metaxisdata demo — 方案设计（中文版）

> 英文版为主文档：[design.md](design.md)。本文件是其中文镜像，内容与英文版一致。
>
> 一句话：**一条 `docker compose up -d` 拉起 MySQL + PostgreSQL + StarRocks + Airflow + metaxisdata，
> 自动灌数据、跑管道、发 OpenLineage 事件，用户打开 <http://localhost:8083> 就能看到完整的演示环境。**

---

## 0. 结论速览

| 项 | 结论 |
| --- | --- |
| 交付形态 | 公开仓库 `metaxisdata-demo`，一条 compose 命令拉起整栈 |
| 平台 | **只用官方镜像** `ghcr.io/ranxy/metaxisdata:v0.1.1`，不从源码构建，保持轻量、专注「怎么用」 |
| 数据库 | MySQL 8（`shop`，OLTP）→ PostgreSQL 16（`warehouse`，ods/dwd/ads）→ StarRocks（`serving`，OLAP）+ 平台自身 PostgreSQL 存储 |
| ETL | Airflow 3.1 单容器 `airflow standalone` + 自带 `SqlCopyOperator` 承担跨库搬运 |
| 血缘来源 | SQL 分析器（视图/物化视图）、OpenLineage（SQL facet + 列级 facet）、手工 SQL、应用 SQL |
| 文档 | 英文为主（`README.md`、`docs/design.md`），中文镜像（`README.zh.md`、`docs/design.zh.md`）；代码注释一律英文 |
| 验证 | 刻意保持轻量：`make smoke` 只回答「演示环境是否已填充」。这是 demo，不是测试套件 |
| 密钥 | 仓库内零密钥；运行时生成的 key 落在被 gitignore 的 `state/` 卷 |

---

## 1. 目标与非目标

### 目标

1. 演示**不同数据库类型的元数据采集**。
2. 演示**基于 SQL 定义的血缘分析**。
3. 演示**基于 OpenLineage 的血缘分析**。
4. **一条 `docker compose up`** 拉起数据库 + ETL + 平台。
5. **全自动注入**：不需要手工注册实例、不需要逐条点选跑管道。
6. **尽可能覆盖产品功能面**，实在无法集成的明确写清楚。
7. **可复现、可重置**：`make up` / `make reset` / `make smoke`。
8. **可公开**：无凭据、无 token，不泄露任何真实部署信息。

### 非目标

- 不替代平台自身的集成测试；我们演示产品，不测产品。
- 不是生产拓扑；仓库里的凭据都是本地一次性值。
- 不从源码构建 metaxisdata，也不演示未发布的功能。
- 不追求覆盖平台支持的每一种引擎。

---

## 2. 设计前的调研结论（均已核实）

### 2.1 平台能力（关键项）

| 能力 | 事实 |
| --- | --- |
| 支持的引擎 | `MSSQL / MYSQL / POSTGRES / TIDB / MARIADB / OCEANBASE / STARROCKS / DORIS`（`proto/store/store/common.proto:16-30`，服务端白名单 `backend/api/v1/instance_service.go:185-194`） |
| 元数据采集 | 八种引擎都实现了实例与 schema 同步（`backend/plugin/db/{mysql,pg,starrocks,mssql}`） |
| SQL 血缘分析器 | 只有 MySQL 系、PostgreSQL、StarRocks；**MSSQL 没有分析器**（`backend/plugin/lineage/engines/engines.go:26-34`） |
| 建实例 | `CreateInstance` 必须恰好一个 `ADMIN` 数据源，且**成功即自动触发一次同步**（`backend/api/v1/instance_service.go:158-226`） |
| 采集账号 | 采集永远用 `ADMIN` 数据源，`READ_ONLY` 不参与（`backend/runner/schemasync/syncer.go:448,601`） |
| 首个账号 | 无用户时 `CreateUser` 免凭据；首个 `END_USER` 自动成为 `workspaceAdmin`（`backend/api/v1/user_service.go:243-270`） |
| 密码策略 | 会校验长度/数字/字母/大写/特殊字符（`backend/api/v1/user_service.go:277-292`） |
| 脚本化通道 | ConnectRPC `POST /metaxisdata.v1.<Service>/<Method>`（protojson）与 grpc-gateway `/v1/*` 并存；**两者 body 形状不同**，脚本统一用 Connect |
| CSRF | 带 `Authorization` 的请求豁免；无 Cookie 的请求放行（`backend/server/csrf.go:29-34`） |
| OL 摄入 | `POST /api/v1/lineage`（`/batch` 同义），纯 REST，**必须 `Authorization: Bearer ol_…`**；只接受 RunEvent；**只有 `COMPLETE` 事件产生血缘**（`backend/api/v1/openlineage_handler.go:148-166`、`backend/plugin/openlineage/processor.go:48-72`） |
| 带 SQL 的事件 | 带 `job.facets.sql` 的事件由**服务端自己的分析器**重算，生产者上报的 `columnLineage` facet 被**故意忽略**（`backend/plugin/openlineage/sql_facet_lineage.go`） |
| 命名空间未映射 | 不报错：回退为 `external:<namespace>:<name>`，并给管理员发去重通知（`backend/plugin/openlineage/resolver.go:120-213`） |
| Airflow 深链 | 由事件的 `run.facets.airflow.taskInstance.log_url` 自动派生（`backend/plugin/openlineage/airflow_links.go:9-47`） |
| 血缘读接口 | `GET /v1/lineages`（单对象）、`GET /v1/lineages:graph`（depth 1~10）、`POST /v1/lineages:counts`；列级血缘在每条 relation 内部，不是独立 RPC |
| 健康/版本 | `GET /healthz` → `OK`；`GET /api/version` → `{version,git_commit,build_time}`；均匿名 |
| 进程环境变量 | 只有 `PG_URL`（必需）、`METAXISDATA_ENCRYPTION_KEY`、`METAXISDATA_ENCRYPTION_KEY_PREVIOUS`；其余 `METAXISDATA_*` 是镜像 entrypoint → flag 的映射 |
| 存储 | 启动即自动迁移；数据库用户需要建表与建扩展（`pg_trgm`）权限 |

### 2.2 已发布镜像 v0.1.1

- GHCR 上的 tag：`v0.1.1`、`0.1.1`、`sha-ffc38ba`、`latest`（都指向同一个 digest `sha256:db3b75d2…`），另有旧的 `v0.1.0` 系列。
- `v0.1.1` = commit `ffc38ba`，构建于 2026-10-13。
- 对镜像内二进制做特性探测：`applicationSql`、`application-sql`、`identityProviders`、`IdentityProviderService`、`manualSqls`、`namespaceMappings`、`deviceLogins`、`mcp_enabled` **全部存在**。
- 因此**不需要能力探测**：本方案里的所有场景在固定镜像上都可用。

### 2.3 参考实现 `test/e2e_etl`

| 事实 | 说明 |
| --- | --- |
| 链路 | MySQL `e2e_ods`（8 表 + 3 视图）→ PG `e2e.e2e_ods` → PG `e2e_dwd`（6 视图 + 1 物化视图 + 2 表）/ `e2e_ads` → StarRocks `e2e_ods`（5 表）→ StarRocks `e2e_ads`（2 视图 + 1 异步物化视图）；4 个 DAG、26 个 task、约 50 秒 |
| 数据（seed `20260407`） | 300 customers / 100 products / 2000 orders / 4407 order_items / 1823 payments / 89 refunds / 12 regions / 12 sales_reps；派生 2000 fact / 300 customer / 1921 daily-sales |
| 血缘规模 | 每跳 distinct 关系对：MySQL 内部 7 · MySQL→PG 8 · PG 内部 34 · PG→StarRocks 5 · StarRocks 内部 4；分析对象 19 个；每条链路 30 个 OL run |
| 分析器压力面 | 多 CTE `INSERT … SELECT`、跨 3 schema join、窗口函数、`FILTER`、`DISTINCT ON`、`GROUPING SETS`、`LATERAL`、`generate_series`、数组/jsonb、StarRocks 异步物化视图 |
| 需要去掉的宿主耦合 | 容器名 `mysql8-db-1`/`pg16-db-1`/`starrocks`；`localhost`/`127.0.0.1`；Airflow home 与 DataX 的绝对路径；task 内的 `docker exec` |
| fixture 建的账号 | **没有**——MySQL `dev/dev`、PG `dev` 角色都得我们自己创建 |

### 2.4 硬约束

| # | 约束 | 影响 |
| --- | --- | --- |
| C1 | OL 摄入必须有 API Key，而 key 只能在平台引导之后签发 → Airflow 的 transport 配置必须**运行时生成** | 用 `state/ol.env` + entrypoint 等待解决 |
| C2 | 数据集解析靠实例注册的 `host:port` **字符串**匹配（只折叠 `localhost`/`127.0.0.1`/`::1`），不一致会**静默**变成 external dataset | 命名必须单一来源 |
| C3 | 未限定 schema 的 SQL 会静默丢血缘 | DAG 内所有语句全限定 |
| C4 | `REFRESH MATERIALIZED VIEW`、`CREATE MATERIALIZED VIEW … WITH DATA` 上游解析器不支持，只体现为提取告警 | 视为预期，写进文档（正好演示平台把上游丢失显性化） |
| C5 | StarRocks allin1 镜像 3.15GB、约 4GB 内存、启动 30~60 秒、仅 amd64 | `mem_limit: 4g` + healthcheck + `lite` profile |
| C6 | 本机已占用 5432/9030/8030/1433 端口 | 默认不发布数据库端口 |
| C7 | DataX 无可靠公开二进制（官方 OSS 403）、其 provider 不在 PyPI | DataX 只能作为可选 profile |

### 2.5 本机环境

Docker 29.9.0 / Compose 5.6.0，24 核 / 45GB RAM / 628GB 可用磁盘；`ghcr.io/ranxy/metaxisdata`、`starrocks/allin1-ubuntu:4.1-latest`、`mysql:8.x`、`postgres:16`、`mssql:2019` 都已缓存；Docker Hub 与 PyPI 可达。

---

## 3. 架构

### 3.1 拓扑

```
                        ┌──────────────────────────────────────────────┐
   浏览器 ─────────────▶│  metaxisdata :8083（server + 内嵌前端）        │
  localhost:8083        └───────┬────────────────────────┬─────────────┘
                                │ 元数据采集              │ POST /api/v1/lineage
        ┌───────────────┬───────┴────────┬───────────────┴──────────┐
        ▼               ▼                ▼                          ▼
 ┌─────────────┐ ┌────────────────┐ ┌──────────────────┐  ┌──────────────────┐
 │ mysql-shop  │ │postgres-       │ │ starrocks-serving│  │ metadata-db      │
 │ MySQL 8     │ │warehouse       │ │ StarRocks allin1 │  │ 平台自身存储（不发布）│
 │ 库: shop    │ │ PG 16          │ │ 库: ods / ads    │  └──────────────────┘
 │ 8 表 3 视图 │ │ 库: warehouse  │ │ 5 表              │
 └──────▲──────┘ └───────▲────────┘ └────────▲─────────┘
        │                │                   │
        │        ┌───────┴───────────────────┴────────┐
        └────────┤  airflow :8080（standalone）         │
                 │  ingest_shop_to_warehouse          │  SQL facet ──┐
                 │  build_warehouse_layers            │  列级 facet ──┤
                 │  publish_to_serving                │               │
                 │  build_serving_aggregates          │               │
                 └────────────────────────────────────┘               ▼
   一次性容器：  warehouse-init  →  bootstrap  →  etl-runner  →  smoke
```

### 3.2 服务清单

| 服务 | 镜像 | 宿主端口 | 作用 |
| --- | --- | --- | --- |
| `metadata-db` | `postgres:16-alpine` | — | 平台自身元数据库 |
| `metaxisdata` | `ghcr.io/ranxy/metaxisdata:${METAXISDATA_VERSION:-v0.1.1}` | `8083:8083` | 平台 server + 前端 |
| `mysql-shop` | `mysql:8.4` | —（可选 `13306`） | OLTP 源库，数据库 `shop`，由 `fixture/mysql/` 初始化 |
| `postgres-warehouse` | `postgres:16` | —（可选 `15432`） | 数仓，数据库 `warehouse`，schema `ods`/`dwd`/`ads` |
| `starrocks-serving` | `starrocks/allin1-ubuntu:4.1-latest`，`mem_limit: 4g` | —（可选 `19030`/`18030`） | OLAP 服务层，数据库 `ods`/`ads` |
| `airflow` | `docker/Dockerfile.airflow`（`apache/airflow:3.1.8-python3.12` + providers） | `8080:8080` | `airflow standalone`：webserver + scheduler + triggerer |
| `warehouse-init` | `docker/Dockerfile.tools` | — | 等健康后建 PG schema、下 StarRocks DDL |
| `bootstrap` | 同一 tools 镜像 | — | 建管理员、注册实例、同步、签发摄入 key、灌手工 SQL/应用 SQL/治理样例 |
| `etl-runner` | Airflow 镜像 | — | 按序触发 4 个 DAG 并等待 |
| `smoke` | tools 镜像 | — | 轻量检查环境是否已填充 |
| `[mssql-erp]` `[mariadb-crm]` | `mssql:2019-latest`、`mariadb:11` | — | `extra` profile：多两种引擎（SQL Server 仅元数据） |
| `[ollama]` | `ollama/ollama` | `11434:11434` | `llm` profile：本地 OpenAI 兼容端点，Explain SQL 可离线 |
| `[mxd]` | demo 工具链构建 | — | `mcp` profile：设备登录与 CLI 读回 |

### 3.3 Profile 划分

| Profile | 增加内容 | 适用 |
| --- | --- | --- |
| （默认） | 上述全部，含 StarRocks | 完整体验：三引擎 + 五跳血缘 + OpenLineage |
| `lite` | 去掉 `starrocks-serving`（DAG 3/4 自动跳过） | 内存紧张（< ~10GB） |
| `extra` | `mssql-erp`、`mariadb-crm` | 强调「多数据库类型」 |
| `llm` | `ollama` | 无云端 key 演示 Explain SQL |
| `mcp` | `mxd` | 设备登录 `/device`、MCP 工具、OAuth 同意页 |
| `datax` | DAG 1/3 的 DataX 版本 | 保真复刻 `test/e2e_etl`（需本机 DataX） |
| `ports` | 把数据库端口映射到 1xxxx | 用本地客户端连进去看数据 |

### 3.4 资源预算

| 组件 | 常驻内存 | 镜像体积 |
| --- | --- | --- |
| `metadata-db` + `metaxisdata` | ~400MB | ~0.3GB |
| `mysql-shop` + `postgres-warehouse` | ~800MB | ~1.1GB |
| `starrocks-serving` | ~3.5GB | ~3.15GB |
| `airflow` | ~1.5GB | ~1.3GB |
| **默认合计** | **≈ 6.2GB（建议可用 ≥ 10GB）** | **≈ 5.9GB** |
| `extra` | +2.2GB | +1.9GB |
| `datax` | +0.5GB | +0.4GB |

---

## 4. 关键决策

| # | 决策 | 理由 |
| --- | --- | --- |
| **D1** | **只用官方镜像**，用 `METAXISDATA_VERSION` 固定（默认 `v0.1.1`） | demo 仓库公开且与源码仓库解耦；固定 tag 可复现，`latest` 会漂移 |
| **D2** | 默认 ETL = **Airflow + 自带 `SqlCopyOperator` + `SQLExecuteQueryOperator`**；DataX 降级为可选 profile | DataX 无可靠二进制、需要 JVM；demo 的主角是 metaxisdata |
| **D3** | Airflow 用**单容器 `airflow standalone`** | 演示场景下组件越少越稳；`etl-runner` 共享镜像走 CLI，不需要 API 鉴权 |
| **D4** | 引导走 **ConnectRPC + Bearer token** | gateway 与 Connect 的 body 形状不同易踩坑；带 Authorization 豁免 CSRF；`mxd` CLI 不能建实例/同步 |
| **D5** | **摄入 key 运行时生成**：`bootstrap` 写 `state/ol.env`，Airflow entrypoint 等文件出现再启动 | 解决 C1，且不需要二次重启 Airflow |
| **D6** | **命名单一来源**：`.env` 同时喂给实例注册、Airflow connection、copy 任务与 DAG | 直接对应 C2：任何一处拼写不一致都会静默产生 external dataset |
| **D7** | **复用 fixture 的 SQL**，只重命名数据库/schema | 1MB、确定性、已覆盖分析器全部难点、行数可对账 |
| **D8** | 默认**不发布数据库端口**；`ports` profile 映射到 1xxxx | 本机端口已被占用（C6） |
| **D9** | 验证只做**轻量 `make smoke`** | 需求是 demo 不是测试：只回答「环境是否已填充」 |
| **D10** | 所有一次性服务**幂等**：先查后建、「已存在」视为成功、装载前先清空 | `docker compose up` 会被反复执行 |
| **D11** | StarRocks 保留在默认 profile，`mem_limit: 4g` + 自带 healthcheck | 多引擎 + 多跳血缘是 demo 的核心卖点 |
| **D12** | 手工 SQL 与应用 SQL 由 `bootstrap` 一并灌入 | 两条低成本血缘入口，多点亮两个页面 |
| **D13** | Explain SQL 用**本地 OpenAI 兼容容器**（`llm` profile）；SSO 只文档说明 | 保持自包含；IdP 页面能渲染，但完整 SSO 登录需要外部 IdP |
| **D14** | **仓库零密钥**：只有一次性 demo 凭据，运行时 key 落 `state/`，并提供 `make check-secrets` | 仓库公开 |

---

## 5. 演示场景与功能覆盖

| 场景 | 动作 | 应看到什么 |
| --- | --- | --- |
| **S1** 多引擎元数据采集 | Data → Instances → `shop.orders` 详情 | 三个实例（MySQL / PostgreSQL / StarRocks）均有 `last_sync_time`；列/索引/DDL/大小；`make evolve` 演示元数据历史与 diff |
| **S2** SQL 血缘 | `warehouse.dwd.v_order_line` → Lineage | 上游 `warehouse.ods.order_items`，下游 `mv_daily_sales` → `ads.ads_daily_sales`；出现 `AGGREGATE` 与 `DIRECT` 两种关系；可展开列级 |
| **S3** 端到端五跳 | `shop.orders` → Lineage 逐层展开 | `shop.orders → ods.orders → v_order_base/v_order_line → mv_daily_sales/dwd_* → ods.dwd_order_fact → ads.v_sr_* / mv_sr_region_daily`，无断点 |
| **S4** OpenLineage | OpenLineage → Jobs → `build_warehouse_layers` 的 run | ≥30 个 run；SQL facet；Airflow 深链回 8080；`REFRESH MATERIALIZED VIEW` 的提取告警（C4，属预期） |
| **S5** 命名空间映射 | `ol` profile 发一个命名空间不匹配的 COMPLETE 事件，再建映射重发 | 先出现 `external:…` 数据集 + 管理员通知；建映射后归入真实表 |
| **S6** 应用 SQL | `bootstrap` 注册应用并上报参数化语句 | Application SQL 页出现应用、语句、次数；表侧可见「哪些应用在读我」 |
| **S7** 治理 | Settings → Users/Groups/Roles/IAM/Audit Log/Notifications | 预置的第二个用户、组、自定义角色；引导过程的完整审计；至少一条通知 |
| **S8** 可选：Explain SQL / MCP / CLI | `--profile llm` / `--profile mcp` | Explain SQL 离线可用；`/mcp`、`/.well-known/*`、`/oauth/*` 生效；`mxd auth login` 走 `/device` 设备登录 |

### 5.1 功能覆盖矩阵

| 功能区 | 覆盖 | 方式 |
| --- | --- | --- |
| Home 仪表盘 | ✅ 默认 | 引导 + 管道运行后自然非空 |
| Instances / 数据源 / 同步 | ✅ 默认 | 三个实例注册并同步 |
| Databases / schemas | ✅ 默认 | 同上 |
| 元数据浏览（表/列/索引/DDL/历史/diff） | ✅ 默认 | 采集；`make evolve` 演示历史 |
| 表级 + 列级血缘 | ✅ 默认 | 视图、物化视图、OpenLineage、手工 SQL |
| 手工 SQL | ✅ 默认 | `bootstrap` 灌一条 |
| 应用 SQL | ✅ 默认 | 应用 + key + 报表由 `bootstrap` 灌入 |
| Explain SQL | ⚙️ `llm` profile | 本地 OpenAI 兼容端点或云端 key |
| OpenLineage 浏览（overview/jobs/datasets/events/run 详情） | ✅ 默认 | 四个 DAG 的事件 |
| 命名空间映射 | ⚙️ `ol` profile | 未映射事件 → external dataset → 建映射修复 |
| OpenLineage API Keys | ✅ 默认 | `bootstrap` 签发 1 个（界面显示 masked） |
| Airflow 深链 | ✅ 默认 | 事件自带 airflow facet |
| 通知（SSE 收件箱） | ✅ 默认 | 未映射命名空间通知 + 一条摄入失败样例 |
| MCP + OAuth 同意页 | ⚙️ `mcp` profile | loopback 上的 `external_url` |
| 设备登录 + `mxd` CLI | ⚙️ `mcp` profile | `mxd auth login` |
| Settings：general/environments/users/roles/groups/IAM | ✅ 默认 | 已预置/可渲染 |
| Settings：LLM providers | ⚙️ `llm` profile | 同上 |
| Settings：applications | ✅ 默认 | 已预置 |
| Settings：OpenLineage（映射/keys/保留期） | ✅ 默认 | 已预置 |
| Settings：identity providers | ⚠️ 部分 | 页面可渲染；完整 SSO 登录需要外部 IdP |
| SQL Server / MariaDB 采集 | ⚙️ `extra` profile | SQL Server 仅元数据 |

---

## 6. 目录结构

```
metaxisdata-demo/
├── docker-compose.yml               # 默认栈
├── docker-compose.profiles.yml      # extra / llm / mcp / datax / ports
├── .env.example                     # 非密钥的 demo 配置
├── Makefile                         # up / down / reset / logs / smoke / evolve / check-secrets
├── README.md / README.zh.md         # 快速开始 + S1–S8 走查
├── docs/
│   ├── design.md / design.zh.md     # 本方案（英文为主 + 中文镜像）
│   └── images/                      # README 引用的截图
├── docker/
│   ├── Dockerfile.airflow           # Airflow 3.1.8 + providers + openlineage-python
│   ├── Dockerfile.tools             # psql/mysql 客户端 + python，供一次性容器使用
│   ├── Dockerfile.datax             # 可选 profile
│   └── entrypoint-airflow.sh        # 等 state/ol.env → source → airflow standalone
├── airflow/
│   ├── dags/{ingest_shop_to_warehouse,build_warehouse_layers,publish_to_serving,build_serving_aggregates}.py
│   └── plugins/sql_copy_operator.py # 跨库搬运并上报 OpenLineage facet
├── fixture/                         # 从 test/e2e_etl vendoring，仅重命名库/schema
│   ├── UPSTREAM                     # 来源仓库与 commit
│   ├── mysql/{10_schema,20_seed,90_views,99_grants}.sql
│   ├── pg/{05_ods_ddl,10_dwd_ddl,20_dwd_load,30_ads_ddl,40_ads_load,50_experimental_pg_features}.sql
│   └── starrocks/{10_ods_ddl,20_ads_ddl}.sql
├── scripts/
│   ├── warehouse_init.sh            # schema + StarRocks DDL
│   ├── bootstrap.py                 # 账号/实例/同步/摄入 key/治理/样例
│   ├── etl_runner.sh                # 按序触发 DAG
│   ├── smoke.py                     # 轻量「是否已填充」检查
│   ├── evolve.sh                    # 一次 schema 变更 + 重新同步
│   ├── emit_openlineage.py          # 可选合成生产者
│   └── check_secrets.sh             # 公开仓库守卫
├── vendor/                          # 仅 datax profile 使用
└── state/                           # 运行时 key/token，gitignore
```

---

## 7. 启动时序、命名与凭据

### 7.1 时序

```
metadata-db(healthy)
  └─▶ metaxisdata(/healthz)
        └─▶ mysql-shop · postgres-warehouse · starrocks-serving (healthy)
              └─▶ warehouse-init   建 PG schema、下 StarRocks DDL
                    └─▶ bootstrap  账号 → 实例 → 同步 → 摄入 key → 映射
                                   → 应用 SQL + 手工 SQL → 治理样例 → 写 state/ol.env
                          └─▶ airflow  等 state/ol.env 出现后启动
                                └─▶ etl-runner  按序触发 4 个 DAG 并等待
                                      └─▶ smoke  断言演示环境已填充
```

`make up` 结尾自动执行 `make smoke`，因此「半填充」的环境不会被误报为就绪。

### 7.2 命名（模仿正常项目）

| 概念 | 取值 |
| --- | --- |
| compose 项目名 | `metaxisdata-demo` |
| 实例 | `mysql-shop`、`postgres-warehouse`、`starrocks-serving`（可选 `mssql-erp`、`mariadb-crm`） |
| 库 / schema | MySQL `shop`；PostgreSQL `warehouse` 下 `ods`/`dwd`/`ads`；StarRocks `ods`/`ads` |
| Airflow 连接 | `shop_mysql`、`warehouse_postgres`、`serving_starrocks` |
| DAG | `ingest_shop_to_warehouse`、`build_warehouse_layers`、`publish_to_serving`、`build_serving_aggregates` |
| 表/视图名 | 沿用 fixture（`orders`、`v_order_line`、`dwd_order_fact`、`mv_daily_sales`、`ads_daily_sales` 等），只改限定名里的库/schema 部分 |

### 7.3 凭据与公开仓库策略

仓库里只有**一次性本地凭据**：

| 位置 | 值 |
| --- | --- |
| MySQL | `demo` / `demo`，root `demo-root` |
| PostgreSQL（数仓与平台库） | `demo` / `demo` |
| StarRocks | `root`，空密码 |
| 平台管理员 | `admin@demo.local` / `Demo-admin-1234!` |
| Airflow | `admin` / `admin` |

保持可公开的四条规则：

1. **不提交任何生成的 key**：`ol_…`、`asql_…`、`sa_…` 都在运行时创建并写入 `state/`（已 gitignore）。
2. `.env` gitignore；`.env.example` 只放有文档说明的 demo 值。
3. `make check-secrets` 扫描工作区里的 key 前缀与私钥，命中即失败。
4. README 明确写出：这些凭据仅用于本地容器。

### 7.4 幂等与重置

- `bootstrap`：先查后建；已存在视为成功；同名 description 的 key 已存在则跳过。
- `warehouse-init`：`CREATE … IF NOT EXISTS`，并按 fixture 原有方式先 drop 再 create。
- DAG：装载前 truncate/delete；fixture SQL 本身幂等。
- `make reset`：`docker compose --profile "*" down -v` 后重新 `make up`。
- `make up`：只重跑一次性容器。

---

## 8. ETL 设计

| DAG | 步骤 | 血缘来源 |
| --- | --- | --- |
| `ingest_shop_to_warehouse` | 建 `ods` 表 → 8 个 `SqlCopyOperator` 并行搬运 → 行数对账 | copy operator 的 OpenLineage 列级 facet（服务端按注册表校验） |
| `build_warehouse_layers` | `10_dwd_ddl.sql`（6 视图 + 1 物化视图 + 2 表）→ `20_dwd_load.sql`（多 CTE `INSERT…SELECT`）→ `30_ads_ddl.sql` / `40_ads_load.sql` → `50_experimental_pg_features.sql` | SQL facet → 服务端**自己的**分析器 |
| `publish_to_serving` | 建 StarRocks `ods` 表 → 5 个搬运（其中一个读物化视图） | 列级 facet |
| `build_serving_aggregates` | StarRocks 视图 + 异步物化视图 | SQL facet（`REFRESH … WITH DATA` 会产生预期内的提取告警，见 C4） |

`SqlCopyOperator` 要点：

- 入参：`src_conn_id` / `dst_conn_id` / `select_sql` / `dst_table` / `column_map` / `batch_size`。
- 流式读 + 批量 `executemany` 写，先 truncate；StarRocks 走 **9030 MySQL 协议 + `INSERT`**，不需要 Stream Load 的 8030。
- 通过 Airflow **原生 facet 钩子** `get_openlineage_facets_on_start()` 上报 `OperatorLineage`，传输与鉴权交给官方 provider；自建 transport 的兜底分支必须自己带 Bearer，否则必然 401（DataX provider 的兜底分支就是这样）。
- 数据集命名符合解析器预期：`mysql://mysql-shop:3306` / `shop.orders`、`postgres://postgres-warehouse:5432` / `warehouse.ods.orders`。

---

## 9. 验证（轻量）

`make smoke`（`scripts/smoke.py`）只回答一个问题——**演示环境是否已填充**：

1. 常驻服务 healthy、一次性服务退出码 0；
2. `GET /healthz` 200，管理员能登录；
3. 三个实例存在且 `last_sync_time` 非空；
4. 每个实例都有表，且至少一个视图或物化视图；
5. `shop.orders` 既有上游也有下游，且图能走到 StarRocks；
6. OpenLineage 里每个 DAG 至少 1 个 run。

不做数字基线，不移植 `verify_chain.sql`。更深的校验属于产品自身的测试套件，不属于本仓库。

---

## 10. 风险

| # | 风险 | 对策 |
| --- | --- | --- |
| R1 | StarRocks 重（3.15GB / ~4GB 内存 / 启动慢） | `mem_limit: 4g` + 自带 healthcheck + `lite` profile |
| R2 | 本机端口冲突 | 默认不发布数据库端口；`ports` profile 映射 1xxxx |
| R3 | Airflow 镜像构建需要 PyPI | 文档给出 `PIP_INDEX_URL` / `PIP_TRUSTED_HOST` 覆盖方式 |
| R4 | Airflow 必须在摄入 key 之后启动 | `bootstrap` 写 `state/ol.env`，entrypoint 等待；`smoke` 在无 run 时报错 |
| R5 | 主机名不一致会静默产生 external dataset（C2） | 命名单一来源（D6）；`smoke` 在血缘走不到 StarRocks 时失败 |
| R6 | 未限定 schema 的 SQL 静默丢血缘（C3） | DAG 内所有语句全限定 |
| R7 | 物化视图提取告警看着像报错（C4） | 文档写明属预期，并作为产品特性展示 |
| R8 | DataX 无可靠二进制、provider 不在 PyPI（C7） | DataX 仅可选 profile，provider 随 profile vendoring |
| R9 | StarRocks 仅 amd64 | README 标注；Apple Silicon 用 `lite` |
| R10 | 一次性容器失败不易察觉 | `make up` 结尾跑 `make smoke`，并打印失败容器日志尾部 |
| R11 | 密钥误入仓库 | `make check-secrets` + 代码评审 |

---

## 11. 实施计划（每个 step 一次提交）

| Step | 范围 | 提交信息 |
| --- | --- | --- |
| 0 | 本方案、双语 README、`.gitignore`、密钥守卫 | `docs: add the demo design and repository scaffolding` |
| 1 | `docker-compose.yml`（`metadata-db` + `metaxisdata`）、`.env.example`、Makefile | `feat(compose): run the platform with its metadata store` |
| 2 | `mysql-shop`、`postgres-warehouse`、`starrocks-serving`、vendoring 的 fixture、`warehouse-init` | `feat(data): add the shop, warehouse and serving databases` |
| 3 | `bootstrap.py`：账号、实例、同步、摄入 key、手工/应用 SQL、治理样例 | `feat(bootstrap): register instances, sync metadata and seed samples` |
| 4 | Airflow 镜像、`SqlCopyOperator`、4 个 DAG、`etl-runner` | `feat(etl): add the Airflow pipeline and OpenLineage lineage` |
| 5 | `extra` / `llm` / `mcp` / `datax` / `ol` profile、`evolve.sh`、合成生产者 | `feat(profiles): add the optional engines, LLM, MCP and DataX profiles` |
| 6 | 双语走查文档、截图、`smoke` 收尾 | `docs: add the walkthrough and screenshots` |

每个 step 结束时栈都要仍可运行（`make up && make smoke`），提交信息遵循源码仓库使用的 conventional commits 风格。

---

## 12. 决策记录与待确认项

### 12.1 已确认

| 项 | 结论 |
| --- | --- |
| ETL 组件 | Airflow + 自带 `SqlCopyOperator`；DataX 为可选 profile |
| 引擎范围 | 默认 MySQL + PostgreSQL + StarRocks；SQL Server / MariaDB 走 `extra` |
| 平台镜像 | 只用官方镜像、不从源码构建；由 `METAXISDATA_VERSION` 固定（v0.1.1） |
| 仓库 | 公开：零密钥、零 token，只有一次性 demo 凭据 |
| 文档 | 英文为主 + 中文镜像；代码注释一律英文 |
| 验证 | 轻量 `make smoke`，不做测试套件 |
| 命名 | 贴近正常项目（§7.2） |
| 过程 | step by step，每个 step 一次提交（§11） |

### 12.2 待确认

1. 可选 profile 是否保持在默认快速开始之外，以保证首次启动足够快？（暂定：是）
2. 三引擎是否足够支撑「不同数据库类型」的叙事，还是把 SQL Server 放进默认（代价 +1.5GB 镜像、+2GB 内存）？（暂定：默认三引擎）
3. README 的截图：由本机采集，还是先留占位由维护者补？
