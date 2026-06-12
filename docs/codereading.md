# Milvus 源码阅读指南

## 1. 项目总览

Milvus 是一个高性能向量数据库，为 AI 应用提供海量非结构化数据（文本、图像、多模态）的存储与检索。采用 Go + C++ 混合架构，核心向量检索引擎用 C++ 实现（`internal/core/`），分布式协调层用 Go 实现。

- **模块**: `github.com/milvus-io/milvus` (root) + `github.com/milvus-io/milvus/pkg/v3` (独立 go.mod)
- **语言**: Go (分布式/API 层) + C++ (搜索引擎/Knowhere) + Rust (Tantivy 全文检索)
- **协议**: gRPC + Protocol Buffers
- **注册中心**: etcd (服务发现 + 元数据存储)
- **消息队列**: Kafka / Pulsar / RocksMQ(Standalone)

---

## 2. 核心设计框架

### 2.1 分层架构

```
┌─────────────────────────────────────────────────────────┐
│  SDK 层: pymilvus, Java SDK, Go SDK, Node.js SDK        │
├─────────────────────────────────────────────────────────┤
│  Proxy (接入层): gRPC 网关, 鉴权, 限流, 路由             │
├────────────────────┬────────────────────────────────────┤
│  Coordinators      │  Nodes (执行层)                     │
│  · RootCoord       │  · QueryNode (查询 + 向量检索)      │
│  · DataCoord       │  · DataNode  (数据持久化)           │
│  · QueryCoord(v2)  │  · StreamingNode (WAL 写入)         │
├────────────────────┴────────────────────────────────────┤
│  Streaming System (流系统 / WAL)                         │
│  · StreamingCoord (协调) · StreamingNode (执行)          │
│  · WAL Backend: Kafka/Pulsar/Woodpecker/RocksMQ          │
├─────────────────────────────────────────────────────────┤
│  存储层: MinIO/S3/Azure/GCS (对象存储)                    │
│  · Binlog 格式 (插入/删除/索引数据)                       │
│  · Packed 格式 (v2 存储优化)                              │
├─────────────────────────────────────────────────────────┤
│  C++ 搜索引擎 (segcore): Knowhere, Faiss, DiskANN         │
│  · 向量索引: IVF_FLAT, HNSW, SCANN, DiskANN ...          │
│  · 标量索引: Tantivy (Rust, 全文检索)                     │
│  · 查询执行: 向量搜索 + 标量过滤 + 混合搜索               │
└─────────────────────────────────────────────────────────┘
```

### 2.2 核心数据流

**写路径 (Write Path)**:
```
Client → Proxy → StreamingClient.Append → StreamingNode → WAL Backend
                                                     ↓
                                              DataNode (消费)
                                                     ↓
                                              MinIO/S3 (Binlog 持久化)
                                                     ↓
                                              QueryNode (加载到 segcore)
```

**读路径 (Read Path)**:
```
Client → Proxy → QueryCoord (路由) → QueryNode (检索 + 计算) → Proxy → Client
```

**DDL 路径 (CreateCollection等)**:
```
Client → Proxy → RootCoord → StreamingClient.Broadcast → StreamingNodes → WAL
         ↓                ↓
    MetaCache      etcd (元数据持久化)
```

### 2.3 两种部署模式

- **Standalone Mode**: 所有组件在一个进程中运行 (`cmd/roles/roles.go:MilvusRoles`)
- **Cluster Mode**: 每个组件独立部署，通过 etcd 服务发现，K8s 原生

### 2.4 核心数据类型

- **Collection**: 类似数据库 Table，包含 Schema（字段定义）
- **Segment**: 数据存储的基本单元，分为 Growing（可写）和 Sealed（只读）
- **VChannel**: 逻辑分片，一个 Collection 按 shard 分成多个 VChannel
- **PChannel**: 物理通道，映射到 WAL 的一个 Topic/Partition
- **TimeTick**: 全局单调递增逻辑时钟，保证写入有序
- **Hybrid Timestamp (HybridTs)**: TSO(全局排序) + LocalTs(本地排序)

---

## 3. Key Features

| 功能 | 说明 | 关键代码位置 |
|------|------|-------------|
| 向量相似性检索 | 支持 IVF/HNSW/DiskANN 等多种索引 | `internal/core/src/index/` |
| 混合搜索 (Hybrid Search) | 向量 + 标量过滤联合查询 | `internal/proxy/task_search.go` |
| 标量过滤 | 支持 =, >, <, IN, LIKE, REGEX 等 | `internal/core/src/expr/` |
| 全文检索 | Tantivy(Rust) 集成，支持 BM25 | `internal/core/thirdparty/` |
| 多向量字段 | 一个 Collection 可定义多个向量字段 | `internal/proxy/task_search.go` |
| 分组聚合 (GroupBy) | 搜索结果按字段分组 | `internal/proxy/task_search.go:GroupByFieldKey` |
| 流式写入 | 基于 WAL 的实时数据摄入 | `internal/streamingnode/` |
| Partition | 逻辑分区隔离数据 | `internal/rootcoord/` |
| 结构化数组 | Struct/Array 类型字段 | `internal/proxy/task_insert.go` |
| Upsert / Partial Update | 插入或更新 / 部分字段更新 | `internal/proxy/task_upsert.go` |
| RBAC | 基于角色的访问控制 | `internal/rootcoord/meta_rbac.go` |
| CDC / Replication | 跨集群数据复制 | `internal/cdc/`, `internal/proxy/replicate/` |
| External Table | 外部数据源映射 | `internal/datacoord/external_collection_refresh_manager.go` |
| Snapshot | 数据快照 | `internal/datacoord/snapshot.go` |
| Entity Level TTL | 按条目的自动过期 | `internal/compaction/` |

---

## 4. Core Components 详解

### 4.1 Proxy (接入层) — `internal/proxy/`

最重要的入口，所有客户端请求的第一站。

**核心文件**:
- `proxy.go` — Proxy 核心结构体，持有 MetaCache, Coordinator 客户端, 限流器等
- `impl.go` — **约8000行!** 实现 `milvuspb.MilvusServiceServer` 的全部 gRPC 接口
- `task.go` — Task 框架定义 (`task` interface: PreExecute/Execute/PostExecute)
- `task_scheduler.go` — DD 任务调度器（串行执行 DDL）
- `task_search.go` — 搜索任务实现，包括 Hybrid Search, GroupBy, Iterator
- `task_insert.go`, `task_delete.go`, `task_upsert.go` — 写操作任务
- `task_index.go`, `task_flush.go`, `task_alias.go` — DDL/DML 操作
- `meta_cache.go` — 本地元数据缓存 (Schema, Collection, Partition)
- `search_pipeline.go`, `query_pipeline.go` — 搜索/查询管道

**设计要点**:
- 使用 Task 模式 (`task` interface)，每种请求对应一个 Task
- DDL 任务串行执行 (TaskScheduler)，DML 任务可并发
- 通过 StreamingClient 写入 WAL (新架构) 或 MsgStream (旧架构)

### 4.2 RootCoord (根协调器) — `internal/rootcoord/`

DDL 入口，管理集群元数据 (Collection, Partition, Field, Alias, RBAC)。

**核心文件**:
- `root_coord.go` — 约3569行，核心协调逻辑
- `meta_table.go` — 元数据表 (Collection/Partition 管理)
- `scheduler.go` — 任务调度 (ID 分配 + 任务执行)
- `ddl_callbacks_*.go` — 各种 DDL 回调实现
- `dml_channels.go` — DML Channel 分配
- `timeticksync.go` — 时间同步

**设计要点**:
- 元数据持久化到 etcd (通过 kv/metastore 抽象)
- 通过 TSO 分配全局唯一 ID 和时间戳
- 三阶段执行: PrePare → Execute → Complete

### 4.3 DataCoord (数据协调器) — `internal/datacoord/`

管理 Segment 生命周期：分配、Flush、Compaction、索引构建。

**核心文件**:
- `server.go` — 核心服务器
- `meta.go` — Segment 元数据管理
- `segment_manager.go` — Segment 分配与管理
- `compaction_trigger.go`, `compaction_trigger_v2.go` — Compaction 触发
- `compaction_policy_*.go` — L0/Single/Clustering/ForceMerge 策略
- `compaction_task_*.go` — Compaction 任务执行
- `index_meta.go`, `index_service.go` — 索引元数据与服务
- `garbage_collector.go` — 垃圾回收 (清理无用的 binlog/index 文件)
- `handler.go` — Watch Channel 处理
- `import_*.go` — Bulk Insert 支持
- `snapshot.go`, `snapshot_manager.go` — Snapshot 管理

**设计要点**:
- Handshake Channel: DataCoord 通过 Channel 下发命令到 DataNode
- Compaction: L0 (小段合并) → Mix (混合) → Clustering (聚类) → ForceMerge
- 基于 etcd Watch 实现节点发现

### 4.4 QueryCoord(v2) — `internal/querycoordv2/`

管理 QueryNode 的负载均衡和 Segment 分配。

**核心文件**:
- `server.go` — 核心服务器
- `meta/` — 元数据管理层
- `dist/` — 分布层 (Segment → Node 映射)
- `assign/` — 分配算法
- `balance/` — 负载均衡算法
- `checkers/` — 健康检查与自动修复
- `job/` — 异步任务 (Load/Release/Handoff)
- `observers/` — 观察者 (Leader 选举, Collection 状态)
- `session/` — QueryNode 会话管理
- `task/` — 任务执行

### 4.5 QueryNode(v2) — `internal/querynodev2/`

**最值得深读的模块！** 向量搜索的执行引擎，Go 层通过 CGo 调用 C++ segcore。

```
QueryNode
├── server.go           入口，实现 types.QueryNode
├── delegator/          每个 VChannel 对应一个 Delegator
│   ├── delegator.go    核心：管理 Growing/Sealed Segments，执行 Search/Query
│   ├── delegator_data.go        数据加载逻辑
│   ├── distribution.go          Segment 分布管理
│   ├── segment_pruner.go        Segment 裁剪
│   ├── pk_filter.go             PK 过滤优化
│   ├── deletebuffer/            删除缓冲
│   └── snapshot.go              Snapshot 支持
├── segments/           与 C++ segcore 的桥接层
│   ├── segment.go      CGo 包装, CSearchResult, CRetrieveResult
│   ├── collection.go   CCollection 管理
│   ├── segcore.go      Segcore 初始化
│   ├── search.go       搜索入口 (调用 C++ Search)
│   ├── retrieve.go     原始数据检索
│   ├── segment_loader.go   从 S3 加载 Sealed Segment
│   ├── segment_l0.go       L0 Segment (删除记录)
│   ├── result.go       结果处理
│   ├── search_reduce.go    多 Segment 结果归并
│   └── manager.go      Segment Manager
├── pipeline/           流数据摄入管道
│   ├── pipeline.go     管道编排
│   ├── insert_node.go  插入节点
│   ├── delete_node.go  删除节点
│   ├── filter_node.go  过滤节点
│   └── manager.go      管道管理
├── pkoracle/           PK Oracle (主键去重/布隆过滤器)
├── cluster/            分布式查询 (多 QueryNode 协同)
└── tasks/              异步任务
```

**关键数据流**:
1. Growing Segment: StreamingNode → Pipeline (Insert/Delete Node) → Delegator → C++ segcore
2. Sealed Segment: DataCoord 通知 → Segment Loader → MinIO 下载 → C++ segcore 加载
3. Search: Client → Proxy → Delegator.Search() → C++ segcore → 多段结果归并 → Proxy → Client

### 4.6 DataNode — `internal/datanode/`

数据持久化节点，消费 WAL 消息，写入对象存储。

**核心文件**:
- `data_node.go` — 核心结构体
- `services.go` — gRPC 服务接口
- `compactor/` — Compaction 执行器
- `index/` — 索引构建服务
- `importv2/` — Bulk Import v2
- `external/` — 外部数据源处理

### 4.7 Streaming System (流系统) — `internal/streamingcoord/` + `internal/streamingnode/`

v2.3+ 引入的新 WAL 系统，替代旧的 msgstream。

**StreamingCoord** (运行在 RootCoord 进程内):
- 单例协调器，管理 PChannel 分配
- `server/broadcaster/` — 跨 PChannel 原子广播 (DDL/DCL)
- `server/balancer/` — 负载均衡

**StreamingNode**:
- `server/wal/` — WAL 核心逻辑 (TimeTick, Txn, Lock)
- `server/walmanager/` — WAL Manager
- `server/flusher/` — 数据刷盘

**数据流**:
```
Proxy → StreamingClient → StreamingNode (Append to PChannel) → WAL Backend
                                       ↓
                              DataNode (Consume & Persist to S3)
```

详细文档: `docs/agent_guides/streaming-system/streaming-system.md`

### 4.8 C++ Segcore — `internal/core/src/`

高性能向量检索引擎的核心 C++ 代码。

**目录结构**:
- `segcore/` — 核心搜索执行引擎
- `index/` — 向量索引 (IVF/HNSW/DiskANN/SCANN)
- `expr/` — 表达式求值器 (标量过滤)
- `query/` — 查询处理
- `storage/` — C++ 端存储接口
- `common/` — 公共类型 (Schema, Types)
- `exec/` — 执行器框架
- `clustering/` — 聚类 Compaction
- `plan/` — 查询计划
- `monitor/` — 内存/性能监控
- `mmap/` — 内存映射文件

### 4.9 存储层 — `internal/storage/` + `internal/storagev2/`

- `internal/storage/` — Binlog 格式 (读写, 编解码)
  - `binlog_writer.go` / `binlog_reader.go` — Binlog 文件读写
  - `insert_data.go` / `delta_data.go` — 插入/删除数据编解码
  - `data_codec.go` — 数据序列化
  - `minio_object_storage.go` / `azure_object_storage.go` — 云存储适配
  - `remote_chunk_manager.go` — 远程存储分块管理
  - `pk_statistics.go` — PK 统计信息 (Bloom Filter)
- `internal/storagev2/packed/` — v2 打包格式 (更高效)

### 4.10 公共包 — `pkg/`

- `pkg/proto/` — Protobuf 定义
- `pkg/mq/` — 消息队列抽象 (Kafka/Pulsar/RocksMQ)
- `pkg/streaming/` — 流系统客户端库
- `pkg/util/` — 工具函数 (paramtable, typeutil, merr, etc.)
- `pkg/metrics/` — Prometheus 指标定义
- `pkg/kv/` — KV 存储抽象 (etcd/TiKV)
- `pkg/log/` — 日志库

---

## 5. 最值得读的 Top 10 代码

| 排名 | 文件 | 行数 | 为什么重要 |
|------|------|------|-----------|
| 1 | `cmd/roles/roles.go` | 652 | **进程入口**。Standalone/Cluster 模式如何启动所有组件，理解部署模型的唯一入口 |
| 2 | `internal/proxy/impl.go` | 7965 | **API 全集**。所有 gRPC 接口实现在此，从 CreateCollection 到 Search/Query/Insert，是理解外部 API 如何映射到内部组件的最佳文件 |
| 3 | `internal/querynodev2/delegator/delegator.go` | 1619 | **查询核心**。ShardDelegator 是每个 VChannel 的查询代理，管理 Growing/Sealed Segment，处理 Search/Query/GetData，是查询路径的枢纽 |
| 4 | `internal/querynodev2/segments/segment.go` | 1785 | **CGo 桥接层**。Go 与 C++ segcore 的交互全部在此，理解向量检索如何调用 C++ 引擎的关键 |
| 5 | `internal/proxy/task.go` | 4306 | **Task 框架**。所有 Proxy 请求都以 Task 形式调度执行，理解 Milvus 任务调度模型的基础 |
| 6 | `internal/rootcoord/root_coord.go` | 3569 | **DDL 总控**。Collection/Partition/Field 的创建、修改、删除全在此协调，涉及 TSO 分配、etcd 写入、WAL 广播 |
| 7 | `internal/datacoord/server.go` | 1278 | **数据生命周期管理**。Segment 分配、Flush 触发、Compaction 调度、索引构建、GC 都在此协调 |
| 8 | `internal/querycoordv2/server.go` | 945 | **负载均衡**。Segment 如何在 QueryNode 之间分配、迁移、平衡，是分布式查询性能的关键 |
| 9 | `docs/agent_guides/streaming-system/streaming-system.md` | 41 | **WAL 架构**。理解 Milvus 流式写入系统的顶层架构文档，虽然短但包含所有子文档的索引链接 |
| 10 | `internal/proxy/task_search.go` | ~5000+ | **搜索实现**。Hybrid Search、GroupBy、Iterator、ANN Search、Range Search 等所有搜索变体 |

**补充推荐** (进阶必读):
- `internal/storage/binlog_writer.go` — 理解数据持久化格式 (Binlog)
- `internal/proxy/meta_cache.go` — 本地元数据缓存机制
- `internal/querynodev2/pipeline/pipeline.go` — 流式数据摄入管道
- `internal/querynodev2/segments/search.go` — 搜索到 C++ 的调用链
- `internal/rootcoord/meta_table.go` — Collection/Partition 元数据管理
- `internal/datanode/data_node.go` — 数据持久化节点

---

## 6. 推荐阅读顺序

```
第1天: cmd/roles/roles.go → cmd/components/*.go
      理解: 项目如何启动，有哪些组件，Standalone vs Cluster

第2天: internal/proxy/impl.go (重点看 CreateCollection, Insert, Search 三个方法)
      理解: 一个请求从客户端到内部的完整路径

第3天: docs/agent_guides/streaming-system/streaming-system.md
      理解: WAL 系统架构 + 数据写入路径

第4天: internal/rootcoord/root_coord.go + internal/datacoord/server.go
      理解: Coordinator 如何管理元数据和调度任务

第5天: internal/querynodev2/delegator/delegator.go + segments/segment.go
      理解: 查询执行引擎 + Go/C++ 交互

第6天: internal/proxy/task.go + task_search.go
      理解: Task 框架 + 搜索的完整实现

第7天: internal/core/src/segcore/ (C++ 部分)
      理解: 向量检索引擎的底层实现
```
