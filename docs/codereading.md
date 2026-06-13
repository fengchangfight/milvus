# Milvus 源码阅读指南

## 1. 项目总览

Milvus 是一个高性能向量数据库，为 AI 应用提供海量非结构化数据（文本、图像、多模态）的存储与检索。采用 Go + C++ 混合架构，核心向量检索引擎用 C++ 实现（`internal/core/`），分布式协调层用 Go 实现。

- **模块**: `github.com/milvus-io/milvus` (root) + `github.com/milvus-io/milvus/pkg/v3` (独立 go.mod)
- **语言**: Go (分布式/API 层) + C++ (搜索引擎/Knowhere) + Rust (Tantivy 全文检索)
- **协议**: gRPC + Protocol Buffers
- **注册中心**: etcd (服务发现 + 元数据存储)
- **消息队列**: Kafka / Pulsar / RocksMQ(Standalone)

### 1.1 Milvus 的输入是什么?

**向量 (Vector) + 标量字段 (Scalar Fields)**。Milvus 不负责把原始数据转成向量 (Embedding)，只负责存储和检索。

```
原始数据 (文本/图片/音频)
    ↓ 你调用 embedding 模型 (OpenAI/BGE/ColBERT...)
向量 [0.12, -0.34, ...]   ← 这才是 Milvus 的输入
    +
标量字段 {id, title, price, ...}
    ↓
一起 insert 进 Milvus
```

- Milvus 是 **向量数据库**，不是文档数据库。它不知道你的原始文本/图片长什么样
- 新版支持 **Function Field**: insert 时自动调用外部 embedding 服务，但本质上还是"先转向量再存"
- 搜索时同样: 你先把 query 用同一模型转成向量，再发给 Milvus 做相似度检索
- 返回的是最相似的向量所对应的标量字段 (如 text, id)，而不是原始 chunk

---

## 2. 物理架构: 谁运行在哪里?

### 2.1 两种部署模式

**Standalone (单机模式)**: 所有组件跑在 **1 个进程** 里。
**Cluster (分布式模式)**: 每个组件是 **独立进程 (K8s Pod)**,通过 gRPC 通信。

### 2.2 进程/节点一览

| 节点 | 进程数 | 类型 | 一句话职责 |
|------|--------|------|-----------|
| **Proxy** | 多实例 | 无状态 | gRPC 网关,接收用户所有请求,转发给后端 |
| **RootCoord** | 1 个(单例) | 有状态 | DDL 老大: 建表/删表/改 Schema/RBAC |
| **DataCoord** | 1 个(单例) | 有状态 | 数据管家: 管 Segment 生命周期,触发 Flush/Compaction/建索引 |
| **QueryCoord** | 1 个(单例) | 有状态 | 查询调度: 决定哪些 Segment 加载到哪些 QueryNode |
| **QueryNode** | 多实例 | 有状态(内存) | 查询工人: 把数据加载到内存,执行向量检索 |
| **DataNode** | 多实例 | 无状态 | 写入工人: 消费 WAL 消息,把数据持久化到 S3 |
| **StreamingCoord** | 0(嵌入) | 嵌入 RootCoord | WAL 协调: 管理消息通道分配 |
| **StreamingNode** | 多实例 | 有状态 | WAL 写入: 接收写入请求,append 到消息队列 |
| **MixCoord** | 1 个(单例) | 有状态 | Standalone 模式下 RootCoord+DataCoord+QueryCoord 三合一 |

> **关键理解**: Coordinators(协调器) 是大脑,负责决策; Nodes(节点) 是手脚,负责干活。大脑用 etcd 做持久化记忆,手脚挂了可以换新的。

### 2.3 组件通信方式

```
外部依赖 (独立进程):
  etcd (服务发现+元数据) ←→ 所有组件
  MinIO/S3 (对象存储)    ←→ DataNode(写) + QueryNode(读)
  Kafka/Pulsar (消息队列) ←→ StreamingNode(写) + DataNode(读)

内部 RPC (gRPC):
  Client → Proxy ................... 用户请求入口
  Proxy → RootCoord ............... DDL 操作 (建表/删表)
  Proxy → QueryCoord .............. 查询路由
  Proxy → StreamingNode ........... 数据写入 (WAL)
  RootCoord → DataCoord ........... 下发 DDL 给数据层
  DataCoord → DataNode ............ 下发 Flush/Compaction 任务
  QueryCoord → QueryNode .......... 下发 Load/Release 任务
  Proxy → QueryNode ............... 直接查询 (搜索/检索)
```

---

## 3. 逻辑架构: 数据如何流转?

### 3.1 分层抽象 (从上到下是请求流,从下到上是依赖流)

```
第 1 层  接入层 (Access)
         Proxy: 你只跟这一层打交道,它替你找后端

第 2 层  协调层 (Coordination)
         RootCoord(元数据) + DataCoord(数据生命周期) + QueryCoord(查询调度)
         它们不存数据,只做决策和记账

第 3 层  流系统 (Streaming / WAL)
         StreamingCoord + StreamingNode + Kafka/Pulsar
         写入的数据先到这里,像快递中转站,保证不丢

第 4 层  执行层 (Execution)
         DataNode(负责存) + QueryNode(负责查)
         真正的体力劳动者

第 5 层  存储层 (Storage)
         MinIO/S3: 持久化文件 (Binlog)

第 6 层  引擎层 (Engine)
         C++ segcore: 向量索引 + 相似度计算,被 QueryNode 用 CGo 调用
```

### 3.2 写路径 — 数据怎么存进去?

```
你 insert 100 条向量
  → Proxy 收到,分配时间戳,发到 WAL (Kafka)
  → StreamingNode 把数据 append 到消息队列 (每 1 个 VChannel 对应 1 个队列分区)
  → DataNode 订阅队列,收到数据,攒够一批或到时间就 flush
  → DataNode 把数据转成 Binlog 文件,上传到 MinIO/S3
  → DataNode 通知 DataCoord: "Segment 写完了!"
  → DataCoord 通知 QueryCoord: "有新的 Sealed Segment 可用"
  → QueryCoord 让某个 QueryNode 把 Segment 从 S3 加载到内存
  → 现在数据可以被查询了
```

**为什么这么设计?** 写和查完全解耦。写入只走 WAL,不阻塞查询; 查询只读内存和 S3,不受写入影响。

### 3.3 读路径 — 数据怎么查出来?

```
你搜索 "找最相似的 10 个向量"
  → Proxy 收到,查 MetaCache 知道这个 Collection 有哪些 VChannel
  → Proxy 问 QueryCoord: "每个 VChannel 的数据在哪些 QueryNode 上?"
  → Proxy 向对应的 QueryNode 并发发送 Search 请求
  → QueryNode 在自己的 Growing Segment(内存) + Sealed Segment(S3加载的) 中搜索
  → 搜索时调用 C++ segcore 做向量相似度计算 (最快)
  → 每个 QueryNode 返回 topK 结果
  → Proxy 汇总所有结果,全局排序,返回 topK 给客户端
```

### 3.4 一张总图: 物理部署 vs 逻辑数据流

```
┌─────────┐     ┌──────────┐     ┌──────────┐
│  你写的  │────→│  Proxy   │────→│Streaming │────→ Kafka
│ Python  │     │ (gRPC)   │     │  Node    │    (WAL)
│  代码   │     └──────────┘     └──────────┘      │
└─────────┘          │               ↑             │
                     │               │             ↓
                     ↓               │        ┌──────────┐
                ┌──────────┐    ┌────────┐   │ DataNode │
                │RootCoord │    │Stream  │←──│ (消费WAL) │
                │(etcd元数)│    │Coord   │   └──────────┘
                └──────────┘    └────────┘        │
                     │               ↑            ↓
                     ↓               │        MinIO/S3
                ┌──────────┐         │       (Binlog)
                │DataCoord │         │           │
                │(Segment) │         │           ↓
                └──────────┘    ┌──────────┐
                     │          │QueryCoord│   ┌──────────┐
                     ↓          │(Load决策) │──→│QueryNode │
                ┌──────────┐    └──────────┘   │(C++搜)   │
                │QueryCoord│                   └──────────┘
                └──────────┘                        ↑
                     │                              │
                     ↓                              │
                QueryNode ────── 读取 S3 Binlog ────┘
                (内存中检索)
```

---

## 4. 设计这个系统要解决哪些问题?

### 问题 1: 向量搜索不是传统数据库能干的
传统数据库 (MySQL/PostgreSQL) 按精确值查找。向量搜索是"找最相似的 100 个",需要算余弦距离/欧氏距离,这是计算密集型任务。**解决**: 用 C++ 写专用搜索引擎 (Faiss/HNSW/DiskANN),CPU/GPU 加速。

### 问题 2: 数据量太大,一台机器放不下
AI 应用动辄几十亿向量,单机内存/磁盘都不够。**解决**: 按 VChannel (逻辑分片) 把数据打散到多个 QueryNode,每个只负责一部分,查询时并发搜索再汇总。

### 问题 3: 实时写入 + 高性能查询的矛盾
如果边写边建索引,写入会很慢。如果只建好索引再查,实时性差。**解决**: LSM-tree 思想 — Growing Segment (内存中,不建索引,暴力搜索) + Sealed Segment (持久化后建索引,快速搜索),后台自动 Compaction。

### 问题 3b: 为什么有 VChannel 和 PChannel 两层通道？
直接让每个 Shard 对应一个 Kafka Partition 不行吗？
- **物理资源的限制**: Kafka 的 Partition 数量有限且固定(默认 16 个)。如果 100 个 Collection 各 8 个 Shard,就需要 800 个 Partition — 太多,Kafka 撑不住
- **多租户共享**: 用固定 PChannel 池(16个),VChannel 在其上多路复用。不同 Collection 的 VChannel 共享同一个 PChannel
- **解耦逻辑与物理**: 建多少 Shard 是 Collection 层面的逻辑决策,有多少 Kafka Partition 是集群层面的物理决策,两者不应耦合

详见 `docs/questions.md` §Q1。

### 问题 4: 写入不能丢
用户 insert 的数据必须持久化,节点挂了不能丢。**解决**: WAL (Write-Ahead Log) 机制。数据先写 Kafka/Pulsar (高可靠),再异步刷到 S3。如果 DataNode 挂了,换个新的从 WAL 重新消费就行。

### 问题 5: Coordinator 挂了怎么办?
RootCoord/DataCoord/QueryCoord 都是单例,挂了系统就不可用。**解决**: 状态存在 etcd 里 (高可用),Coordinator 挂了重新选举一个,从 etcd 恢复状态继续工作。

### 问题 6: 查询延迟要低
向量搜索是 CPU 密集型,每个查询可能扫几百万向量。**解决**: 
- 索引 (IVF: 聚类缩小搜索范围 / HNSW: 图索引快如闪电)
- 多段并发 (一个 Collection 的多个 Segment 分布在多个 QueryNode 上并行搜)
- 内存优先 (Sealed Segment 加载到内存再搜,避免磁盘IO)

### 问题 7: 多种查询类型要共存
有时用户想"按向量相似度搜 + 按价格>100 过滤 + 按品类分组"。**解决**: Hybrid Search — C++ segcore 支持向量搜索 + 标量表达式过滤同时执行,Proxy 层做 GROUP BY 聚合。

### 问题 8: 向量索引怎么这么快？
向量搜索是 O(N*dim) 的暴力计算,十亿量级不可行。**解决**:
- **IVF (聚类)**: 先 K-Means 聚成 N 个簇,搜索时只搜最近 nprobe 个簇,精度/速度可调
- **HNSW (图)**: 构建多层近邻图,搜索时贪心遍历,理论复杂度 O(log N)
- **DiskANN**: 索引存磁盘按需加载,内存不够也能搜海量数据
- **PQ/SCANN (量化)**: 将高维向量压缩编码,在压缩空间里快速计算近似距离
- 所有方法通过 **Knowhere** 统一封装,建表时通过 `index_type` 参数选择

---

## 5. 核心设计框架 (详细)

### 5.1 分层架构

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

### 5.2 核心数据流 (速查)

| 操作 | 数据流 |
|------|--------|
| **写** | Client → Proxy → StreamingNode → WAL → DataNode → MinIO → QueryNode |
| **读** | Client → Proxy → QueryCoord → QueryNode (segcore) → Proxy → Client |
| **DDL** | Client → Proxy → RootCoord → etcd + WAL Broadcast → StreamingNodes |

### 5.3 两种部署模式

- **Standalone**: 所有组件 1 个进程 (见上文 §2)
- **Cluster**: 每个组件独立 K8s Pod, gRPC 通信 (见上文 §2)

### 5.4 核心数据类型

- **Collection**: 类似数据库 Table，包含 Schema（字段定义）
- **Shard**: Collection 的逻辑分片,建表时指定 `shards_num`。决定写入和查询的并行度,每个 Shard 对应 1 个 VChannel
- **Segment**: 数据存储的基本单元，分为 Growing（可写）和 Sealed（只读）
- **VChannel (Virtual Channel)**: 逻辑通道,1 Shard = 1 VChannel。命名格式 `<pchannel>_<collectionID>v<shardIndex>`。同一 VChannel 内消息保序
- **PChannel (Physical Channel)**: 物理通道,固定数量 (默认 16),每 1 个 PChannel 映射到 1 个 Kafka Topic/Partition。N 个 VChannel 复用 1 个 PChannel
- **TimeTick**: 全局单调递增逻辑时钟，保证写入有序
- **Hybrid Timestamp (HybridTs)**: TSO(全局排序) + LocalTs(本地排序)

**Shard → VChannel → PChannel 关系图**:
```
Collection (shards_num=2)
  ├── Shard 0  ←→  VChannel "dml_0_12345v0"  ─┐
  └── Shard 1  ←→  VChannel "dml_3_12345v1"  ─┤
                                                ├─ 复用 PChannel 池 (固定16个)
  其他 Collection 的 VChannels ─────────────────┘
```
`ToPhysicalChannel("dml_0_12345v0")` → `"dml_0"` （提取所属 PChannel,见 `pkg/util/funcutil/func.go:393`）

---

## 6. Key Features

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

## 7. Core Components 详解

### 7.1 Proxy (接入层) — `internal/proxy/`

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

### 7.2 RootCoord (根协调器) — `internal/rootcoord/`

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

### 7.3 DataCoord (数据协调器) — `internal/datacoord/`

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

### 7.4 QueryCoord(v2) — `internal/querycoordv2/`

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

### 7.5 QueryNode(v2) — `internal/querynodev2/`

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

### 7.6 DataNode — `internal/datanode/`

数据持久化节点，消费 WAL 消息，写入对象存储。

**核心文件**:
- `data_node.go` — 核心结构体
- `services.go` — gRPC 服务接口
- `compactor/` — Compaction 执行器
- `index/` — 索引构建服务
- `importv2/` — Bulk Import v2
- `external/` — 外部数据源处理

### 7.7 Streaming System (流系统) — `internal/streamingcoord/` + `internal/streamingnode/`

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

### 7.8 C++ Segcore — `internal/core/src/`

高性能向量检索引擎的核心 C++ 代码。**底层通过 Knowhere 统一封装多种索引库** (Faiss/HNSW/DiskANN/SCANN/GPU-CAGRA)，Go 端通过 CGo 调用。具体支持哪些索引类型由编译进 C++ 的 segcore 在运行时暴露 — 见 `internal/util/vecindexmgr/vector_index_mgr.go:117-134`。

**目录结构**:
- `segcore/` — 核心搜索执行引擎
- `index/` — 向量索引 (IVF/HNSW/DiskANN/SCANN)，通过 Knowhere 适配
- `expr/` — 表达式求值器 (标量过滤)
- `query/` — 查询处理
- `storage/` — C++ 端存储接口
- `common/` — 公共类型 (Schema, Types)
- `exec/` — 执行器框架
- `clustering/` — 聚类 Compaction
- `plan/` — 查询计划
- `monitor/` — 内存/性能监控
- `mmap/` — 内存映射文件

### 7.9 存储层 — `internal/storage/` + `internal/storagev2/`

- `internal/storage/` — Binlog 格式 (读写, 编解码)
  - `binlog_writer.go` / `binlog_reader.go` — Binlog 文件读写
  - `insert_data.go` / `delta_data.go` — 插入/删除数据编解码
  - `data_codec.go` — 数据序列化
  - `factory.go` — 存储工厂: 根据 `common.storageType` 选择 `local` / `remote`(MinIO) / `opendal`
  - `local_chunk_manager.go` — 本地磁盘实现 (开发/测试用)
  - `remote_chunk_manager.go` — S3/MinIO/Azure 云存储
  - `minio_object_storage.go` / `azure_object_storage.go` — 云存储适配
  - `pk_statistics.go` — PK 统计信息 (Bloom Filter)
- `internal/storagev2/packed/` — v2 打包格式 (更高效)

### 7.10 公共包 — `pkg/`

- `pkg/proto/` — Protobuf 定义
- `pkg/mq/` — 消息队列抽象 (Kafka/Pulsar/RocksMQ)
- `pkg/streaming/` — 流系统客户端库
- `pkg/util/` — 工具函数 (paramtable, typeutil, merr, etc.)
- `pkg/metrics/` — Prometheus 指标定义
- `pkg/kv/` — KV 存储抽象 (etcd/TiKV)
- `pkg/log/` — 日志库

---

## 8. 最值得读的 Top 10 代码

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

## 9. 推荐阅读顺序

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
