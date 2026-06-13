# Milvus 常见问题

## Q1: Shard / VChannel / PChannel 的关系

这是 Milvus 数据分片的**三层抽象**:

```
Collection (一张表)
  │  shards_num = 2 (建表时指定)
  │
  ├── Shard 0  ←→  VChannel "dml_0_12345v0"
  │                   │ 由 StreamingCoord 分配到某个 PChannel
  │                   ↓
  ├── Shard 1  ←→  VChannel "dml_3_12345v1"
  │                   │
  │    其他 Collection 的 VChannel 也混在一起   ←  多租户复用
  │
  ↓
PChannel 池 (固定 16 个,类似 Kafka Topic 的分区池)
  ├── PChannel dml_0  →  Kafka Topic partition 0
  ├── PChannel dml_1  →  Kafka Topic partition 1
  ├── ...
  └── PChannel dml_15 →  Kafka Topic partition 15
```

### 三层逐一定义

| 概念 | 是什么 | 数量 | 谁管理 |
|------|--------|------|--------|
| **Shard** | Collection 的逻辑分片,决定写入/查询的并行度 | 建表时指定,默认 2 | RootCoord |
| **VChannel** | 每个 Shard 对应 1 个 VChannel,是数据的**逻辑通道** | = Shard 数 | StreamingCoord 分配 |
| **PChannel** | 物理通道,映射到 Kafka Topic/Partition,**多个 VChannel 共享 1 个 PChannel** | 固定 16 个 | StreamingCoord 管理 |

### VChannel 命名规则

VChannel 的名字自描述了一切信息:

```
dml_0_12345v0
 │    │  │  │
 │    │  │  └── shard 序号 (第 0 个 shard)
 │    │  └──── 所属 Collection ID (12345)
 │    └─────── 所在的 PChannel (dml_0)
 └──────────── 通道前缀
```

通过 `funcutil.ToPhysicalChannel(vchannel)` 可以提取出 PChannel 名:
```go
func ToPhysicalChannel(vchannel string) string {
    index := strings.LastIndex(vchannel, "_")
    return vchannel[:index]  // "dml_0_12345v0" → "dml_0"
}
```

关键代码: `pkg/util/funcutil/func.go:393-402`、`internal/rootcoord/ddl_callbacks_create_collection.go:88-92`

### 为什么这样设计?

1. **Shard 让查询并行化** — 一个 Collection 拆成 N 个 Shard,搜索时每个 Shard 独立搜索,结果汇总排序。Shard 太多了浪费,太少了无法并行,默认 2 是经验值
2. **VChannel 是消息的路由单位** — 同一 Shard 的写入必须有序(同一 VChannel 内消息保序),不同 Shard 之间无需保证顺序
3. **PChannel 是物理资源池** — Kafka 的 Topic/Partition 数是有限且固定的(默认 16)。N 个 Collection × M 个 Shard = N×M 个 VChannel,全部复用这 16 个 PChannel。**PChannel 数量决定了集群的总写入吞吐上限**

### 对查询的影响

```
Proxy 收到 search 请求
  → 查 MetaCache: 这个 Collection 有 2 个 Shard
  → 也就是有 2 个 VChannel
  → 每个 VChannel 的数据可能在不同 QueryNode 上
  → Proxy 并发请求各 QueryNode
  → 汇总结果
```

每个 Shard 在 QueryNode 侧对应一个 **ShardDelegator** (`internal/querynodev2/delegator/delegator.go`),管理该 Shard 的 Growing + Sealed Segment。

---

## Q2: S3一定要用吗？可以用其他替代吗？

**不是必须的。** Milvus 支持三种存储类型，通过 `common.storageType` 配置项切换：

| 值 | 存储后端 | 适用场景 |
|----|---------|---------|
| `remote` (默认) | MinIO / S3 / Azure Blob / GCS / OpenDAL | 生产环境，分布式部署 |
| `local` | 本地文件系统 (`localStorage.path` 指定目录) | 单机开发测试 |
| `opendal` | Apache OpenDAL 统一存储抽象 | 支持更多存储后端 |

**本地存储的实现**在 `internal/storage/local_chunk_manager.go`，本质是 `os.WriteFile` + `mmap` 读写。

**关键代码**: `internal/storage/factory.go:17-26` — 工厂根据 `StorageType` 决定创建 `LocalChunkManager` 还是 `RemoteChunkManager`。

> **注意**: 生产环境推荐用 S3/MinIO，因为 QueryNode 和 DataNode 都需要访问同一份存储。本地磁盘模式仅在 Standalone 单进程下方便调试。

---

## Q2: etcd 原理简述一下

etcd 是一个**分布式 KV 存储**，核心基于 **Raft 共识算法**。

**核心机制**:
1. **Raft 一致性** — 多个 etcd 节点组成集群，选举一个 Leader，所有写操作通过 Leader 同步到 Follower，多数派确认后返回成功
2. **MVCC 存储** — 键值对带版本号 (revision)，每次修改版本号递增
3. **Lease 租约** — 客户端定期续约，租约过期则自动删除关联的 key，用于节点存活检测
4. **Watch 监听** — 客户端可 watch 某个 key 或前缀，变更时 push 通知，不需要轮询

**Milvus 怎么用 etcd**:
- **服务发现**: 每个组件启动时注册自己的地址到 etcd (带 lease)，其他组件通过 watch 发现新节点上线/下线
- **元数据存储**: Collection Schema、Segment 状态、索引元数据等都存 etcd
- **Leader 选举**: 单例 Coordinator (RootCoord/DataCoord/QueryCoord) 通过 etcd 做选主，挂了自动切

> Milvus 可选 **TiKV** 替代 etcd (通过 `internal/kv/tikv/` 适配)，两者共用同一套 `internal/kv/` 抽象接口。

---

## Q3: 向量检索引擎到底用哪个？可以配置吗？

**Milvus 使用 Knowhere，Knowhere 内部包装了多个引擎。** Knowhere 不是搜索引擎本身，而是对多种向量索引库的**统一抽象层**，类似 ODBC/JDBC 之于数据库。

**可配置的索引类型 (建 Collection 时通过 `index_type` 参数指定)**:

| 引擎 | 索引类型 | 说明 |
|------|---------|------|
| Faiss (Meta) | `IVF_FLAT`, `IVF_PQ`, `IVF_SQ8`, `FLAT`, `BIN_FLAT`, `BIN_IVF_FLAT` | CPU 通用，工业标准 |
| HNSW (hnswlib) | `HNSW`, `IVF_HNSW` | 图索引，速度快，内存大 |
| DiskANN (Microsoft) | `DISKANN` | 磁盘索引，内存受限场景 |
| ScaNN (Google) | `SCANN` | 高压缩比 + 高性能 |
| GPU-Raft (NVIDIA) | `GPU_IVF_FLAT`, `GPU_IVF_PQ` | GPU 加速 |
| GPU-CAGRA (NVIDIA cuVS) | `GPU_CAGRA`, `GPU_BRUTE_FORCE` | GPU 图索引 |
| 稀疏向量 | `SPARSE_INVERTED_INDEX`, `SPARSE_WAND` | 稀疏/全文向量 |

**运行时发现**: 具体支持哪些索引由编译进 C++ 的 segcore 决定，Go 端通过 `C.GetIndexListSize()` / `C.GetIndexFeatures()` 在运行时获取 — 见 `internal/util/vecindexmgr/vector_index_mgr.go:117-134`。

```python
# 配置示例
index_params = {
    "index_type": "IVF_FLAT",   # 用 Faiss IVF 索引
    "metric_type": "COSINE",    # 余弦相似度
    "params": {"nlist": 1024}
}
```

---

## Q4: IVF 思想？

**IVF (Inverted File)** = 用聚类缩小搜索范围，用空间换时间。

**思路类比**: 你要在 1000 万本书里找一本书，但不逐本翻 — 先把书分到 1024 个书架 (聚类)，查的时候只看最近的那几个书架。

**算法步骤**:

```
建索引阶段:
  1. 随机采样部分向量，用 K-Means 聚成 N 个簇 (默认 N=1024~65536)
  2. 每个簇有一个中心点 (centroid)
  3. 把全部向量按最近的中心点分到对应的簇，存到倒排列表 (inverted list)

搜索阶段:
  1. 把查询向量和 N 个中心点算距离 → 找最近的 nprobe 个簇
  2. 只在这 nprobe 个簇内的向量中做精确距离计算
  3. 返回最近的 topK 个
```

**核心参数**:
- `nlist` (建索引时) — 聚多少簇。越大精度越高，但内存越大
- `nprobe` (搜索时) — 搜几个簇。越大精度越高，但越慢

**为什么有效**: 如果向量在 1024 维空间中均匀分布，搜 1% 的簇就能覆盖大部分可能结果，速度提升 100 倍。

---

## Q5: WAL 是啥？跟 Kafka 什么关系？

**WAL (Write-Ahead Log)** 是一个**设计模式/逻辑概念**：数据在最终持久化之前，先写入一个追加日志 (append-only log)，保证不丢。

**Kafka 是实现 WAL 的物理载体之一**。

```
        逻辑层                    物理层
    ┌──────────────┐      ┌─────────────────┐
    │   WAL 抽象    │←────│ Kafka (分布式)   │
    │  (Append/     │←────│ Pulsar (分布式)   │
    │   Read/       │←────│ Woodpecker(自研) │
    │   Truncate)   │←────│ RocksMQ (单机)   │
    └──────────────┘      └─────────────────┘
```

**关键理解**:

- **WAL 是协议，Kafka 是实现。** Milvus 定义了 `WALImpls` 接口 (`pkg/streaming/walimpls/wal.go`)，Kafka/Pulsar/Woodpecker/RocksMQ 都是其实现
- **不是用 Kafka 做 pub/sub，而是当做日志存储。** 每个 PChannel 对应用 1 个 Kafka Topic/Partition，数据严格有序
- **Write-Ahead 的含义**: 
  1. 用户 insert → Proxy → append 到 Kafka (WAL) → 立即返回成功
  2. DataNode 异步消费 WAL → 持久化到 S3 → 通知 DataCoord 完成
  3. 如果 DataNode 在步骤 2 之前挂了 → 换个新的从 WAL 重新消费，数据不丢
- **Standalone 模式**: 用 RocksMQ (内嵌 RocksDB)，不需要启动 Kafka，适合单机开发

**为什么不用 Kafka 直接存数据而是再写一份 S3？**
- Kafka 存数据贵 (内存 + 磁盘)，S3 便宜
- WAL 保留时间短 (比如 3 天)，S3 永久保留
- WAL 是流式顺序读写，S3 适合大文件随机读 (Sealed Segment 有索引后 QueryNode 直接加载)

---

## Q6: Insert 请求的完整写入路径

一条数据从 Proxy 到 S3 一共经过 7 个阶段:

### 阶段 1: Proxy 接收 + TSO 分配

```go
// internal/proxy/impl.go:2744
func (node *Proxy) Insert(ctx context.Context, request *milvuspb.InsertRequest) (*milvuspb.MutationResult, error)
```

Proxy 把 `InsertRequest` 包成 `insertTask`，推入 `dmQueue` (DML 队列)。

**入队时立即分配 TSO** (`internal/proxy/task_scheduler.go:221`):
```go
ts, err = queue.tsoAllocatorIns.AllocOne(t.TraceCtx())  // 向 RootCoord 请求全局时间戳
t.SetTs(ts)  // 写入 BeginTimestamp 和 EndTimestamp
```
TSO 是一个全局单调递增的 int64，RootCoord 是 TSO 的唯一源。每个 insert 请求得到一个 TSO，用来保证全局写入顺序。

### 阶段 2: PreExecute — 分配 RowID + 校验

```go
// internal/proxy/task_insert.go:103
func (it *insertTask) PreExecute(ctx context.Context) error
```

- 分配 RowID (全局唯一主键，基于 TSO 生成)
- 给每行数据设置时间戳 (`BeginTimestamp`)
- 校验字段类型、长度限制、Partition Key 等

### 阶段 3: Execute — 按 VChannel 拆分 + 写入 WAL

```go
// internal/proxy/task_insert_streaming.go:29
func (it *insertTask) Execute(ctx context.Context) error
```

1. **按 PK Hash 分配到 VChannel**: 每行数据根据主键 hash 决定去哪个 Shard (VChannel)
2. **构建 Insert 消息** (`message.NewInsertMessageBuilderV1()`):
   - **Header**: CollectionID + Partition → Segment 分配信息 (`PartitionSegmentAssignment`)
   - **Body**: 列式字段数据 (`FieldsData[]`), RowIDs, Timestamps
3. **写入 WAL**:
   ```go
   resp := streaming.WAL().AppendMessages(ctx, msgs...)
   ```

### 阶段 4: StreamingNode — Segment 分配 + 持久化到 WAL 后端

Proxy 的 Append 请求到达 StreamingNode，经过 interceptor 链:

1. **Shard Interceptor** (`shard_interceptor.go:148`) — 最关键的步骤:
   - 检查 Collection Schema 版本是否匹配
   - **分配 Segment**: `shardManager.AssignSegment(req)` → 如果当前 Segment 满了或超时了就建新 Segment
   - 将 Segment 分配结果附到消息上
2. **Lock Interceptor** — 按 key 加锁保证同 VChannel 内顺序
3. **TimeTick Interceptor** — 维护消息的逻辑时间戳
4. **WAB (Write Ahead Buffer)** — 缓冲后批量刷盘
5. **WAL 后端写入** (`pkg/streaming/walimpls/wal.go:38`): 调用具体的 WAL 实现 (Kafka/Pulsar/RocksMQ) 持久化

### 阶段 5: DataNode 消费 — 从 WAL 读到内存 WriteBuffer

DataNode 订阅 WAL (`internal/flushcommon/pipeline/flow_graph_dmstream_input_node.go:44`)：

```
dmStreamNode (订阅 WAL)
  → ddNode (过滤: 按 MsgType 分类 Insert/Delete/CreateSegment)
  → writeNode (处理 Insert)
```

**writeNode** (`flow_graph_write_node.go:101`):
1. `PrepareInsert()` — 将 `msgstream.InsertMsg` 转为 `storage.InsertData` (按 Segment 分组的列式数据)
2. `WriteBuffer.BufferData()` — 数据进入内存 WriteBuffer,等待触发 flush

### 阶段 6: Flush — 内存 WriteBuffer → Binlog 文件

当满足 flush 条件时 (大小/时间/Fence 信号):

**SyncTask.Run()** (`internal/flushcommon/syncmgr/task.go:116`):
1. 根据存储版本选 Writer: `BulkPackWriter` (V1) / `BulkPackWriterV2` (V2) / `BulkPackWriterV3` (V3)
2. `BulkPackWriter.Write()` (`pack_writer.go:70`) 写入 4 类文件:
   - **InsertLog**: 字段数据,每个字段独立一个 binlog 文件
   - **StatsLog**: PK Bloom Filter
   - **DeltaLog**: 删除记录
   - **BM25Stats**: 全文检索统计

### 阶段 7: Binlog 格式 — 落盘到 S3/MinIO

```
一个 Segment 的落盘产物:
  Segment_12345/
    ├── insert_log/
    │   ├── 456_100_0.binlog      ← Field 100 (主键) 的插入数据
    │   ├── 456_101_0.binlog      ← Field 101 (向量) 的插入数据
    │   └── 456_102_0.binlog      ← Field 102 (标量) 的插入数据
    ├── stats_log/
    │   └── 456_100_0.binlog      ← PK 统计 (Bloom Filter)
    └── delta_log/
        └── 456_0_0.binlog        ← 删除记录

binlog 文件内部格式 (binlog_writer.go:122):
┌──────────────────────┐
│ MagicNumber (0xfffabc)│  ← 4 bytes
├──────────────────────┤
│ Descriptor Event      │  ← 元数据: CollectionID, PartitionID, SegmentID, FieldID, TS range
├──────────────────────┤
│ Insert Event 1        │  ← EventHeader(Timestamp, TypeCode, Length) + 列式 Payload
├──────────────────────┤
│ Insert Event 2        │
├──────────────────────┤
│ ...                   │
└──────────────────────┘
```

每个 `Insert Event` 包含 `InsertData` (列式存储):
- 系统字段: RowID (FieldID=0), Timestamp (FieldID=1)
- 用户字段: 按 FieldID 索引 (例如 FieldID=100 是 pk, FieldID=101 是 vector)
- 数据序列化为 Arrow Record Batch 或 Protobuf bytes

### 端到端时序图

```
Client          Proxy         RootCoord    StreamingNode    WAL(Kafka)    DataNode      S3/MinIO
  │               │               │              │              │             │             │
  │ Insert(100条) │               │              │              │             │             │
  │──────────────→│               │              │              │             │             │
  │               │ AllocTSO()    │              │              │             │             │
  │               │──────────────→│              │              │             │             │
  │               │  ts=1001      │              │              │             │             │
  │               │←──────────────│              │              │             │             │
  │               │               │              │              │             │             │
  │               │ PreExecute:   │              │              │             │             │
  │               │ 分配RowID     │              │              │             │             │
  │               │ 校验Schema    │              │              │             │             │
  │               │               │              │              │             │             │
  │               │ Execute:      │              │              │             │             │
  │               │ 按PK hash→    │              │              │             │             │
  │               │ 2个VChannel   │              │              │             │             │
  │               │               │ AppendMessages             │             │             │
  │               │───────────────│─────────────→│              │             │             │
  │               │               │ Assign Seg   │              │             │             │
  │               │               │              │ Append(WAL)  │             │             │
  │               │               │              │─────────────→│             │             │
  │               │←── 返回成功 ──│←─────────────│              │             │             │
  │←── 返回成功 ──│               │              │              │             │             │
  │               │               │              │              │             │             │
  │               │               │              │  消费消息     │             │             │
  │               │               │              │              │────────────→│             │
  │               │               │              │              │             │ BufferData  │
  │               │               │              │              │             │ 内存缓冲     │
  │               │               │              │              │             │             │
  │               │               │              │              │   ...攒够flush条件...     │
  │               │               │              │              │             │             │
  │               │               │              │              │             │ SyncTask    │
  │               │               │              │              │             │ BuildBinlog │
  │               │               │              │              │             │─────────────→│
  │               │               │              │              │             │←── 上传完成 │
```

### 关键的"分叉"点

- **Proxy 按 PK Hash 分 VChannel** (`task_insert_streaming.go:101`): 决定数据去哪个 Shard
- **StreamingNode 分配 Segment** (`shard_interceptor.go:206`): 决定数据写入哪个 Segment (Growing/新建)
- **DataNode Flush** (`write_buffer.go:510`): 决定何时把内存数据刷到 S3
- **Field 级别独立 Binlog**: 每个字段独立一个文件,加载时可以只读需要的字段

### 代码索引

| 步骤 | 文件 | 行列 |
|------|------|------|
| Proxy 入口 | `internal/proxy/impl.go` | :2744 |
| TSO 分配 | `internal/proxy/task_scheduler.go` | :221 |
| PreExecute | `internal/proxy/task_insert.go` | :103 |
| Execute (WAL 写入) | `internal/proxy/task_insert_streaming.go` | :29 |
| WAL AppendMessages | `internal/distributed/streaming/util.go` | :22 |
| StreamingNode Appender | `internal/streamingnode/server/wal/adaptor/wal_adaptor.go` | :154 |
| Shard Interceptor | `internal/streamingnode/server/wal/interceptors/shard/shard_interceptor.go` | :148 |
| WAL 后端接口 | `pkg/streaming/walimpls/wal.go` | :31 |
| DataNode 订阅 | `internal/flushcommon/pipeline/flow_graph_dmstream_input_node.go` | :44 |
| DD Node 过滤 | `internal/flushcommon/pipeline/flow_graph_dd_node.go` | :96 |
| WriteNode | `internal/flushcommon/pipeline/flow_graph_write_node.go` | :101 |
| WriteBuffer | `internal/flushcommon/writebuffer/write_buffer.go` | :510 |
| SyncTask Run | `internal/flushcommon/syncmgr/task.go` | :116 |
| Binlog Writer | `internal/storage/binlog_writer.go` | :122 |
| Binlog 格式定义 | `internal/storage/event_header.go`, `insert_data.go`, `data_codec.go` | — |
