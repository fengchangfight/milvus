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
