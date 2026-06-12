# Milvus 源码研读方法

## 核心策略: 入口驱动 + 数据流追踪 + 分层验证

### 1. 从入口文件找组件拓扑

大型 Go 项目的入口通常在 `cmd/` 目录。

**方法**: 找到 main → run → 组件注册链,绘制组件启动图。
**Milvus 实践**:
- `cmd/main.go` → `cmd/milvus/run.go:49` → `cmd/roles/roles.go:369 Run()`
- 在 `roles.go:486-521` 看到所有组件按条件启动: MixCoord, QueryNode, DataNode, Proxy, StreamingNode, CDC
- 在 `cmd/components/` 下看到每个组件的 `NewXxx()` 工厂方法

**产出**: 一张组件拓扑图 + 每个组件的工厂方法位置

### 2. 追踪一条完整请求路径

选一个最常见的 API,从 Proxy → Coordinator → Node → Storage 完整追踪。

**Milvus 实践**:
- 选 `CreateCollection` (非 trivial, 涉及 RootCoord + WAL 广播)
- `internal/proxy/impl.go` 搜索 `CreateCollection` → 找到 gRPC handler
- handler 创建 `createCollectionTask` → 推入 TaskScheduler → Execute → 调用 RootCoord
- `internal/rootcoord/root_coord.go` → `ddl_callbacks_create_collection.go` → etcd 写入 + WAL 广播
- 看设计文档 `docs/design-docs/design_docs/20211217-milvus_create_collection.md` 验证理解

**产出**: 请求的完整调用链,从 gRPC 入口到存储持久化

### 3. 识别核心抽象 (Interface)

找项目中最关键的 Interface,它们定义了组件间的契约。

**Milvus 实践**:
- `internal/types/types.go` 定义了所有组件接口: Component, Proxy, DataNode, QueryNode, RootCoord, DataCoord, QueryCoord
- 每个组件都有 `var _ types.Xxx = (*Xxx)(nil)` 编译期验证
- 搜索 `types.Xxx` 找到接口的实现者和调用者

**产出**: 组件接口图 + 依赖关系矩阵

### 4. 分层深入: 自顶向下

| 层级 | 关注点 | Milvus 对应目录 |
|------|--------|----------------|
| L1 入口层 | 进程启动,配置文件,组件注册 | `cmd/`, `configs/` |
| L2 API 层 | gRPC 接口,请求路由,鉴权限流 | `internal/proxy/` |
| L3 协调层 | 元数据管理,任务调度,负载均衡 | `internal/rootcoord/`, `internal/*coord*/` |
| L4 执行层 | 数据处理,查询执行,向量检索 | `internal/querynodev2/`, `internal/datanode/` |
| L5 流系统 | WAL 写入/消费,消息传递 | `internal/streaming*/`, `pkg/streaming/` |
| L6 存储层 | 对象存储,数据格式,序列化 | `internal/storage/`, `internal/storagev2/` |
| L7 引擎层 | C++ 索引,查询,向量计算 | `internal/core/src/` |

**每次深入一层时,先读该层的 README/设计文档,再读代码。**

### 5. 利用 CLAUDE.md / AGENTS.md

Milvus 项目提供了 `CLAUDE.md` (88行),包含:
- 子系统索引 (`docs/agent_guides/streaming-system/`)
- 测试命令 (`go test -tags dynamic,test -gcflags="all=-N -l"`)
- 代码规范 (merr 错误处理, paramtable 配置, log 日志)
- PR 规范 (commit 格式, 设计文档要求)

**把这个文件当作优先参考资料。**

### 6. 利用设计文档验证理解

`docs/design-docs/design_docs/` 包含 50+ 设计文档,按日期命名:
- `20211217-milvus_create_collection.md` — Collection 创建流程
- `20210731-index_design.md` — 索引构建架构
- `20211214-milvus_hybrid_ts.md` — 混合时间戳设计
- `20230418-querynode_v2.md` — QueryNode v2 设计
- `20250610-rls_design.md` — 行级安全
- `20251114-snapshot_design.md` — Snapshot 机制

**方法**: 选定要读的模块 → 找到对应的设计文档 → 先读文档建立心智模型 → 再读代码验证 → 最后交叉验证文档与代码的一致性

### 7. 核心数据结构优先

理解一个系统,先搞清楚它操作什么数据。

**Milvus 核心数据结构**:
- `Collection` (Schema + Properties) — 数据表定义
- `Segment` (Growing/Sealed) — 数据存储单元
- `VChannel / PChannel` — 逻辑/物理分片
- `TimeTick / HybridTs` — 逻辑时钟
- `Binlog` — 持久化文件格式
- `IndexMeta` — 索引元数据

**方法**: 找到定义这些结构的 proto 文件 → 理解每个字段的含义 → 追踪谁创建、谁消费这些结构

### 8. 利用 Git Log 了解演进

```bash
git log --oneline -50 internal/querynodev2/delegator/delegator.go
```

看一个文件的修改历史,了解:
- 谁在维护这个模块
- 最近的改动趋势
- 关联的 Issue/PR 编号

### 9. 阅读测试代码

测试是理解模块行为的最佳文档:
```bash
# 单元测试
internal/proxy/task_search_test.go
internal/querynodev2/delegator/delegator_test.go

# 集成测试
tests/go_client/testcases/search_test.go
tests/go_client/testcases/insert_test.go
```

### 10. 建立个人知识索引

为每个核心组件创建索引卡片:

```
【QueryNode】
- 入口: server.go:NewQueryNode()
- 核心抽象: ShardDelegator (delegator/delegator.go)
- 关键文件: segment.go, search.go, pipeline.go
- 设计文档: 20230418-querynode_v2.md, 20211223-knowhere_design.md
- 测试入口: server_test.go, delegator_test.go
- 依赖: etcd, MinIO, segcore(CGo)
```

---

## 一次典型研读 Session 流程

```
1. 选模块 (如 QueryNode 搜索路径)
2. 读设计文档 (如 querynode_v2.md)
3. 读测试用例 (如 delegator_test.go 中的 TestSearch)
4. 从头到尾追踪一个 Search 请求:
   Proxy.task_search.go → Proxy.search_pipeline.go
   → QueryCoord (路由) → QueryNode.delegator.Search()
   → segment.search() → C++ segcore
5. 画调用链流程图
6. 记录到 codereading.md
```

---

## 工具链

| 工具 | 用途 |
|------|------|
| `rg/grep` | 搜索函数定义、调用关系 |
| `glob` | 按文件名模式查找 |
| `git log --oneline` | 查看文件修改历史 |
| `git blame` | 找到某行代码的作者/commit |
| `go test -tags dynamic,test -gcflags="all=-N -l"` | 运行 Milvus 单元测试 |
| `make test-querycoord` 等 | 按模块运行测试 |
