# 代码规模与统计

## 代码规模总览

| 类别 | 文件数 | 行数 | 说明 |
|------|--------|------|------|
| **Go 源码**（非测试/生成/vendor） | 120 | **29,815 行** | 主程序 + 核心库 |
| **Go 测试代码** | 112 | **63,743 行** | 单测 + E2E（测试比源码还多） |
| Go 合计 | 232 | **93,558 行** | — |
| Shell 脚本（hack/） | — | ~2,180 行 | 验证/部署脚本 |
| YAML（charts/hack） | — | ~2,365 行 | Helm 模板 + 配置 |
| Markdown 文档 | — | ~5,849 行 | docs + README |

## 按目录分布（Go 源码，非测试）

| 目录 | 行数 | 内容 |
|------|------|------|
| `pkg/` | **25,614** | 核心库（绝大部分代码） |
| `cmd/` | 2,814 | 三个二进制入口 |
| `hack/` | 663 | 自定义验证工具（preferredimports、rbaccheck） |
| `test/` | 724 | E2E 测试框架 |

`pkg/` 内部进一步拆分（含测试的粗略量级）：

| 子目录 | 规模 | 角色 |
|--------|------|------|
| `pkg/device/` | ~9,800 行源码 + 大量测试 | 设备抽象层 + 16 个厂商后端 |
| `pkg/device-plugin/` | ~7,700 行源码 | NVIDIA 设备插件内部实现（fork 自 NVIDIA 官方） |
| `pkg/scheduler/` | ~3,500+ 行源码 | 调度器核心 + 路由 + 策略 |
| `pkg/util/` | ~800 行 | 节点锁/选举/客户端 |
| `pkg/monitor/` | ~800 行 | 容器 GPU 监控 |

## 几个值得注意的点

1. **测试代码量 > 源码量**（63,743 vs 29,815，约 2.1:1）——这是质量信号，说明项目测试覆盖投入很大。尤其 `pkg/device/nvidia/device_test.go` 单文件就 ~110KB（是最大的测试文件），覆盖 MIG 拓扑、P2P 评分等复杂逻辑。

2. **纯 Go 源码约 3 万行**属于**中小型项目**，对于想完整读懂一个项目的新手是合适的规模——比动辄几十万行的 Kubernetes 本体小一个量级，但已足够体现"调度器扩展 + 设备插件 + 容器内拦截"的完整设计。

3. **`libvgpu.so` 的 C 代码不计入**——它是 HAMi 的运行时隔离核心，在 `libvgpu/` 子模块里（当前 checkout 为空），实际是另一个仓库维护的，所以这里的 Go 行数没包含它。

4. **16 个设备后端是代码主体之一**——`pkg/device/` 近万行里相当一部分是各厂商后端的重复模式实现（每个后端实现相同的 13 个接口方法），这也是为什么 NVIDIA 后端单独就 ~1000 行而简单后端只有几百行。

## 学习投入预估

按 3 万行源码、中等复杂度估算，配合已有的 6 篇学习文档，一个有 Go+K8s 基础的人：

- **完整读懂核心链路**（scheduler + nvidia 后端 + device plugin allocate）：大约需要 **1-2 周**集中投入
- **做到能贡献第一个 PR**：再加 **3-5 天**熟悉测试模式和提交流程

## 统计方法说明

上述数据用以下方式统计（可复现）：

```bash
# Go 源码总行数（排除测试/生成代码/vendor/子模块）
git ls-files '*.go' | grep -v '_test\.go$' \
  | grep -vE '(zz_generated|\.pb\.go)' \
  | grep -vE '^vendor/|^libvgpu/|^third_party/' \
  | xargs cat | wc -l

# Go 测试代码总行数
git ls-files '*_test.go' | grep -vE '^vendor/' | xargs cat | wc -l

# 按顶层目录统计
for d in cmd pkg charts hack test lib; do
  git ls-files "$d/*.go" | grep -v '_test\.go$' | xargs cat | wc -l
done
```

> 注：`pkg/device-plugin/` 的规模偏大是因为它 fork 自 NVIDIA 官方 k8s-device-plugin 并改造，包含了完整的设备插件框架代码；`.golangci.yaml` 也特意把 `pkg/device-plugin/` 排除在 lint 之外。

---

## main.go 读完后的精读路径

读 `cmd/scheduler/main.go` 时，你已经看到了调度器的**启动骨架**：初始化设备 → 建 Scheduler → 起缓存同步循环 → 起指标服务 → 注册 6 个 HTTP 路由 → 起服务。接下来最好的做法是**顺着请求流走**，而不是按目录乱读。

### 推荐阅读路径（按请求流，4 步走）

```
main.go(已读) → routes/route.go → scheduler.go:Filter → score.go:calcScore → device.Fit()
```

### 第 1 步：HTTP 入口 `pkg/scheduler/routes/route.go`

从 **`PredicateRoute()`（第 52 行）** 开始读。这是 `/filter` 路由的入口，整个文件才 200 多行，6 个 handler 都在这。

读这一个函数要搞清楚：kube-scheduler 发来的 `ExtenderArgs` 怎么被解析、`WaitForCacheSync` 在等什么、最后怎么返回 `ExtenderFilterResult`。读完它你会发现核心调用就是一行 `s.Filter(extenderArgs)`。

> 同文件里可以顺手扫一眼 `Bind()`（114 行）和 `NumaRefit()`（191 行），但**先别深入**，它们是 Filter 走通后的后续环节。

### 第 2 步：核心 `scheduler.go` 的 `Filter()`（第 1136 行）

这是**整个项目最核心的函数**，顺着第 1 步的 `s.Filter()` 进来。建议把它分成 5 段，每段问自己一个问题：

| 段落（按代码顺序） | 要搞清楚的问题 |
|---|---|
| ① 解析资源请求 | `device.Resourcereqs(pod)` 怎么从 Pod spec 算出"要几张 GPU、多少显存" |
| ② 构建 NodeUsage 快照 | `getNodesUsage()`（813 行，可跳进去看一眼就回来）怎么把物理设备 + 已分配用量拼成每个节点的"剩余视图" |
| ③ 并行评分 | `calcScore()` 怎么对所有候选节点打分 |
| ④ 选节点+写注解 | 怎么按 binpack/spread 选最佳节点，然后 `PatchAnnotations` 把分配方案写成 Pod 注解 |
| ⑤ 预留+回滚 | 怎么把这次分配记进 `podManager`/`quotaManager`，patch 失败怎么回滚 |

> 注意：之前读过的 `RegisterFromNodeAnnotations`（缓存同步）产出的 `nodeManager` 缓存，就是 ② `getNodesUsage()` 的数据来源——这一步正好把"后台同步"和"请求处理"两头接上了。

### 第 3 步：评分 `score.go` 的 `calcScore()`（第 410 行）

第 2 步 ③ 调用的 `calcScore` 在另一个文件。它对每个节点开 goroutine 并行算分，每个节点调 `scoreNode()`（345 行）。

这一步只需搞清楚一件事：`scoreNode()` 怎么"模拟分配"（deep copy 节点用量 → 试着把 Pod 放上去 → 算利用率），以及它怎么调 `devPlugin.Fit()` 判断放不放得下。

### 第 4 步：设备级 `Devices.Fit()` —— 选一个后端读

第 3 步的 `Fit()` 是接口调用，具体实现是各厂商后端。先读 **`pkg/device/nvidia/device.go` 的 `Fit()`**（最完整、最有代表性，约第 853 行附近）。

只看一个循环：它遍历节点上每张 GPU，依次检查健康/型号/UUID/显存/核心/配额……看一张卡能不能满足请求。这个检查顺序就是 HAMi 调度的精髓。

### 节奏总结

| 步 | 文件 | 函数(行号) | 目标 |
|---|---|---|---|
| 1 | `routes/route.go` | `PredicateRoute` (52) | 看清 HTTP 请求怎么进来 |
| 2 | `scheduler.go` | `Filter` (1136) | **核心**：5 段全流程 |
| 3 | `score.go` | `calcScore` (410) / `scoreNode` (345) | 并行评分怎么算 |
| 4 | `device/nvidia/device.go` | `Fit` (~853) | 单卡怎么判断放得下 |

第 2 步是重中之重，建议在那多花时间，其余几步都是为它服务的。读到第 4 步，就完成了"一个 Pod 进来 → 选节点 → 选 GPU → 出分配方案"的完整闭环，HAMi 调度的核心就通了。

> 之后想继续，再回头读 `Bind()`（1067 行，加锁+真正绑定）、`Devices` 接口定义（`pkg/device/devices.go`）、各策略实现（`pkg/scheduler/policy/`）。但**先别碰**，把上面 4 步走通再说，避免一上来就读得太散。

> 注：以上行号基于 2026-09 的代码，后续若文件改动行号可能漂移，以 `grep -nE '^func ... Filter|calcScore' pkg/scheduler/*.go` 为准。
