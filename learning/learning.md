# 调度器启动流程

md/scheduler/main.go

调度器的启动骨架：初始化设备 → 建 Scheduler → 起缓存同步循环（go sher.RegisterFromNodeAnnotations()） → 起指标服务 → 注册 6 个 HTTP 路由 → 起服务。接下来最好的做法是顺着请求流走，而不是按目录乱读。

# NodeCacheCapable 是什么

- 是 kube-scheduler extender 配置里的一个能力声明位，写在调度器策略/配置文件里，告诉 kube-scheduler："这个 extender 自己维护了一份节点缓存，不需要你把完整节点对象传过来"。

为什么 HAMi 要设成 true（设成 true 的好处）：

- 省网络带宽和序列化开销。
- HAMi 本来就自己维护节点缓存。
- HAMi 的过滤依赖自己的设备视图，不是 kube-scheduler 的节点快照。HAMi 调度看的是"这个节点上有几张卡、被哪些 Pod 占了多少显存"——这些信息 kube-scheduler 的 Node 对象里根本没有（HAMi 通过 node annotation 和自己的 cache 维护）。传完整 NodeList 过来 HAMi 也用不上那个设备视图，反而是个累赘。

设成 true 的代价和注意点：
- 必须保证本地缓存与 apiserver 一致。
    - 如果 HAMi 的 informer 缓存落后了（网络抖动、informer resync 没跟上），它过滤的是过时的节点列表，可能漏选新节点或误选已删除的节点。这就是为什么 Start() 里 scheduler.go:433-435 要 WaitForCacheSync 等 Node informer 同步完才置 started=1，route.go:90 还要 s.WaitForCacheSync(r.Context()) 在每次 filter 时再确认一次缓存已同步——这是 nodeCacheCapable: true 模式下的安全底线。
- 节点删除时的竞态。
    - kube-scheduler 给的 NodeNames 里可能还有刚被删的节点名（它自己也有缓存延迟），HAMi 用名字去本地缓存查时要处理"查不到"的情况。HAMi 的 getNodesUsage 就要做这种防御。
- 协议必须对齐。
    - 一旦声明 true，kube-scheduler 只发名字，HAMi 必须能只靠名字工作；而 HAMi 返回时也必须填 NodeNames 而非 Nodes。两边任何一方填错字段，调度就会失效（通过节点为空，Pod 永远 pending）。HAMi 在 scheduler.go:1155 这种"无 HAMi 资源直接放行"的分支里，特意把两个字段都回传就是为了在这种边界情况下也不出错。

# WaitForCacheSync 缓存同步

route.go:112 的 `s.WaitForCacheSync(r.Context())` 等的是 **HAMi scheduler extender 进程内部自己的一个标志位 `s.synced`**，和 kube-scheduler 没有任何关系。

## 等的是谁：HAMi 自己的，不是 kube-scheduler 的

看实现 scheduler.go:686-696：

```go
func (s *Scheduler) WaitForCacheSync(ctx context.Context) bool {
    err := wait.PollUntilContextCancel(ctx, syncedPollPeriod, true, func(context.Context) (done bool, err error) {
        return s.synced.Load(), nil   // ← 轮询 s.synced 这个 atomic.Bool
    })
    ...
}
```

它只是个轮询循环，每隔 `syncedPollPeriod` 检查一次 `s.synced`，等到它变 `true` 才返回。kube-scheduler 完全不参与这个过程。

## `s.synced` 的真正含义：设备清单已就绪

关键在它**在哪里被置位**——不是在 `Start()` 那个 informer 缓存同步处，而是在 `register()` 函数末尾。看 scheduler.go:610-618：

```go
func (s *Scheduler) register(...) {
    ...
    _, overallnodeMap, _, err := s.getNodesUsage(&nodeNames, nil)  // 扫描所有节点的设备用量
    if err != nil {
        klog.ErrorS(...)
        return
    }
    s.overviewstatus = *overallnodeMap   // 重建节点用量快照

    // Set synced to true only after getNodeUsage() succeeds
    s.synced.Store(true)                  // ← 这里！
}
```

而 `register()` 是被 `RegisterFromNodeAnnotations` 那个后台循环调用的（就是 scheduler.go:447 那个 `for/select` 死循环）。

所以 `s.synced = true` 的完整条件是：**后台循环至少成功跑完了一轮"扫描所有节点、读取各厂商设备清单、计算用量快照"**。

## 缓存同步的源是什么（四样合起来的产物）

HAMi 在等的"缓存"不是单一来源，而是两个层级叠加：

### 第一层：K8s informer 本地缓存（节点/Pod 对象本身）

这是 `Start()` 阶段建立的，scheduler.go:433-435：

```go
informerFactory.Start(s.stopCh)
informerFactory.WaitForCacheSync(s.stopCh)   // ← 等 Pod/Node informer 缓存同步
```

这一层保证 HAMi 能从本地 Lister 拿到**准确的节点列表和 Pod 列表**。源是 **apiserver**（informer 通过 watch/list 从 apiserver 拉取对象到本地）。

但注意：这一层同步完，`s.synced` 并没被置位。也就是说 **informer 缓存就绪 ≠ 设备视图就绪**。

### 第二层：HAMi 自己的设备清单缓存（基于 informer + 节点注解构建）

这才是 `s.synced` 等的东西。它由 `register()` 循环构建，源是**多个**：

1. **节点注解 `hami.io/node-{vendor}-register`**——各厂商设备插件上报到节点 annotation 里的设备清单（每张卡的型号、显存、核数等）。这是设备数据的**第一手来源**，由各节点的 device plugin 写入。
2. **Node informer 缓存**——`s.nodeLister.List(labelSelector)` 列出节点，读上面那些注解。
3. **各厂商 device backend 的 `GetNodeDevices` / `CheckHealth`**——scheduler.go:514-515 那段，对每个节点、每个厂商调用 `devInstance.GetNodeDevices(node)` 和 `devInstance.CheckHealth(...)`，把注解里的原始设备清单解析成结构化的 `NodeDevices`，并做健康检查。
4. **Pod informer 缓存**——`getNodesUsage` 里要算"每个节点上各 Pod 已占了多少设备"，这依赖 Pod 列表（哪些 Pod 调度在哪个节点、占了什么资源）。

这四样合起来，`register()` 才能产出 `s.overviewstatus`（每节点用量快照），最后 `s.synced.Store(true)`。

## 为什么 filter 时要再等一次

route.go:112 在每次 `/filter` 请求进来时都 `WaitForCacheSync`，是因为 `s.synced` **可能会被重新置 false**。看 scheduler.go:113：

```go
OnStoppedLeading: func() {
    s.synced.Store(false)   // ← 失去 leader 身份时，标记为未同步
},
```

当 HAMi 多副本部署、发生主备切换时，刚当选的 leader 之前可能不是 leader、没跑过 `register()`，它的 `s.synced` 是 `false`。新 leader 必须等 `RegisterFromNodeAnnotations` 循环触发 `leaderNotify`→跑一轮 `register()`→`s.synced=true`，才能开始过滤。否则它会基于空的设备视图做调度，所有 Pod 都会 pending。

注释 scheduler.go:507-509 也专门讲了 `s.synced` 在主备切换时的置位竞态：

```go
// Lock here to avoid setting s.synced to false, when we lost leadership, while doing register.
// 1. lost leadership before register: synced will set to false in callbacks, and register will be skipped because IsLeader() returns false
// 2. lost leadership during or after register: synced will set to true after finishing register, and callback will set it to false again after lock is acquired by callback
```

## 完整的等待链路梳理

把整条链画出来：

```
HAMi 进程启动
  └─ Start()
       └─ informerFactory.WaitForCacheSync()        第一层：K8s 对象缓存就绪（源=apiserver）
       └─ s.started = 1                              放行后台循环
  └─ go RegisterFromNodeAnnotations()               后台循环（已在跑）
       └─ select { nodeNotify | leaderNotify | ticker }
            └─ register()
                 ├─ 读节点注解（源=device plugin 写的 annotation）
                 ├─ GetNodeDevices/CheckHealth（源=各厂商 backend）
                 ├─ getNodesUsage（源=Pod informer 缓存）
                 ├─ overviewstatus 重建
                 └─ s.synced = true  ← WaitForCacheSync 等的就是这个

/filter 请求进来
  └─ s.WaitForCacheSync(r.Context())   轮询 s.synced，就绪才调 s.Filter()
```

## 和 nodeCacheCapable 的呼应

HAMi 设了 `nodeCacheCapable: true`，意味着它**向 kube-scheduler 承诺"我自己有缓存，你只发节点名字就行"**。这个承诺的责任，就由这条 `WaitForCacheSync` 守住：**HAMi 在用自己的设备缓存做调度决策前，必须确认那份缓存真的构建完毕**。如果 `s.synced` 还没 true 就去过滤，相当于在"空头承诺"——告诉 kube-scheduler"我有缓存"但实际还没构建好，会过滤出错误结果。

route.go:93-94 在 `WaitForCacheSync` 返回 false 时返回错误：

```go
synced := s.WaitForCacheSync(r.Context())
if !synced {
    err := fmt.Errorf("context cancelled")
    ...
    extenderFilterResult = &extenderv1.ExtenderFilterResult{Error: err.Error()}
}
```

宁可拒绝这次 filter 让 kube-scheduler 重试，也不基于不完整的设备视图做决策。

## 一句话总结

`WaitForCacheSync` 等的是 **HAMi 自己**的设备清单缓存就绪（`s.synced` 标志），与 kube-scheduler 无关。这个缓存的源是**四样合起来的产物**：节点 annotation 里的设备清单（device plugin 上报）+ Node informer 缓存 + 各厂商 backend 解析 + Pod informer 缓存，由后台 `register()` 循环构建。filter 时再等一次，是为了守住 `nodeCacheCapable: true` 这个承诺——不基于未构建好的缓存做调度决策，尤其在主备切换后新 leader 刚上任时。

# HAMi Predicate 和 Bind 的流程总览

HAMi 作为 kube-scheduler 的 extender，通过两个 HTTP 路由参与调度：`/filter`（过滤）和 `/bind`（绑定）。两阶段的通信双方始终是 **kube-scheduler ↔ HAMi**，kubelet 不参与。

## Predicate（/filter）阶段

### 数据流向

```
kube-scheduler ──HTTP POST /filter──> HAMi ──返回 JSON──> kube-scheduler
   (ExtenderArgs)                              (ExtenderFilterResult)
```

### 关键数据结构

**输入 `ExtenderArgs`**（kube-scheduler 发来）：

```go
type ExtenderArgs struct {
    Pod       *v1.Pod      // 正在被调度的 Pod（核心：带 vGPU 资源请求）
    Nodes     *v1.NodeList // 候选节点（nodeCacheCapable=false 时用，HAMi 不走这条）
    NodeNames *[]string    // 候选节点名（nodeCacheCapable=true 时用，HAMi 走这条）
}
```

**输出 `ExtenderFilterResult`**（HAMi 返回）：

```go
type ExtenderFilterResult struct {
    Nodes         *v1.NodeList  // 通过过滤的节点
    NodeNames     *[]string     // 通过的节点名（HAMi 填这个）
    FailedNodes   FailedNodesMap // 被淘汰的节点 + 失败原因
    FailedAndUnresolvableNodes FailedNodesMap // 抢占也救不回来的节点
    Error         string        // 整体错误信息
}
```

### 处理步骤（route.go:60-151）

1. `checkBody` 校验请求体非空。
2. `io.LimitReader` 限制 body 1MB，防资源耗尽攻击；`io.TeeReader` 留底（注：当前 buf 未被读取，属冗余）。
3. `json.Decode` 把请求体解码进 `ExtenderArgs`。
4. 校验 `extenderArgs.Pod == nil` → 返回错误。
5. **`s.WaitForCacheSync(r.Context())`** —— 等 HAMi 自己的设备清单缓存就绪（`s.synced`），未就绪返回 `context cancelled` 错误。这是 `nodeCacheCapable: true` 模式下的安全底线。
6. `s.Filter(extenderArgs)` 执行实际过滤：对每个候选节点，调用各厂商 device backend 判断 Pod 能否在该节点分配设备。
7. `json.Marshal` 结果，`writeResponse` 返回 HTTP 200 + JSON。

### 注意点

- 即使出错（解析失败、缓存未同步、Filter 内部错误）也是 HTTP 200 + `ExtenderFilterResult.Error`，不用 HTTP 错误码——这是 extender 协议约定：过滤失败是结果里的 `Error` 字段，让 kube-scheduler 统一解析。
- `hasHAMiResource == false` 的 Pod（不申请 vGPU 资源）直接把候选节点原样回传（`Nodes` 和 `NodeNames` 都回传），兼容两种协议形态。

## Bind（/bind）阶段

### 数据流向

```
kube-scheduler ──HTTP POST /bind──> HAMi ──返回 JSON──> kube-scheduler
   (ExtenderBindingArgs)                     (ExtenderBindingResult)
                                          ↓
                              HAMi 直接写 apiserver 完成 Pod binding
                              （不经过 kubelet）
```

### 关键数据结构

**输入 `ExtenderBindingArgs`**（kube-scheduler 发来）：

```go
type ExtenderBindingArgs struct {
    PodName      string     // Pod 名
    PodNamespace string     // 命名空间
    PodUID       types.UID  // Pod UID
    Node         string     // kube-scheduler 已选定的目标节点
}
```

**输出 `ExtenderBindingResult`**（HAMi 返回）：

```go
type ExtenderBindingResult struct {
    Error string   // 空串=成功，非空=失败原因
}
```

### 处理步骤（route.go:154-204 + scheduler.go:1067-1134）

1. `checkBody` + `LimitReader` + `TeeReader`（同 filter，buf 同样未被读取）。
2. `json.Decode` 解码进 `ExtenderBindingArgs`。
3. `s.Bind(extenderBindingArgs)` 执行实际绑定，内部流程：
   1. 用 `PodName` 去 `podLister` 查完整 Pod 对象（`nodeCacheCapable` 模式下 kube-scheduler 只发名字）。
   2. 用 `Node` 去 `nodeLister` 查目标节点。
   3. `acquireNodeLocks` 加节点分布式锁，防并发绑定。
   4. `PatchPodAnnotations` 给 Pod 打 `DeviceBindPhase: "allocating"` 注解。
   5. **`s.kubeClient.CoreV1().Pods().Bind()`** —— 直接调 apiserver 写 `Binding` 对象，完成 Pod 到节点的绑定。这一步是 HAMi 亲自写 apiserver，不是 kube-scheduler 写。
   6. 失败时 `releaseAllDevices` 释放已占设备、回滚。
4. `json.Marshal` 结果，`writeResponse` 返回。

### 为什么 HAMi 要接管 bind

正常 K8s 流程里绑定是 kube-scheduler 自己写 apiserver。配了 `bindVerb: bind` 后，kube-scheduler 改成调 HAMi 让 HAMi 去写。原因：HAMi 必须在绑定的同时塞进自己的私货——设备分配注解、节点锁、bindphase 标记——这些是 kube-scheduler 原生绑定做不了的。HAMi 借 bind 的时机把设备分配结果写进 Pod annotation，供下游 device plugin 读取。

## kubelet 何时出场

bind 写完 apiserver 后，kubelet 才作为下游消费者出场，但走的是另一条链：

```
HAMi 写 binding 到 apiserver
  → apiserver 更新 Pod.spec.nodeName
    → 目标节点 kubelet 发现 Pod 要就绪
      → kubelet 调该节点 device plugin 的 Allocate()（gRPC）
        → device plugin 读 Pod annotation 里的设备分配结果
        → 设置容器环境变量、挂载设备
      → kubelet 起容器
```

注意：device plugin 的 `AllocateRequest` 是另一套协议，不是 `ExtenderBindingArgs`。HAMi scheduler 和 device plugin 是两个独立进程，通过 Pod annotation 间接传递设备分配信息，不直接通信。

## 三阶段职责对照

| 阶段 | 谁调谁 | 传什么 | 干什么 |
|------|--------|--------|--------|
| filter | kube-scheduler → HAMi `/filter` | `ExtenderArgs`（Pod+节点名） | HAMi 返回哪些节点能跑 |
| bind | kube-scheduler → HAMi `/bind` | `ExtenderBindingArgs`（Pod名+节点名） | HAMi 写 apiserver 完成绑定 + 塞设备分配注解 |
| allocate | kubelet → device plugin | `AllocateRequest` | device plugin 读注解、设环境变量、挂设备 |

## 一句话总结

Predicate 阶段 kube-scheduler 发 `ExtenderArgs`（Pod+候选节点名）给 HAMi，HAMi 过滤后返回 `ExtenderFilterResult`（通过/淘汰节点+错误）；Bind 阶段 kube-scheduler 发 `ExtenderBindingArgs`（Pod名+选定节点）给 HAMi，HAMi 直接写 apiserver 完成绑定并返回 `ExtenderBindingResult`（仅成功/失败）。两阶段都是 kube-scheduler ↔ HAMi 通信，kubelet 要等 bind 写完 apiserver 后才通过 device plugin 的 Allocate 接口出场，传的是另一套协议。

---

# HAMi 在 kube-scheduler 调度流水线中的位置

## 关键澄清：不是"HAMi 过滤了 GPU，CPU/mem 要 scheduler 再做一次"

顺序是反过来的：**kube-scheduler 先用内置 filter 把 CPU/mem/亲和性/taints 等通用条件全筛一遍，把通过的候选节点交给 HAMi，HAMi 在此基础上再做 GPU 专用的过滤+打分+选节点**。CPU/mem/亲和性不是"再做一次"，是 HAMi 接力棒的上一棒。

## kube-scheduler 调度流水线

```
kube-scheduler 调度一个 Pod:
  1. 内置 filter 们（并行跑）:
     - NodeUnschedulable     （节点是否 cordon）
     - NodeName             （指定节点名）
     - TaintToleration      （污点容忍）
     - NodeAffinity         （节点亲和性）
     - NodeResourcesFit     （CPU/mem/存储够不够）   ← 通用资源过滤
     - PodTopologySpread    （拓扑分布）
     - ...几十个
  2. extender filter（串行，在所有内置 filter 之后）:
     - 调 HAMi /filter                          ← HAMi 接在后面
  3. 内置 score 们 + extender prioritize（HAMi 没配 prioritize）
  4. 选最优节点
  5. extender bind（HAMi /bind）
```

**extender 的 filter 排在所有内置 filter 插件之后**，HAMi 拿到的是已经被内置过滤器筛过的候选节点子集。

## HAMi 的介入条件：managedResources

HAMi 不是对所有 Pod 都过滤。chart 配置里有 `managedResources`（configmap.yaml:42-43），kube-scheduler **只在 Pod 申请了 `managedResources` 列表里的资源时才会调 HAMi**：

```yaml
managedResources:    # 只有申请这些资源才调 HAMi
  - hami.io/gpu
  - hami.io/mlu
  ...
```

代码里也对应（scheduler.go:1150）：Pod 不申请任何 HAMi 资源时，直接原样回传所有候选节点，不做任何过滤。

## HAMi 的 filter 不只是过滤（反直觉设计）

HAMi 的 filter 函数其实干了四件事，把本该 bind 做的工作前移了（scheduler.go:1180-1207）：

```go
nodeScores, err := s.calcScore(nodeUsage, resourceReqs, args.Pod, failedNodes)  // 1. 打分
sort.Sort(nodeScores)                      // 2. 按分排序
m := (*nodeScores).NodeList[len-1]         // 3. 选最高分节点
annotations[util.AssignedNodeAnnotations] = m.NodeID    // 4. 选定节点写进 Pod annotation
s.podManager.AddPod(args.Pod, m.NodeID, effectiveDevices) // 5. 内存预占设备
util.PatchPodAnnotations(args.Pod, annotations)          // 6. patch 注解到 apiserver
```

HAMi 借 filter 的时机做完了：选节点 + 写分配结果 + 预占设备。这是上一个问题"filter 和 bind 为什么不能合并"的答案——**HAMi 已经把能前移的工作都前移到 filter 了**，bind 阶段（scheduler.go:1067-1133）只是把这个既定结果落定为正式 binding（写 `Binding` 对象），不再重新选节点。

## 关于打分：HAMi 没用标准 prioritizeVerb

chart 配置里只有 `filterVerb` 和 `bindVerb`，**没有 `prioritizeVerb`**（configmap.yaml:37-38）。HAMi 把打分逻辑塞进了 filter 函数里（scheduler.go:1180 的 `calcScore`），在 filter 返回前就排好序选好节点了。

所以 kube-scheduler 标准流水线里的 score 阶段对 HAMi 是透明的——HAMi 自己选节点，不等 kube-scheduler 的 score。

**副作用**：HAMi 选的节点可能和 kube-scheduler score 阶段选的不一致。因为 HAMi 在 filter 里就把选定节点写进了 Pod annotation（`AssignedNodeAnnotations`），kube-scheduler 后续 score 再怎么打分，bind 时 HAMi 读的是 annotation 里 HAMi 自己定的节点。这是个有意的"劫持"——HAMi 要掌控 GPU 节点选择，不能让 kube-scheduler 的通用 score 覆盖。

## 职责划分对照

| 维度 | 谁负责 | 说明 |
|------|--------|------|
| CPU/mem 是否够 | kube-scheduler 内置 filter | `NodeResourcesFit` 插件 |
| 节点亲和性 | kube-scheduler 内置 filter | `NodeAffinity` 插件 |
| 污点容忍 | kube-scheduler 内置 filter | `TaintToleration` 插件 |
| 拓扑分布 | kube-scheduler 内置 filter | `PodTopologySpread` 插件 |
| GPU 显存够不够、卡能不能分 | **HAMi filter** | 申请 HAMi 资源时才介入 |
| GPU 节点间选哪个最优 | **HAMi filter 里的 calcScore** | HAMi 自己打分排序，不走标准 prioritize |
| 最终写 binding | **HAMi bind** | HAMi 接管，不交给 kube-scheduler 默认 bind |

## 一句话总结

kube-scheduler 先用内置 filter 把 CPU/mem/亲和性/taints 等通用条件全筛一遍，把通过的候选节点交给 HAMi；HAMi（仅当 Pod 申请 vGPU 资源时介入）在此基础上做 GPU 专用过滤，并在 filter 里就完成打分选节点+写分配注解+内存预占，bind 阶段只是落定结果。CPU/mem/亲和性不是"HAMi 过滤后 scheduler 再做一次"，而是 HAMi 接力棒的上一棒。

---

# `scheduler.go` 的 `Filter()`

## 解析资源请求，`device.Resourcereqs(pod)` 怎么从 Pod spec 算出"要几张 GPU、多少显存"
