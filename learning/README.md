# HAMi 学习文档

> 本目录是针对 HAMi（Heterogeneous AI Computing Virtualization Middleware）项目的系统性学习笔记。
> HAMi 是一个 CNCF 孵化项目，为 Kubernetes 提供异构 AI 加速器（GPU/NPU/HCU 等）的设备虚拟化能力。

## 文档目录

| 文档 | 内容 | 建议阅读顺序 |
|------|------|-------------|
| [01-架构图.md](./01-架构图.md) | 整体架构图、部署形态、目录结构 | ① 先读，建立全局认知 |
| [02-核心调用链路.md](./02-核心调用链路.md) | Pod 从提交到设备分配的完整调用链路（时序图） | ② 深入理解核心流程 |
| [03-组件功能及用途.md](./03-组件功能及用途.md) | 各组件的功能、职责和用途 | ③ 逐一理解每个组件 |
| [04-组件调用关系.md](./04-组件调用关系.md) | 组件之间的调用关系、接口边界、数据流 | ④ 理解组件如何协作 |
| [05-学习路线.md](./05-学习路线.md) | 从哪里入手学习 HAMi | ⑤ 规划自己的学习路径 |
| [06-贡献代码入门指南.md](./06-贡献代码入门指南.md) | 首次贡献开源项目的上手指南 | ⑥ 准备动手贡献 |

## 图形说明

本文档中的图形使用 **Mermaid** 语法编写。Mermaid 的优势：

- GitHub 原生渲染（无需插件）
- VS Code 安装 "Markdown Preview Mermaid Support" 插件即可预览
- 语法简洁，便于版本控制和后续修改

如果你更喜欢 PlantUML，可以将 Mermaid 图粘贴到 [mermaid.live](https://mermaid.live) 编辑器中查看，或使用工具转换为 PlantUML 语法。

## 快速概览

HAMi 由三个二进制组件构成：

1. **Scheduler Extender**（`cmd/scheduler/`）— 扩展 kube-scheduler 的 HTTP 服务，负责设备感知的 filter/score/bind
2. **Device Plugin**（`cmd/device-plugin/nvidia/`）— Kubernetes 设备插件，向 kubelet 注册 vGPU 资源
3. **vGPU Monitor**（`cmd/vGPUmonitor/`）— 每节点监控守护进程，采集 GPU 使用指标并执行资源隔离

核心数据流：

```
Pod 提交 → MutatingWebhook(注入调度器名) → Scheduler filter/score/bind(写入设备分配注解)
→ Device Plugin Allocate(读取注解，注入环境变量和挂载) → vGPU Monitor(监控和限制)
```

详见各文档。
