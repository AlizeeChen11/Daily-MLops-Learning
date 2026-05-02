# 10000+ H100/H200 GPU 调度系统设计

## 背景问题

如果拥有 10000+ 张 H100/H200 GPU，同时业务包含 training、inference、batch inference、evaluation、interactive notebook 等不同形态，应该如何构建一个调度系统，才能高效调用这些 GPU 资源？

这个规模下，问题已经不只是“如何把 GPU 放进 Kubernetes”或者“如何让 Ray 自动扩容”。核心挑战是把大规模 GPU 集群建设成一个可治理、可调度、可观测、可复用的 AI 资源平台。

## 总体结论

10000+ GPU 应该按“AI 资源操作系统”的思路设计，而不是只把它看成一个 Kubernetes 集群。

整体上需要分层：

```text
业务入口层
  Training / Inference / Evaluation / Batch / Interactive Notebook

平台调度层
  Queue / Quota / Priority / Preemption / SLA / Cost Policy

AI 作业编排层
  Ray / PyTorchJob / MPIJob / Slurm Job / Kubernetes Job / RayService

集群调度层
  Kubernetes + Kueue / Volcano 或 Slurm
  Gang Scheduling / Topology-aware Scheduling / Quota Enforcement

资源抽象层
  GPU Pool / Node Pool / IB Domain / NVLink Domain / Storage Domain

基础设施层
  H100/H200 Nodes / IB or RoCE / NVMe / Object Storage / Monitoring
```

全局调度系统负责公平性、队列、配额、拓扑和 SLA；Ray、PyTorch、MPI 等框架负责作业内的分布式执行；推理平台负责在线服务的弹性、稳定性和灰度发布。

## 核心原则

不要让所有业务直接抢 GPU。应该先把 GPU 按用途和物理能力切成资源池，再通过统一的作业入口进行调度。

推荐至少拆分这些资源池：

```text
training-large-pool      -> 大规模训练，8/16/32/64/128+ GPU gang scheduling
training-small-pool      -> 小训练、fine-tuning、实验
inference-online-pool    -> 在线推理，低延迟，高可用，禁止被训练抢占
inference-batch-pool     -> 离线推理、embedding、评测
dev-interactive-pool     -> notebook、debug、临时实验
system-reserved-pool     -> 平台服务、monitoring、control plane
```

每个资源池需要独立定义：

- GPU 类型：H100、H200
- 节点形态：单机 8 GPU、NVLink/NVSwitch、IB/RoCE
- 队列策略
- 团队和项目配额
- 优先级
- 抢占规则
- 最大作业规模
- 空闲回收策略
- 是否允许混部

业务不应该直接声明“我要 GPU”，而应该声明更完整的资源意图：

```text
我要 64 张 H100，要求 IB，同一 placement group，训练队列，优先级 P2。
```

或者：

```text
我要 20 个 H200 inference replica，每个 replica 1 GPU，延迟 SLA 50ms。
```

## Training 和 Inference 的调度差异

Training 和 Inference 的资源特征不同，不应该混用同一套简单策略。

Training 的特点：

- 需要 gang scheduling，一组 GPU 必须同时拿到。
- 对拓扑敏感，例如同机 NVLink/NVSwitch、跨机 IB。
- 作业时间长。
- 可以排队。
- 可以通过 checkpoint 支持抢占和恢复。
- 容易造成 GPU 碎片。

Inference 的特点：

- 更关注延迟和稳定性。
- 需要 replica autoscaling。
- 需要灰度、滚动升级、快速回滚。
- 不适合被大训练任务挤掉。
- 大模型推理可能需要 tensor parallel / pipeline parallel，也会关心拓扑。

因此建议采用两套策略：

```text
Training Scheduler:
  Queue + Quota + Gang Scheduling + Topology-aware + Preemption + Checkpoint

Inference Scheduler:
  Service Autoscaling + Reserved Capacity + Model-aware Placement + SLA Protection
```

## 底层调度选型

如果平台以 Kubernetes/AKS 为主，推荐组合：

```text
Kubernetes + Kueue / Volcano + KubeRay + NVIDIA GPU Operator
```

关键组件职责：

- Kubernetes：统一容器和资源平台。
- Kueue：队列、quota、作业准入控制，适合 batch 和 training。
- Volcano：gang scheduling、AI/HPC 作业调度能力强。
- KubeRay：管理 RayCluster、RayJob、RayService。
- NVIDIA GPU Operator：管理 GPU driver、device plugin、DCGM exporter、MIG 等能力。
- Node Feature Discovery：自动标记 GPU、IB、CPU、NUMA、拓扑能力。
- RDMA device plugin：在 Kubernetes 中暴露 IB/RDMA 设备资源。
- Cluster Autoscaler / Node Auto Provisioning：云上环境中进行节点层扩缩容。

如果环境更偏 HPC 或裸金属大规模训练，Slurm 仍然是强选项：

```text
大规模训练：Slurm 或 Volcano/Kueue
在线推理：Kubernetes
Ray：作业内分布式执行框架
```

很多超大规模平台会采用混合模式：

```text
大训练使用 Slurm 或 Kubernetes batch scheduler。
在线推理使用 Kubernetes。
Ray 用于 RL、分布式 Python、复杂 DAG、Ray Train、Ray Serve 等场景。
```

## 拓扑感知是关键

10000+ H100/H200 的最大问题通常不是“有没有 GPU”，而是“拿到的 GPU 是否在正确的拓扑里”。

资源需要被建模成类似结构：

```text
cluster
  rack
    leaf switch
      ib fabric domain
        node
          gpu
```

调度系统需要支持：

- 同机 8 GPU 优先，利用 NVLink/NVSwitch。
- 多机训练尽量放在同一个 IB island、rack 或 fabric domain。
- 避免跨慢链路拼接 64/128/256 GPU 训练任务。
- 避免把一个完整 8-GPU 节点切得太碎。
- 对大训练任务使用 compact placement。
- 对小任务使用 bin packing。
- 对在线推理使用 spread placement，提高可用性。

可以用一句话概括：

```text
training prefers compact placement
inference prefers spread placement
```

## 统一资源声明和作业入口

业务方不建议直接写底层 Kubernetes YAML、RayCluster YAML 或 Slurm 脚本。平台应该提供统一的 Job API，例如：

```yaml
kind: AIJob
metadata:
  name: llama-pretrain
spec:
  type: training
  framework: ray
  priority: p2
  queue: training-large
  resources:
    gpuType: H100
    gpuCount: 128
    gpuPerNode: 8
    requireIB: true
    topology: compact
  checkpoint:
    enabled: true
    path: s3://checkpoints/llama-pretrain
  image: registry/train:latest
  command: python train.py
```

平台控制面再把这个高层声明翻译成具体执行后端：

- RayJob
- RayCluster + ray job submit
- PyTorchJob
- MPIJob
- VolcanoJob
- Kueue Workload
- Kubernetes Job
- Slurm Job

这样可以把业务需求和底层调度细节解耦，也方便统一做 quota、priority、审计、计费、监控和故障处理。

## 队列和配额模型

调度系统至少需要支持这些维度：

```text
team quota
project quota
business priority
job priority
gpu type quota
pool quota
burst quota
preemptible quota
reserved quota
```

示例：

```yaml
core-training-team:
  guaranteed: 2048 H100
  maxBurst: 4096 H100

inference-platform:
  guaranteed: 1024 H200
  preemptible: false

research:
  guaranteed: 512 H100
  maxJobSize: 64 GPUs
  preemptible: true
```

需要区分不同资源语义：

- guaranteed quota：保证资源。
- borrowed quota：空闲时可借用资源。
- preemptible quota：可被高优任务抢占的资源。
- reserved capacity：在线服务专用资源，默认不借给训练。

## Training 调度策略

Training 侧建议实现：

- Gang scheduling：必须一次性拿齐所需 GPU，否则不启动。
- Checkpoint-aware preemption：抢占前通知作业保存 checkpoint。
- Large job reservation：大作业可以预约未来资源窗口。
- Fair sharing：多团队公平共享资源。
- Backfilling：小作业利用大作业等待期间的碎片资源。
- Topology-aware placement：按 NVLink、IB、rack、fabric domain 放置。
- Failure recovery：节点或 GPU 故障后自动重试、缩容或恢复。

## Inference 调度策略

Inference 侧建议实现：

- Reserved GPU pool：在线推理资源池不被训练抢占。
- Autoscaling：基于 QPS、latency、queue depth、GPU utilization、tokens/sec 扩缩容。
- Model-aware placement：按模型大小、并行策略、KV cache 需求选择节点。
- Rolling update：滚动升级。
- Canary deployment：灰度发布。
- Warm pool：预热容量，降低冷启动延迟。
- Fast rollback：推理服务异常时快速回滚。
- Spread placement：副本跨 rack、zone、故障域分布。

LLM serving 可根据场景选择：

- vLLM：LLM serving、高吞吐。
- TensorRT-LLM：极致推理性能。
- Triton：多模型、多后端统一 serving。
- Ray Serve：Python 生态、复杂 DAG、在线服务编排。
- KServe：Kubernetes 原生 inference 管理。

## 混部策略

训练、推理、batch、notebook 可以共享大集群，但不应该无规则混部。

推荐原则：

- 在线 inference 不被 training 抢占。
- 低优 batch 可以使用 inference 空闲资源，但必须可快速驱逐。
- 小实验优先使用碎片资源。
- 大训练优先使用完整 8-GPU 节点。
- 交互式 notebook 设置较小 quota 和 idle timeout。
- 抢占必须和 checkpoint、重试、通知机制绑定。

## Ray 在系统中的定位

Ray 不建议作为唯一的全局 GPU 调度器。Ray 更适合作业内的分布式执行框架。

推荐定位：

```text
全局资源调度：Kubernetes + Kueue/Volcano/Slurm
作业内调度：Ray
在线推理：Ray Serve / Triton / vLLM / custom serving
```

示例流程：

```text
平台调度器分配 128 张 H100 给一个 RayCluster。
Ray 在这 128 张 GPU 内部调度 actor、task、training worker。
```

不建议让大量 RayCluster 无约束地抢全局 GPU，否则容易造成：

- 资源碎片。
- 队列不可控。
- GPU 分配了但空转。
- 抢占和恢复逻辑混乱。
- 多团队公平性难以保证。

## 推理平台控制面

如果 inference 包含 LLM serving，建议单独建设 serving control plane：

```text
Model Registry
Model Deployment API
Autoscaler
Traffic Router
GPU Scheduler
KV Cache / Prefix Cache Policy
Canary / Rollback
Metrics / Billing
```

推理平台应该关注：

- 模型版本管理。
- 模型加载和预热。
- 多副本流量分配。
- GPU 利用率和延迟之间的平衡。
- token 级吞吐和成本。
- 请求排队和限流。
- 灰度发布和快速回滚。

## 可观测性和计费

大规模 GPU 平台必须做到 GPU 级可观测。建议至少采集：

```text
GPU utilization
GPU memory usage
SM occupancy
HBM bandwidth
NVLink bandwidth
IB bandwidth
PCIe throughput
ECC errors
XID errors
temperature / power
job queue wait time
GPU allocated but idle time
GPU fragmentation
checkpoint frequency
preemption count
inference latency P50/P95/P99
tokens/sec
cost per training step
cost per million tokens
```

推荐工具组合：

```text
DCGM Exporter + Prometheus + Grafana
Kubernetes events
Ray metrics
Kueue / Volcano metrics
NVIDIA Fabric Manager metrics
custom accounting pipeline
```

这些指标不只是用于排障，也应该用于：

- 团队资源账单。
- GPU 空闲治理。
- 容量规划。
- 训练效率优化。
- 推理成本优化。
- 坏卡和坏节点自动隔离。

## 推荐落地架构

可以把平台落成以下模块：

```text
1. 资源层
   H100/H200 node pools，按 GPU 类型、IB 拓扑、业务用途打 label/taint。

2. 集群层
   Kubernetes/AKS 或裸金属 Kubernetes，安装 GPU Operator、RDMA device plugin、NFD、DCGM。

3. 队列层
   Kueue/Volcano 管 training/batch 队列，支持 quota、priority、gang scheduling。

4. 作业层
   RayJob、PyTorchJob、MPIJob、Kubernetes Job、RayService 作为执行后端。

5. 平台 API 层
   业务只提交 AIJob/TrainingJob/InferenceDeployment，不直接接触复杂 YAML。

6. 策略层
   quota、priority、preemption、topology、SLA、cost policy。

7. 可观测层
   GPU 利用率、队列等待、作业成本、推理延迟、拓扑拥塞、故障自动隔离。

8. 自动化层
   checkpoint 抢占、失败重试、坏卡隔离、节点 drain、容量预测、空闲资源回收。
```

## 分阶段建设路线

### 阶段 1：资源标准化

- 统一 GPU node pool 命名、label、taint。
- 标记 H100/H200、IB、NVLink/NVSwitch、rack、fabric domain。
- 安装 GPU Operator、DCGM Exporter、NFD、RDMA device plugin。
- 建立最小可用的 GPU inventory 和监控面板。

### 阶段 2：队列和配额

- 引入 Kueue 或 Volcano。
- 建立 training-large、training-small、inference-online、batch、dev 等队列。
- 实现 team/project quota。
- 支持 priority、gang scheduling 和 basic preemption。

### 阶段 3：统一作业入口

- 定义 AIJob / TrainingJob / InferenceDeployment API。
- 将高层资源声明翻译为 RayJob、PyTorchJob、MPIJob、RayService 等执行后端。
- 接入镜像、命令、环境变量、checkpoint、日志、指标。

### 阶段 4：拓扑感知调度

- 引入 rack、IB fabric、node、GPU 拓扑信息。
- 对大训练做 compact placement。
- 对推理副本做 spread placement。
- 减少跨慢链路训练和 8-GPU 节点碎片。

### 阶段 5：SLA 和成本治理

- 在线推理资源池保留容量。
- batch 使用可抢占资源。
- 建立 GPU 利用率、排队时间、tokens/sec、cost per step、cost per million tokens 的成本模型。
- 自动发现低利用作业、空闲 GPU、异常节点和坏卡。

## 最终目标

最终平台应该做到：

- 用户通过统一 API 提交 training/inference/batch 作业。
- 平台自动选择合适的 GPU 类型、资源池、拓扑和执行后端。
- 大训练任务拿到拓扑正确的 GPU 组。
- 在线推理服务有稳定的 reserved capacity 和 SLA。
- 空闲资源可以被低优任务借用，并在需要时安全抢占。
- 所有 GPU 的利用率、成本、故障和队列状态都可观测。
- Ray、PyTorch、MPI、vLLM、Triton 等框架都能接入同一套资源治理体系。

一句话总结：10000+ H100/H200 GPU 平台的核心不是单点 autoscaling，而是统一资源抽象、队列配额、拓扑感知、SLA 隔离、作业编排和 GPU 级可观测能力的组合。