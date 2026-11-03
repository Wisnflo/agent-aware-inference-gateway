# 工程讲解提纲

展示重点是解释代码的行为、限制与验证依据。以下问题可用于准备实习面试；能运行测试不等于已经掌握实现。

## 三分钟介绍

Agent 多轮请求携带重复上下文，会话亲和可增加同一推理进程上的缓存复用机会，同时可能集中负载。网关在两个本地 llama.cpp 后端前提供路由和有界准入，处理排队、SSE、断连与期限，并用 Prometheus 指标和可控故障测试观察请求生命周期。

设计参考 vLLM Router，路由方法属于成熟方案。工程工作主要是资源所有权、异常路径和可复现实验；不声称新算法、生产级安全或 GPU 性能优势。

## 用源码回答

| 问题 | 代码与验证入口 |
|---|---|
| 为什么使用 SHA-256 rendezvous hashing？后端增减时哪些会话迁移？ | `router.py`，`tests/unit/test_router.py` |
| 先路由再排队有什么代价？为什么 FIFO 按后端保持？ | `admission.py`，`tests/unit/test_admission.py` |
| 全局和后端上限如何共同约束？释放为什么幂等？ | `AdmissionController`、`AdmissionTicket`，admission HTTP 测试 |
| `cancel()` 与 send task 已完成同时发生时，谁关闭 Response？ | `proxy.py` 的任务回收与 cleanup，取消交错回归测试 |
| JSON 正文读取时客户端断开如何发现？下游发送还受期限约束吗？ | `_read_with_disconnect`、`_DeadlineResponse` |
| 为什么重试排队仍受首次执行期限约束？ | `_execute`、`_acquire`，retry deadline 回归测试 |
| 为什么只对连接失败重试？头部后切换后端会怎样？ | `_send_with_disconnect`、可靠性 HTTP 测试 |
| HTTP chunk 和 SSE event 有什么区别？超大事件如何影响指标？ | `sse.py`，分帧和事件上限单元测试 |
| 为什么票据释放不能依赖 timing 指标和 `aclose()` 成功？ | 异常 timing、重复取消和响应关闭回归测试 |
| 健康探测为什么去抖？恢复后为什么立即 failback？ | `backends.py`、`router.py`、health 测试 |
| 请求计数和尝试计数有什么区别？200 SSE 是否成功？ | `metrics.py`、[指标说明](metrics.md) |
| 如何证明CPU执行与基准可比？如何处理拒绝和截断流？ | `bench/`、[Benchmark 说明](benchmarks.md) |

## 最小演示顺序

1. 打开[架构图](assets/architecture.svg)，说明数据路径、健康反馈和容量归属。
2. 运行 `uv run --locked python -m bench.demo --outdir bench/results/demo`，展示同会话两次请求的后端和缓存统计。
3. 解释首选后端停止后的健康迁移，以及 session affinity 恢复时立即 failback 的代价。
4. 展示一项可控回归测试，说明正确行为断言；不要用“复现了缺陷”的 PASS 充当修复成功。
5. 打开[发布验证记录](release-verification.md)，区分已运行结果、未运行矩阵和已知限制。

## 推荐练习

选择一个退出路径，在纸上列出 task、Response 和 admission ticket 的所有者，再用测试验证其结束状态。优先检查断连与响应完成同一轮发生、重试等待耗尽期限，以及响应关闭过程中再次取消。

讲解 Benchmark 时重新从 raw records 计算缓存率，并同时给出 offered、completed 和错误数量。若没有重跑对应条件，直接说明该结果尚未测量，不用历史报告补数。
