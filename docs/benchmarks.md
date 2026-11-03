# Benchmark 口径

Benchmark 用于比较会话路由行为、准入效果和故障响应。当前 README 不发布历史性能提升数字：旧的流式完成判定接受 HTTP 200 和 EOF，而修正后的原生 SSE 测量要求完整事件中的 `stop: true`；历史运行也未统一固定 CPU 执行参数。

## 环境与可比条件

参考引擎为 llama.cpp `785322d17f2d51e5185cf996a1f4e46d87640841`，参考模型为 Qwen2.5-0.5B-Instruct q4_0，文件校验值见[运行手册](runbook.md)。运行时必须保存实际硬件、Python 和依赖版本；记录模型校验值、引擎版本与设备日志。

CPU 运行显式使用 `--device none --no-op-offload -ngl 0`；命令示例同时设置 `LLAMA_ARG_DEVICE=none`、`LLAMA_ARG_N_GPU_LAYERS=0`。启动参数固定线程数、context 和 parallel 配置。构建启用或禁用 Metal 的选项不等同于运行时设备证明。

路由比较中 B0 为 `round_robin`，B1 为 `session_affinity`。二者使用相同请求体、会话头和顺序；相同模型、后端参数及端口；每个策略和重复轮次重启后端和网关，使缓存与计数从相同状态开始。路由 workload 的并发为 1，`n_predict=1`、`temperature=0`、`seed=42`、`cache_prompt=true`。

- `sequential`：同一会话的多轮请求连续到达。
- `shuffled`：固定种子打乱请求次序。
- `negative`：没有共享会话前缀的对照。

## 运行

从仓库根目录执行，提前导出 `LLAMA_SERVER` 和 `GATEWAY_MODEL`。脚本自行管理子进程，不要提前启动相同端口的后端，也不要并发运行这些命令。

```bash
export LLAMA_ARG_DEVICE=none
export LLAMA_ARG_N_GPU_LAYERS=0
uv run --locked python -m bench.run_bench --repeat 5 \
  --workloads sequential,shuffled,negative --strategies b0,b1 \
  --outdir bench/results/route
uv run --locked python -m bench.run_admission_bench --repeat 5 \
  --outdir bench/results/admission
uv run --locked python -m bench.run_reliability_bench --repeat 5 \
  --outdir bench/results/reliability
uv run --locked python -m bench.run_soak --outdir bench/results/soak
```

路由脚本输出 `meta.json`、`raw.json`、`summary.json`、`overhead.json` 和日志；其他脚本按场景输出原始记录、摘要或时间序列。公开前检查元数据和日志中的本机路径，并只保留支持公开结论所需的材料。

## 成功、延迟与分母

原生 `/completion` 流须收到由空行正确分隔的 `stop: true` 事件，并无传输错误，才能计为 completed。未结束的尾部数据不作为完成事件；HTTP 200 后截断归为错误。主动客户端断开与非预期传输异常分开统计。

报告应同时列出 offered、completed、rejected、timed_out 和 errors，避免只展示成功子集。完成请求的延迟 p50/p95 是条件统计；超时与拒绝不会进入这组延迟样本。TTFT 是从请求开始到首个非空生成内容事件的时间，包含排队。

缓存比例按 token 加权：

```text
cache_hit_pct = 100 × Σcache_n / Σ(cache_n + prompt_n)
```

同时报告有效 timing 样本数、后端请求分布和冷会话数。不可把不同提示长度或冷会话比例下的缓存率直接解释成算法优劣。

`llamacpp:prompt_tokens_total` 的语义须按所用引擎版本核对，不能默认包含缓存 token。优先从原始 `timings.cache_n` 和 `timings.prompt_n` 复算；后端计数器作为对账依据。

## 可支持的结论

- 路由比较可说明特定会话结构下的缓存复用与负载分布，不能证明新算法或普遍性能优势。
- 准入比较可说明执行与排队上限、拒绝和等待时间，不预设吞吐或 p95 改善。
- 故障比较可说明健康摘除、迁移及网关容量回收，不证明推理引擎内部取消。
- Soak 记录短时运行中的容量和进程资源变化，不等同于长期稳定性认证。

当前版本的实际执行命令与结果见[发布验证记录](release-verification.md)。未执行的完整矩阵保留为复现步骤，不填写推测值。
