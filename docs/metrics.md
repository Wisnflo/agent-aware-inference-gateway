# 指标说明

`GET /metrics` 输出 Prometheus 文本。指标来自单个网关进程；容量 gauge 从 admission controller 的计数同步，避免在代理的不同分支分别加减。

标签使用固定路由、配置后端和有限结果类型。请求正文、session ID、客户端地址与任意 header 值不作为标签；日志中的会话标识为 SHA-256 的短指纹，仍可用于关联请求。

## 主要指标

| 指标 | 含义与分母 |
|---|---|
| `gateway_requests_total{route,status,backend}` | 一次客户端请求的最终 HTTP 结果；重试成功仍只产生一个请求结果 |
| `gateway_active_requests`、`gateway_in_flight{backend}` | 正在持有执行票据的全局和后端请求数 |
| `gateway_queue_depth`、`gateway_queue_depth_backend{backend}` | 全局和后端等待请求数 |
| `gateway_admission_peak_in_flight`、`gateway_admission_peak_queue_depth` | 进程启动以来的容量峰值 |
| `gateway_admission_admitted_total{backend}` | 获得票据的尝试数，包含重试的新票据 |
| `gateway_admission_rejected_total{backend}` | 因队列已满而拒绝的次数 |
| `gateway_admission_timed_out_total{backend}` | 等待达到 queue timeout 的次数 |
| `gateway_admission_cancelled_total{backend}` | 排队时取消或断连的次数 |
| `gateway_upstream_errors_total{backend,kind}` | 失败的上游尝试数；一次客户端请求可能先失败再重试成功 |
| `gateway_retries_total{backend,reason}` | 连接失败后启动替代尝试的次数 |
| `gateway_retry_results_total{from_backend,to_backend,result}` | 替代尝试的结果 |
| `gateway_execution_timeouts_total{backend,kind}` | 达到 request 或 execution deadline 的次数 |
| `gateway_client_cancelled_total{route,stage}` | 客户端取消，阶段含 queued、connecting、body、streaming |
| `gateway_upstream_disconnects_total{backend,stage}` | connect、body 或 stream 阶段的上游网络或协议异常 |
| `gateway_client_disconnects_total{route}` | 流式响应的客户端断连，属于上一项客户端取消指标的流式子集 |
| `gateway_stream_terminations_total{backend,reason}` | SSE 以 completed、client_disconnect、upstream_error 或 deadline 结束 |
| `gateway_health_checks_total{backend,result}` | 健康探测的成功和失败次数 |
| `gateway_backend_health{backend}` | 经启动检查或运行时去抖后的健康状态，1 为可路由，0 为已摘除 |
| `gateway_backend_removed_total{backend}`、`gateway_backend_restored_total{backend}` | 运行时健康摘除和恢复次数，不含启动转换 |
| `gateway_routing_failovers_total{from_backend,to_backend}` | 路由时因首选不健康而选择备用后端的次数 |

`gateway_requests_total` 与 admission、retry、upstream error 的分母不同，不能直接相加计算总请求数。`/health` 和 `/metrics` 本身不计为推理请求。

## 时间分布

| Histogram | 起止点 |
|---|---|
| `gateway_request_duration_seconds{route,backend}` | 处理请求开始至响应结束；JSON 包括下游发送，SSE 包括流式发送及清理 |
| `gateway_upstream_duration_seconds{route,backend}` | JSON 从发送上游到正文读完；SSE 在流式结束时记录，包含交错进行的下游转发及清理时间 |
| `gateway_ttft_seconds{backend}` | 请求开始到首个非空生成内容事件被网关观察到，包含排队 |
| `gateway_queue_wait_seconds{backend}` | 从票据到达到准入，仅对获准尝试记录 |
| `gateway_backend_execution_seconds{route,backend}` | 从准入到票据释放；JSON 在读完正文后释放，SSE 在流结束时释放 |

`queue_wait_seconds` 的起点位于请求体读取和路由之后；它不是客户端观察到的全部等待时间。JSON 下游发送阶段仍受固定期限约束，但不再占用后端执行名额，因此 total duration 不总是 queue + backend execution。

`request_duration - upstream_duration` 只能作为附加开销的近似值。SSE 的读写交错，两个分布包含重叠阶段；这组差值不是路由函数耗时或纯代理 CPU 开销。路由 Benchmark 另提供客户端直接请求后端与经过网关的比较，也须报告对应环境和样本数。

## SSE 与协议完成

`gateway_stream_terminations_total{reason="completed"}` 表示网关正常读到上游 EOF，没有记录到断连、期限或异常。它不验证生成协议的结束事件，因此不能单独用来计算成功生成数。

原生 llama.cpp Benchmark 要求完整 `stop: true` 事件；OpenAI 兼容流的 `[DONE]` 由上游决定。网关不为异常结束补写完成标记。`gateway_sse_events_total` 记录指标解析器观察到的事件，无法替代客户端完成检查。

SSE 指标解析每个事件最多保留 64 KiB。超限事件会跳过到下一分隔符，以限制解析内存；原始字节继续转发，因此超大事件中的内容或 token timing 可能不进入指标。

## 缓存统计

| 指标 | 来源 |
|---|---|
| `gateway_prompt_tokens_cached_total{backend}` | 有效 `timings.cache_n` 累加 |
| `gateway_prompt_tokens_fresh_total{backend}` | 有效 `timings.prompt_n` 累加 |
| `gateway_prompt_tokens_total{backend}` | 两者之和 |

非整数和负数不计入 token 指标。统计异常被隔离，不改变响应控制流程；极端数值不应作为有效测量依据。OpenAI 兼容 `usage` 结构不等同于原生 llama.cpp timing；缺少 timing 时，这组计数不能代表全部请求的 token 数。

```text
cache_hit_pct = 100 × Σcache_n / Σ(cache_n + prompt_n)
```

不要用缓存 token 除以后端的 fresh-prefill counter。引擎版本改变后，应从原始事件与后端快照重新核对计数语义。比较不同 workload 时还需报告提示长度、冷会话数与后端分布。

## 状态码与失败类型

| 情况 | HTTP 状态 | 主要观测 |
|---|---|---|
| 队列已满 | 429 `queue_full` | admission rejected |
| 排队超时 | 429 `queue_timeout` | admission timed out |
| 请求期限或执行期限耗尽，头部未提交 | 504 `request_deadline` 或 `execution_timeout` | execution timeouts |
| 无健康后端 | 503 `no_healthy_backend` | request result、backend health |
| 可重试连接失败耗尽 | 502 `upstream_connect_error` | upstream errors、retry results |
| 客户端已离开 | 内部记 499 或已提交状态 | client cancelled；499 不发给已离开的客户端 |
| SSE 头部提交后出错或超时 | 保持已提交状态，常为 200 | stream termination、完整结束事件是否存在 |

429 携带 `Retry-After: 1` 和 `error.retry_after_ms`。Admitted 响应携带 `X-Gateway-Queue-Ms`；这些响应字段不作为指标标签。
