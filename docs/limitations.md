# 使用范围与限制

适用范围是受信任的本机实验：一个 Python 网关进程，静态配置的 llama.cpp 后端，以及由使用者控制的请求和模型。默认监听 `127.0.0.1`。开放到不可信网络需要单独设计访问控制和资源保护。

## 路由与后端

- `prefix_affinity` / `kv_cache_aware` 默认使用字符块 SHA-256 前缀；配置 `tokenizer_url` 后使用 llama.cpp `/tokenize` 的 token id。两者都不是 KV block 查询，token 化也不能证明后端缓存仍然存在。
- 前缀 trie 记录经过本网关的请求；有 TTL 和 LRU 上限，默认 `ttl_seconds=900`、`max_nodes=100000`。其他客户端直接写后端形成的缓存不可见。
- `kv_cache_aware` 的 `verified` 只来自响应 `timings.cache_n > 0`；一次 `cache_n == 0` 会移除该 prompt 路径的验证标记，不代表后端其他前缀失效。llama.cpp `/slots` 只暴露缓存 token 数，不暴露前缀身份；`/admin/cache` 仅展示这些计数，不参与路由。
- 没有 prefix-aware 的 LMCache/KV controller、disaggregated prefill/decode 或跨进程缓存索引。
- 会话亲和可能造成后端负载不均；前缀亲和可能把同一前缀集中到一个后端。恢复后立即 failback，可能再次丢失缓存并形成集中请求。等待队列中的请求不会因健康摘除主动迁移。
- 运行时 `/admin/backends` 只有 loopback 限制和可选 bearer token；添加/删除会增量更新 router，保留其余后端的前缀状态。有 active/queued 请求的后端不能直接删除；`/drain` 会等待存量完成，超时后保持 draining。最后一个后端不能删除。drain 不迁移已在队列中的请求，只让它们继续完成。
- 没有完整熔断器或 Kubernetes 服务发现。后端应使用相同模型与兼容协议。OpenAI 兼容端点是转发接口，不覆盖整个 OpenAI API（已提供 completions、chat completions、embeddings 和 models）；`/v1/embeddings` 是否可用取决于 llama.cpp 的启动参数和版本，网关不会替后端做能力探测。

## 取消、期限与资源

- 请求体按 `max_request_body_bytes` 流式读取，超限返回 413；非流式响应仍完整读入内存。容量限制控制执行请求数，不限制所有未准入连接的内存。
- 正常 JSON 和 SSE 的下游发送受请求与执行期限约束。响应开始前已超时时，504 错误发送最多给 50 ms 宽限；事件循环调度、操作系统发送缓冲和响应清理也可能产生退出延迟。头部发出后只能终止传输，不能把 200 改成 504。
- 网关的结束状态与连接清理可由假后端测试确定；尚未证明真实推理引擎所有内部 KV 或 slot 操作均被取消。
- 健康探测使用独立 HTTPX 客户端和 `health.timeout_s` 总超时，但探测仍与推理共享同一上游服务和网络；探测成功不代表模型 slot 或缓存状态正常。

## 指标与成功定义

- 容量和 Prometheus 指标均为单进程数据，多 worker 没有共享容量或指标聚合。
- `gateway_requests_total` 表示 HTTP 结果；已发出 200 的 SSE 随后出错，HTTP 状态仍为 200，须同时查看流式终止原因和客户端完整事件。
- 流式 `completed` 指标表示网关正常读到上游 EOF，不等同于协议完成；Benchmark 额外要求完整的原生 `stop: true` 事件，详见[指标说明](metrics.md)。
- 不使用 session ID、提示词或客户端地址作为指标标签。短会话指纹仍可关联重复会话，不应把它当作匿名化数据。

## 安全与运维

没有推理 API 鉴权、TLS 终止、多租户隔离、计费或用户配额。Admin 端点默认仅 loopback；配置 `admin.token` / `GATEWAY_ADMIN_TOKEN` 后增加 bearer token 校验，但这不是面向不可信网络的完整访问控制。日志不记录提示正文，但会记录 request id、路由和状态；基准输出仍需在公开前检查环境信息和路径。脚本只应管理自身启动的服务；端口被占用时不要终止未知进程。

上游应按本地推理服务使用，不把此项目当作通用 Web 反向代理。共享 HTTPX 客户端拒绝保存上游 Set-Cookie，显式下游 Cookie 仍按请求透传；这不构成用户会话隔离方案。第三方软件和模型按各自许可证使用，见[第三方说明](third-party.md)。

## 性能证据

历史 Week 1–4 数字未按修正后的 SSE 完成条件及显式 CPU 参数重跑，不作为当前版本性能结论。单次演示和真实 E2E 仅说明该配置下的功能行为。

正式比较应保留硬件、模型、设备日志和所有结果分母。完成请求的 p50/p95 排除了拒绝、超时和失败，不能单独用于声称总体用户体验改善；0.5B CPU 结果不能外推到 GPU 集群或更大模型。
