# 发布验证记录

2026-11-03 对本地发行候选执行以下检查。先独立复现旧缺陷，再检查修复后的行为；开发阶段的 Week 1–4 PASS 不作为本次结论依据。当前适合以受信任本机实验项目公开源码和用于实习展示，未上传 GitHub。

## 执行结果

| 检查 | 本次结果 |
|---|---|
| Python 3.11.16，无模型测试 | 158 passed，4 deselected，28.64 s |
| Python 3.12.14，无模型测试 | 158 passed，4 deselected，28.60 s |
| Python 3.13.15，无模型测试 | 158 passed，4 deselected，28.93 s |
| 独立审查场景复测 | 9 passed，2.98 s；使用修复后正确行为断言 |
| 真实 llama.cpp E2E | 4 passed，133 deselected，2.32 s |
| 最简真实演示 | 完整结束事件、缓存复用、健康摘除和迁移均观察到；自身进程清理完成 |
| 真实 Benchmark 最小回归 | sequential / b1 / 1 轮 / n_predict=1：60 请求，60 完成，0 错误 |
| 安装与构建 | 干净目录按锁文件安装；wheel、sdist 构建及包内容检查通过 |
| 依赖公告 | OSV 查询锁文件中 18 个第三方包的精确版本，未返回公告匹配 |
| 密钥检查 | Gitleaks 8.30.1 扫描全部 2 个 Git 提交及当前目录，未发现命中 |
| 隐私与包体 | 交付文件及历史检查未发现真实凭据、个人目录或真实用户请求；本地原始结果和环境文件不打包 |
| 文档与图示 | 本地链接检查通过；Mermaid 渲染为 SVG，预览检查通过 |

无模型命令：`uv run --locked pytest -q tests bench/tests -m 'not real_backend'`，包含单元、fake HTTP、raw HTTP、Benchmark 与进程管理回归。3.11 和 3.12 使用干净源码副本安装。独立审查脚本保存在本地审查材料中；对应可复用用例已收入仓库测试目录。

GitHub Actions 已配置 Linux / Python 3.11–3.13，无需模型；Actions 使用固定提交、只读仓库权限，uv 使用固定版本。上表是在 macOS 执行的本地版本矩阵，尚未触发 GitHub 托管运行，不能据此宣称远程 CI 已通过。

真实测试命令及资源准备见[运行手册](runbook.md)。本次最小回归使用：

```bash
uv run --locked python -m bench.run_bench --repeat 1 \
  --workloads sequential --strategies b1 --n-predict 1 --skip-overhead \
  --outdir bench/results/benchmark-release-20261103
```

## 已修复问题与验证范围

| 问题 | 修复与验证 |
|---|---|
| Host 影响上游路径 | 从已知路由映射上游路径；恶意 Host 不再把 `/completion` 改成其他端点 |
| 取消与请求完成竞争 | 明确响应所有权；两个竞争顺序均检查连接关闭与容量回收 |
| 非法 timings 泄漏容量 | 丢弃无效指标值，清理不受指标解析影响；后续请求仍可准入 |
| Retry 排队超出执行期限 | 重试沿用首次准入期限；从 ticket 实际授予时间计时，延迟交接也不重置预算 |
| JSON 上游读取时客户端断连 | 监听断连并取消读取，关闭响应、释放 ticket |
| JSON 下游发送无限等待 | 发送受总期限与执行期限限制；已过期且尚未发头的错误响应只有 50 ms 发送宽限 |
| 共享 Cookie jar 串会话 | 禁止保存上游 Cookie；不同客户端请求不继承 jar 中的状态 |
| SSE 截断被 Benchmark 算成功 | 必须解析到完整原生 `stop: true` 事件；HTTP 200 和 EOF 不算完成 |
| 额外资源与工具风险 | 重复取消不跳过清理；SSE 指标事件缓冲有界；停止脚本检查进程身份；soak 子任务异常导致失败 |
| 发布材料缺失 | 补齐 MIT LICENSE、第三方说明、仓库内文档、架构图和自动化测试配置 |

## 真实演示

两后端均使用同一模型、4 个 CPU 线程和 1 个 slot。先向同一会话发送两次相同提示，再停止演示启动的首选后端。

| 请求 | 后端 | cache_n | prompt_n | 完整结束事件 |
|---|---|---:|---:|---|
| 首次请求 | A | 0 | 294 | 是 |
| 相同会话与提示 | A | 293 | 1 | 是 |
| 停止 A、健康摘除后 | B | 0 | 294 | 是 |

此次从停止 A 到健康状态变为不可用观察到 0.418 s，配置为 0.25 s 探测间隔、连续失败阈值 2。这是单次功能观察，不是故障恢复延迟上界或性能承诺。

## 环境与资源身份

- 系统：macOS 27.0.1，arm64；真实运行使用 Python 3.13.15。
- llama.cpp：`785322d17f2d51e5185cf996a1f4e46d87640841`；二进制自报 `0.8.0-dev (build 1, commit 785322d)`，AppleClang 21.0.0.21000334，Darwin arm64。
- 模型：Qwen2.5-0.5B-Instruct，Q4_0 GGUF，428,730,208 字节。
- 显式 CPU 参数：`-c 4096 -n 16 -t 4 --threads-batch 4 -ngl 0 --device none --no-op-offload --parallel 1 --metrics --no-webui`。具体请求可覆盖生成长度；最小 Benchmark 使用 1 token。

| 资源 | SHA-256 |
|---|---|
| 本次 llama-server 二进制 | `017f97712c42fde41a0a01fa06a5d6c1fed88d8bb2e0e4c79e44769af619406b` |
| GGUF | `7671c0c304e6ce5a7fc577bcb12aba01e2c155cc2efd29b2213c95b18edaf6ed` |

二进制校验值标识本次本地构建，其他系统重新编译不要求字节相同。源码包不包含二进制或模型。含本机路径的原始运行输出留在忽略目录，公开文档只保留上述必要结果。

## 保留限制

完整路由、准入和可靠性五轮矩阵，以及长期运行实验，本次没有重跑。单策略 60 请求回归和单次演示不构成性能对比；README 已移除旧版提升比例。

当前范围为受信任本机、单进程和静态后端。请求体大小、内存上限、健康连接池隔离、真实引擎内部取消、恢复后立即 failback 等限制见[使用范围与限制](limitations.md)。安全扫描只反映本次版本与规则的结果；构建后端固定版本，但构建传递依赖未全部锁定，不承诺字节级可复现构建。
