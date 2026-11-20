# Project Packaging Report

日期：2026-11-03。对象：Agent-Aware Inference Gateway。本报告记录实际目录检查、
移动/复制、Git 状态、敏感信息检查、重新验收和仍需人工决定的事项。

## 1. 整理前布局

实际只读检查结果：

```
AI-develop/                    # 不是 Git 仓库（无顶层 .git）
  gateway/                     # 独立 Git 仓库，HEAD=0ad8282
    .git/
    pyproject.toml, uv.lock
    src/gateway/, tests/, bench/, scripts/, docs/
    README.md, LICENSE, .github/
    config.example.toml, config.reliability.toml
    bench/results/             # 本地运行结果，已 gitignore
    .venv/                     # 本地环境，已 gitignore
  archive/, pilot/, research/, outputs/
  MIGRATION_REPORT.md, migration_manifest.json, README.md
```

- `gateway/.git` 属于 Gateway 自身，不属于 AI-develop 工作区。
- Gateway 代码只存在于 `gateway/`，没有第二份源码副本。
- Week 1–4 报告与原始数据在 `research/phase-2/implementation/`；
  其他研究在 `research/phase-1/`、`archive/`、`pilot/`、`outputs/`。
- 整理前 Gateway 工作树有 35 项未提交变更（release-hardening 修复、文档、
  测试、CI、图片等），另有已忽略的 `bench/results`、`.venv`、日志。
- 当前环境没有可用的 Gitleaks 二进制；使用 git grep、文件扫描和归档后文本
  扫描做本次检查。工作区 `outputs/release-readiness-20261103/` 保留了前一轮
  Gitleaks 记录，但本次没有把它当作新的扫描结果。

其他项目（TripPilot、Triton Ops Lab、AI-research、PR_Contributor、cpp、
harmony-assign3、python）未移动、未修改。TripPilot/Triton 的实际路径位于
各自工作区，不在 AI-develop 内。

## 2. 整理后布局

```
AI-develop/
  agent-aware-inference-gateway/     # 新独立项目目录
    .git/                            # 原 Gateway 独立仓库，历史保留
    pyproject.toml, uv.lock, LICENSE
    src/gateway/, tests/, bench/, scripts/, docs/, .github/
    README.md, config.example.toml, config.reliability.toml
    evidence/                        # 精选最终证据（新增）
    bench/results/, bench/logs/      # 本地忽略数据，未进入归档
  agent-aware-inference-gateway-20261103.tar.gz
  PROJECT_PACKAGING_REPORT.md
  archive/, pilot/, research/, outputs/   # 未移动
```

旧 `AI-develop/gateway/` 已不存在；所有源码、测试、脚本、文档、配置和 `.git`
通过一次目录移动保留在 `agent-aware-inference-gateway/`，没有重建仓库、没有
改写历史。`gateway/` 名称只保留在历史报告中。

## 3. 移动与复制清单

**移动：**

- 整个 `gateway/` 目录 → `agent-aware-inference-gateway/`，包括 `.git`、
  `uv.lock`、`src/`、`tests/`、`bench/`、`scripts/`、`docs/`、`.github/`、
  `README.md`、`LICENSE`、配置文件，以及既有未提交工作树变更。
- 移动后删除并重建了本地 `.venv`，清除了 `.pytest_cache`、`.ruff_cache`、
  `__pycache__`、`.DS_Store`；这些均为可重建产物，不属于源码历史。
- `bench/results/`（约 12 MB）和 `bench/logs/` 留在项目目录内但被
  `.gitignore` 忽略；不进入归档。

**复制（未修改原件）：**

- `research/phase-2/implementation/week-4-*.md`、
  `week-4-test-results.json`、`project-final-review.md` → `evidence/history/week-4/`。
- Week 4 的 route/admission/reliability/soak `summary.json` 与
  `repeat-stats.json`/`timeseries.json` → `evidence/history/week-4/summaries/`。
- 仅对复制件 `soak-summary.json` 的本地绝对配置路径做了 redaction；
  性能数值未改动。
- 新的当前版本证据：`evidence/tests/pytest-no-model.*`、`pytest-real.*`、
  `uv-build.log`、`evidence/benchmarks/benchmark-smoke-*`。

**未移动/未修改：**

- `AI-develop/archive/`、`pilot/`、`research/`、`outputs/`；
- 其他独立项目；
- `MIGRATION_REPORT.md`、`migration_manifest.json`、AI-develop 根 README；
- 历史研究中的原始路径和原始记录。

## 4. Git 状态

- 新项目是独立 Git 仓库：`agent-aware-inference-gateway/.git`。
- 历史：2 个 commit，HEAD
  `0ad8282eaf1df706d607c58ee3fdda2a62c6527d`。
- 无 Git remote；没有 push，没有创建 GitHub 仓库。
- 整理前的 release-hardening 工作树变更**未被丢弃**；移动后仍在工作树中。
- 当前 `git status --short` 共 36 项：
  - 23 个已跟踪文件修改（proxy、sse、backends、README、docs、bench、
    scripts、pyproject、uv.lock、tests 等）；
  - 未跟踪：`.github/`、`LICENSE`、`bench/demo.py`、`bench/tests/`、
    `docs/assets/`、`docs/benchmarks.md`、`release-*.md`、`third-party.md`、
    `evidence/`、`scripts/manage_backends.py`、两个新测试文件。
- 本次包装**没有**提交这些变更。原因：它们是上一轮发布加固留下的工作树，
  包含独立审查修复；是否作为一次 release commit 需要项目作者确认。
- 源文件/证据中未发现 `/Users/`、`/private/`、`/tmp/` 绝对路径；被忽略的
  `bench/results`/`bench/logs` 含本机路径，但不进入仓库提交或归档。

## 5. 敏感信息与资产检查

- 未发现 `.env`、私钥、API key/token 模式或真实凭据进入可发布文件。
- 可发布源码/文档/evidence 未发现本机绝对路径。
- `bench/results`、`bench/logs` 含本机路径/进程日志，已 gitignore，且从归档排除。
- `.venv`、`__pycache__`、`.pytest_cache`、`.ruff_cache` 已在项目中清理或忽略。
- 模型 GGUF（约 409 MiB）、llama-server 二进制、构建产物未放入项目/归档。
- 归档解包后约 2.6 MB；不包含 `.venv`、bench 结果/日志、模型或二进制。
- 归档完整性：
  - `AI-develop/agent-aware-inference-gateway-20261103.tar.gz`
  - SHA-256 `5dac2f4515b8adce052cbf7d53ab69049441831a45f31ab86a1ef4f9419f9068`
  - 解包文本扫描未发现绝对路径或密钥模式；Git 历史扫描未发现命中。

## 6. 重新验收

在新项目根目录执行：

| 检查 | 命令 | 结果 |
|---|---|---|
| 干净安装 | `rm -rf .venv && uv sync --locked` | 成功，Python 3.13.15，Starlette 1.10.0，pytest 9.2.0 |
| 无模型测试 | `pytest -q tests bench/tests -m 'not real_backend'` | **158 passed, 4 deselected**, 30.38 s |
| 真实双后端 E2E | `GATEWAY_REAL_BACKENDS=1 ... pytest -q -m real_backend` | **4 passed, 133 deselected**, 2.83 s |
| Benchmark 冒烟 | sequential / B1 / 1 repeat / 60 requests | **60 completed, 0 errors**，cache 73.577%，p50 17.529 ms |
| 脚本 | `start_backends.sh` / `stop_backends.sh` | 两后端 health OK；停止后无 llama-server |
| 构建 | `uv build` | wheel + sdist 成功 |

原始命令输出在 `evidence/tests/` 与 `evidence/benchmarks/`。完整 5 次路由/准入/
可靠性矩阵和长期 soak 未在当前 release-hardening 代码上重跑；Week 4 历史结果
仅作为历史证据，不作为当前版本验收依据。

## 7. 仍需人工决定

1. 是否把当前 36 项工作树变更提交为 release commit；本次未提交、未改历史。
2. 是否将 `evidence/history/week-4/` 纳入公开仓库；它含历史数字和旧 commit
   上下文，当前验收应优先引用 `docs/release-verification.md`。
3. 是否加入 GitHub remote 并推送；本次明确未执行。
4. 是否保留本地 `bench/results`（约 12 MB，已忽略）或移到个人备份。
5. 是否接受当前 MIT LICENSE 的版权人写法，或在发布前改为个人/组织名称。
6. 真实基准完整矩阵需要在目标机器上重新运行时，应按 `docs/benchmarks.md`
   固定 CPU 参数和完整 SSE 完成判定；不得引用旧的流量提升数字。

## 8. 最终状态

- 独立项目：`AI-develop/agent-aware-inference-gateway/`
- 归档：`AI-develop/agent-aware-inference-gateway-20261103.tar.gz`
- Git：独立仓库，HEAD `0ad8282...`，无 remote，工作树有 36 项待作者确认的变更。
- 测试：158 passed / 4 deselected（无模型）；4 passed（真实 E2E）；benchmark
  冒烟 60/60；构建成功。
- 未执行：GitHub 创建/push、远程 CI、当前代码下的完整 5 轮历史矩阵。

## 9. 后续更新（2026-11-14）

包装完成后，按用户要求在 `agent-aware-inference-gateway/` 内继续开发，新增：

- `prefix_affinity` 与 `kv_cache_aware` 路由：SHA-256 字符块前缀 trie、
  prompt 提取、基于 llama.cpp `timings.cache_n` 的缓存证据；
- 可选 tokenizer 前缀：通过 llama.cpp `/tokenize` 使用模型 token id，失败时
  回退到字符块；token 缓存有界；
- trie TTL/LRU 淘汰、增量后端注册（不再整体重建 router）、缓存保持型安全重试；
- loopback-only 运行时后端注册、可选 bearer token、审计日志，以及
  `drain` / `undrain` 优雅排水；`GET /admin/cache` 只展示 `/slots` 缓存计数；
- 请求体大小上限（413）、请求 ID 中间件、`traceparent` 透传、健康探测独立
  连接池与总超时；
- `/v1/completions`、`/v1/embeddings`、`/v1/models` 转发；
- 新增 `src/gateway/prefix.py`、`src/gateway/tokenizer.py`、
  `src/gateway/middleware.py`，以及对应的单元、HTTP 和真实后端测试。

重新验收结果（Python 3.13.15，打包目录内）：

| 检查 | 结果 |
|---|---|
| 无模型测试 | **211 passed, 5 deselected**, 40.64 s |
| 真实双后端 E2E | **5 passed, 186 deselected**, 2.64 s |
| 前缀 Benchmark（5 次重复，7 个长 prompt） | b0 cache 50.229% / p50 258.66 ms；p1、kv cache 74.938% / p50 约 9.7–9.9 ms；均 0 错误 |
| 构建 | `uv build` 成功；wheel 包含 `prefix.py`、`tokenizer.py` 和 `middleware.py` |

更新后的归档：

- `dist/agent-aware-inference-gateway-20261114.tar.gz`（约 1.7 MB，262 个条目）
- SHA-256 记录在同目录的 `agent-aware-inference-gateway-20261114.tar.gz.sha256`，
  避免在归档内写入自身校验值造成循环依赖
- 排除了 `.venv`、`bench/results`、`bench/logs`、`.pytest_cache`、`.run`、
  `dist`、`__pycache__` 和 `.DS_Store`；
- 解包文本扫描只命中本报告中对扫描规则的描述，未发现真实本机路径或密钥。

2026-11-03 和 2026-11-11 的归档仍在 `dist/`，分别作为包装时和上一轮功能
更新的历史快照保留。
项目目录经用户后续整理后，AI-develop 根目录只保留两个独立项目，研究档案移入
`internship-assistant/archive/ai-develop-research/`；本报告记录的是当时的布局，
不作为当前根目录结构的描述。

Git 状态：仍为独立仓库、无 remote，`git status --porcelain` 共 57 项
（34 个已跟踪文件修改、23 个未跟踪路径），release-hardening 和新功能变更都未提交；
是否提交由项目作者决定。本报告不构成 GitHub 发布或远程 CI 已通过的声明。
