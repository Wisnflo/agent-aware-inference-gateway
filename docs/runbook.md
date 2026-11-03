# 运行手册

命令默认从网关仓库根目录执行。示例路径由环境变量指定，模型和推理引擎放在仓库之外。

## 准备引擎与模型

需要 Python ≥3.11、uv 和 CMake。参考引擎提交为 `785322d17f2d51e5185cf996a1f4e46d87640841`。在准备存放外部资源的目录中构建：

```bash
export GATEWAY_WORKSPACE="$HOME/gateway-assets"
mkdir -p "$GATEWAY_WORKSPACE"
git clone --filter=blob:none https://github.com/ggml-org/llama.cpp.git \
  "$GATEWAY_WORKSPACE/llama.cpp"
git -C "$GATEWAY_WORKSPACE/llama.cpp" checkout 785322d17f2d51e5185cf996a1f4e46d87640841
cmake -S "$GATEWAY_WORKSPACE/llama.cpp" \
  -B "$GATEWAY_WORKSPACE/llama.cpp/build" \
  -DLLAMA_BUILD_SERVER=ON -DGGML_METAL=OFF -DGGML_CUDA=OFF \
  -DCMAKE_BUILD_TYPE=Release
cmake --build "$GATEWAY_WORKSPACE/llama.cpp/build" --target llama-server -j 4
```

参考模型为 `Qwen/Qwen2.5-0.5B-Instruct-GGUF` 的 `q4_0` 文件，428,730,208 bytes，SHA-256 为 `7671c0c304e6ce5a7fc577bcb12aba01e2c155cc2efd29b2213c95b18edaf6ed`。下载入口使用可变的 `master`，因此以固定校验值校验内容，不接受静默替换。

下面在子 shell 中开启 `set -e`，下载或校验失败即停止该步骤：

```bash
(
  set -eu
  mkdir -p "$GATEWAY_WORKSPACE/models"
  cd "$GATEWAY_WORKSPACE/models"
  curl -L --fail \
    -o qwen2.5-0.5b-instruct-q4_0.gguf \
    'https://modelscope.cn/api/v1/models/Qwen/Qwen2.5-0.5B-Instruct-GGUF/repo?Revision=master&FilePath=qwen2.5-0.5b-instruct-q4_0.gguf'
  printf '%s  %s\n' \
    '7671c0c304e6ce5a7fc577bcb12aba01e2c155cc2efd29b2213c95b18edaf6ed' \
    'qwen2.5-0.5b-instruct-q4_0.gguf' | shasum -a 256 -c -
)
```

确认输出为 `qwen2.5-0.5b-instruct-q4_0.gguf: OK` 后再运行后续命令。其他模型可使用自己的文件路径和可信校验值，但其性能与协议行为需要单独记录。

## 安装网关

回到网关仓库根目录：

```bash
uv sync --locked
cp config.example.toml config.toml
export LLAMA_SERVER="$GATEWAY_WORKSPACE/llama.cpp/build/bin/llama-server"
export GATEWAY_MODEL="$GATEWAY_WORKSPACE/models/qwen2.5-0.5b-instruct-q4_0.gguf"
export LLAMA_ARG_DEVICE=none
export LLAMA_ARG_N_GPU_LAYERS=0
```

`uv.lock` 固定 Python 依赖版本；构建工具由 `pyproject.toml` 固定。启动脚本和演示已通过 CLI 固定 `--device none --no-op-offload -ngl 0`；环境变量同时传给直接启动后端的真实测试。运行后仍应查看设备初始化日志，不能由 CMake 选项推断实际运行设备。

## 最简自动演示

```bash
uv run --locked python -m bench.demo --outdir bench/results/demo
```

演示展示同会话路由、第二次请求的缓存统计，以及后端故障后的健康迁移。脚本自行启动和清理服务，不要先运行下面的手动启动命令。输出目录含 `summary.json`、`config.toml` 和 `logs/`，用于检查这次演示的实际行为。

## 手动启动与请求

```bash
./scripts/start_backends.sh
uv run --locked python -m gateway --config config.toml --strategy session_affinity
```

网关在前台运行，使用 Ctrl-C 停止。在另一个终端发送：

```bash
# 原生非流式
curl --fail-with-body -i http://127.0.0.1:18080/completion \
  -H 'Content-Type: application/json' -H 'X-Session-ID: demo-session' \
  -d '{"prompt":"Say hello.","n_predict":8,"stream":false,"cache_prompt":true}'

# 原生 SSE
curl --fail-with-body -i -N http://127.0.0.1:18080/completion \
  -H 'Content-Type: application/json' -H 'X-Session-ID: demo-session' \
  -d '{"prompt":"Say hello.","n_predict":8,"stream":true,"cache_prompt":true}'

# OpenAI 兼容端点
curl --fail-with-body -i http://127.0.0.1:18080/v1/chat/completions \
  -H 'Content-Type: application/json' \
  -d '{"messages":[{"role":"user","content":"Say hello."}],"max_tokens":8}'

curl --fail-with-body http://127.0.0.1:18080/health
curl --fail-with-body http://127.0.0.1:18080/metrics
```

`X-Gateway-Backend` 给出实际后端，`X-Gateway-Queue-Ms` 给出等待时间。先确认原生 SSE 收到完整 `stop: true` 事件，再把它算作正常完成；`curl` 的退出码和 HTTP 200 不能单独判断流式完成。

## 真实 E2E

真实测试自行启动两个 llama-server，默认端口为 `18190` 和 `18191`，不依赖手动启动的网关。需先设置上述模型和引擎路径：

```bash
GATEWAY_REAL_BACKENDS=1 \
LLAMA_ARG_DEVICE=none LLAMA_ARG_N_GPU_LAYERS=0 \
uv run --locked pytest -q -m real_backend
```

可设置 `GATEWAY_E2E_PORTS=18190,18191` 改测试端口。测试检查原生 JSON、SSE、会话缓存复用和 OpenAI 兼容端点；读取控制台结果，确认没有因缺少资源而 skip。首次下载、模型加载和机器负载会影响耗时。该测试验证协议与功能，不是性能测量或内部推理取消证明。

无需模型的测试：

```bash
uv run --locked pytest -q tests bench/tests -m 'not real_backend'
```

## 健康迁移与期限演练

先在正常响应中记下某个会话的 `X-Gateway-Backend`。只有首选为 A 的会话才适合下列演练；否则选择另一会话，或停止实际首选后端。

```bash
./scripts/stop_backend.sh 18090
curl --fail-with-body http://127.0.0.1:18080/health
```

按默认设置等待两次失败探测，确认 A 为不健康，再发送同一会话请求。目标是返回 B。恢复 A：

```bash
./scripts/start_backend.sh 18090 a
```

等待两次成功探测后，同一会话会立即回到原首选。时间由配置与实际探测耗时决定，不以固定 sleep 代替状态确认。

短期限演练需重启网关：

```bash
uv run --locked python -m gateway --config config.toml \
  --execution-timeout-ms 50 --request-timeout-ms 2000
```

长请求在响应头提交前超时应得到 504；SSE 头部已提交则保持原状态并中止流。检查 `gateway_execution_timeouts_total`、流式终止原因及最终 active/queue，而不要只看状态码。

## 排查与清理

| 现象 | 检查 |
|---|---|
| 端口已占用 | `lsof -iTCP:18090 -sTCP:LISTEN`；确认服务归属，不终止未知进程 |
| `/health` 为 unavailable | 后端日志、模型路径、设备初始化和实际端口 |
| `429 queue_full` | 队列上限、客户端并发和 `gateway_queue_depth` |
| `429 queue_timeout` | 等待期限与后端延迟 |
| `503 no_healthy_backend` | 健康探测结果及进程存活状态 |
| `504` 或 SSE 提前结束 | 请求与执行期限、上游错误、完整结束事件 |
| 同会话没有缓存命中 | 实际后端是否相同、`cache_prompt`、`timings.cache_n` 和可复用前缀 |

手动启动的后端日志位于 `bench/logs/`；Benchmark 日志位于对应输出目录。先 Ctrl-C 停止网关，再运行：

```bash
./scripts/stop_backends.sh
```

脚本核对记录的进程身份后才停止服务；不要以删除 `.run/` 代替停止进程。最后用 `lsof` 检查本次使用的端口，避免清理其他实验服务。
