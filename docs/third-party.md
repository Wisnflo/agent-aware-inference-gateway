# 第三方依赖与设计参考

网关代码采用仓库根目录的 [MIT License](../LICENSE)。该许可证不改变依赖、推理引擎或模型的原有许可。源码包不包含第三方源码副本、llama-server 二进制和模型权重；依赖通过锁文件安装，外部资源由使用者单独取得。

## 设计参考

[vLLM Router](https://github.com/vllm-project/production-stack/blob/main/src/vllm_router/README.md)提供轮询、会话路由与健康发现等设计参考，其所属 [production-stack 采用 Apache-2.0](https://github.com/vllm-project/production-stack/blob/main/LICENSE)。本项目以本地 llama.cpp 为目标，独立组织 Starlette、HTTPX 与 admission controller 的请求生命周期；没有把 production-stack 作为依赖分发。

Session affinity、rendezvous hashing 与健康过滤均为成熟方法。当前实现不包含 prefix-aware 或 KV-aware routing，不宣称提出新路由算法，也没有与 vLLM Router 做同条件的性能优劣比较。后续若直接引入上游代码，应保留其版权及许可证，并核对对应 NOTICE。

## Python 依赖

以下版本以当前 `uv.lock` 和已安装分发包的许可元数据核对；许可正文由各包的 wheel 或 sdist 提供。版本更新时同步复查。表中包含开发与传递依赖，未把它们改授 MIT。

| 分发包 | 锁定版本 | 声明许可证 |
|---|---|---|
| starlette | 1.10.0 | BSD-3-Clause |
| uvicorn | 0.54.0 | BSD-3-Clause |
| httpx | 0.28.1 | BSD-3-Clause |
| prometheus-client | 0.26.0 | Apache-2.0 AND BSD-2-Clause |
| anyio | 4.16.0 | MIT |
| certifi | 2026.10.23 | MPL-2.0 |
| click | 8.5.0 | BSD-3-Clause |
| h11 | 0.16.0 | MIT |
| httpcore | 1.0.9 | BSD-3-Clause |
| idna | 3.20 | BSD-3-Clause |
| typing-extensions | 4.16.0 | PSF-2.0 |
| pytest | 9.2.0 | MIT |
| pytest-asyncio | 1.4.1.post1 | Apache-2.0 |
| iniconfig | 2.3.1 | MIT |
| packaging | 26.3 | Apache-2.0 OR BSD-2-Clause |
| pluggy | 1.7.0 | MIT |
| Pygments | 2.21.0 | BSD-2-Clause |
| colorama | 0.4.6 | BSD-3-Clause |

`colorama` 是 Windows 条件依赖，不在本次 macOS 环境中安装，其 PyPI wheel 的许可正文单独核对。构建后端为 `hatchling==1.33.0`，采用 MIT；其构建依赖还包括上表的 packaging、pluggy，以及 pathspec（MPL-2.0）、tomlkit（MIT）和 trove-classifiers（Apache-2.0）。后面三项许可由 PyPI 分发包核对。Python ≥3.11 不启用其 tomli 条件依赖。

构建后端固定版本，但这些构建传递依赖没有进入 `uv.lock` 的运行依赖图，因此不承诺未来构建字节完全一致。构建隔离环境与网关运行环境区分。

重新核对安装元数据可以运行：

```bash
uv run --locked python - <<'PY'
from importlib.metadata import distributions
for dist in sorted(distributions(), key=lambda item: item.metadata['Name'].lower()):
    meta = dist.metadata
    license_text = meta.get('License-Expression') or meta.get('License', '')
    print(meta['Name'], dist.version, license_text.splitlines()[0] if license_text else '')
PY
```

依赖安装包通常自带许可文件。若制作包含第三方库的容器、离线依赖包或可执行分发，应一并保留对应许可文本与必要通知；当前源码包只携带网关源码和依赖描述。

## 推理引擎与模型

- [llama.cpp](https://github.com/ggml-org/llama.cpp)采用 [MIT License](https://github.com/ggml-org/llama.cpp/blob/785322d17f2d51e5185cf996a1f4e46d87640841/LICENSE)。参考提交为 `785322d17f2d51e5185cf996a1f4e46d87640841`。
- [Qwen2.5-0.5B-Instruct-GGUF](https://huggingface.co/Qwen/Qwen2.5-0.5B-Instruct-GGUF)的发布方 [LICENSE 为 Apache-2.0](https://huggingface.co/Qwen/Qwen2.5-0.5B-Instruct-GGUF/blob/main/LICENSE)。参考量化文件与校验值见[运行手册](runbook.md)。此结论只针对这一模型仓库，不概括其他 Qwen 模型。

模型和引擎均不随源码包分发。替换模型或分发二进制时，应检查实际资源及其依赖的许可证。

## 文档图示

结构图由仓库内 Mermaid 源文件生成；README 展示图以该结构为依据，通过图像生成制作并核对模块关系。Mermaid CLI 是文档生成工具，未加入运行依赖或源码分发。图示仅表达当前代码结构，不包含第三方品牌资产。
