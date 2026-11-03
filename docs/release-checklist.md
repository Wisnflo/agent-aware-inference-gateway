# 发布前检查清单

**本地准备 6/6 完成。** 当前适合公开为受信任本机实验和实习展示项目。已修复独立审查中确认的缺陷；尚未创建远程仓库、提交本次改动或上传 GitHub。

- [x] **独立仓库。** 发布范围为 `gateway/` 独立 Git 仓库及其必要文档、测试和脚本。源码包不包含父级 AI-develop 研究目录或其他项目；当前没有 Git remote。
- [x] **敏感信息。** 扫描全部 2 个 Git 提交和当前目录，复核交付文件中的凭据、个人路径、环境变量、请求及日志。未发现真实密钥或个人路径进入候选包；公开示例使用本地地址与占位资源路径。原始运行结果留在忽略目录。
- [x] **模型与二进制。** 不提交约 409 MiB GGUF、llama-server、`.venv`、缓存及原始日志。wheel、sdist 内容检查通过；[运行手册](runbook.md)提供固定引擎提交、构建步骤、模型下载与 SHA-256 校验。
- [x] **许可证。** 添加 [MIT LICENSE](../LICENSE)，核对当前 Python 依赖、构建依赖、llama.cpp 和所用模型的许可；[第三方说明](third-party.md)明确 vLLM Router 设计参考和实现范围。
- [x] **自动化测试。** [GitHub Actions](../.github/workflows/tests.yml)覆盖无需模型的单元、fake/raw backend 及 Benchmark 回归；本地 Python 3.11、3.12、3.13 各 158 通过。真实 E2E 单独手动运行，本次 4 通过。未上传，因此 GitHub 托管 CI 尚未运行。
- [x] **公开展示。** [README](../README.md)解释问题、架构、运行、证据、Benchmark 口径和限制；提供可编辑[架构图](assets/architecture.mmd)及 SVG。最简真实演示完成两个后端和网关启动、缓存复用、停止首选后端、健康切换及自身进程清理。

实际命令、环境、资源身份和结果见[发布验证记录](release-verification.md)。完整性能矩阵与长期运行实验未重跑，保留限制见[limitations.md](limitations.md)。本清单完成的是本地发布准备，不表示已向不可信网络开放服务或已通过远程 CI。
