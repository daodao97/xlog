# Repository Guidelines

## 项目结构与模块组织
- 顶层包含 `cmd/xlog`（入口 `main.go`）与 `internal` 模块，前者负责二进制启动与配置装载，后者拆分 Collector、Storage 与 Server，保持单一职责。
- `internal/collector` 订阅 Docker 事件并标准化日志；`internal/storage` 通过 DuckDB 落盘并执行查询；`internal/server` 暴露 REST、认证逻辑与内置静态前端，静态文件位于 `internal/server/static`。
- 运行数据保存在 `data/` 目录，默认数据库为 `data/logs.duckdb`；演示相关配置位于仓库根目录的 `docker-compose.yml`、`Dockerfile` 与 `data/seed/`（存在时用于样例导入）。
- 配置管理逻辑集中在 `cmd/xlog/main.go` 的 `config` 结构体与辅助函数，新增配置项时需同时更新 JSON 序列化与环境变量解析。

## 构建、测试与开发命令
- `go run ./cmd/xlog`：本地直接启动服务，支持环境变量热加载调试。
- `go build -o bin/xlog ./cmd/xlog`：生成可执行文件，便于部署，可配合 `./bin/xlog` 运行。
- `go test ./...`：执行全部 Go 单元测试，确保提交前通过；如需对单个包调试，可运行 `go test ./internal/collector`。
- `go vet ./...`：静态检查潜在问题，推荐在改动核心模块前后执行。
- `docker compose up -d --build`：构建并启动完整演示环境（含 BusyBox 日志源）。

## 代码风格与命名约定
- 遵循 Go 官方风格：使用 `gofmt`/`goimports`（Go 1.21+ 内置）格式化，保持 tab 缩进与 `CamelCase` 导出命名，私有符号采用 `camelCase`。
- 服务内的结构体、接口、文件命名以职责为导向，例如 `collector.go`、`storage.go`；新增模块请放入对应子包并附 README 或注释说明职责。
- 日志输出统一使用标准库 `log` 搭配结构化信息，避免引入新的日志依赖；HTTP 处理函数建议返回具象错误并集中在上层转换。
- 引入第三方依赖前确认 `go.mod` 中是否已有替代，统一通过 `go mod tidy` 清理依赖，并在 PR 描述中说明用途。

## 测试规范
- 使用 Go 原生测试框架，测试函数命名为 `TestXxx`，表驱动用例推荐命名 `cases := []struct{...}` 并通过 `t.Run` 子用例描述场景。
- 新功能需覆盖核心 happy path 与错误分支，涉及 DuckDB 查询请使用临时文件（`t.TempDir()`）并在测试结束清理，避免污染仓库。
- 目标是保持 `go test ./...` 全绿；如修改公共 API，请补充或更新 `internal` 对应包下的测试，并考虑增加模拟 Docker 客户端的集成测试。
- 提交前建议执行 `go test -run TestCollector ./internal/collector` 等针对性命令，加速定位回归。

## 提交与 Pull Request 指南
- Commit 信息保持一句话命令式（如 "Add collector retry"），必要时在正文列出要点与破坏性修改提示，避免使用含糊提交（"update"）。
- PR 描述包含：变更背景、主要修改点、验证方式（命令/截图）及相关 Issue 链接；涉及配置或接口变更需同步更新 `README.md` 或示例配置。
- 请求至少一位维护者 Review，针对数据库或安全相关改动建议增加第二位审阅；附上本地测试输出或截图以减少往返沟通。
- 合并前确认分支已与 `main` 同步，CI（若启用）全部通过，无多余调试文件或临时脚本。

## 安全与配置提示
- 服务需要访问宿主 `docker.sock`，请限制容器运行用户，并在非生产环境外开启 `XLOG_VIEW_PASSWORD` 以避免未授权访问。
- 配置优先使用 `XLOG_CONFIG_PATH` JSON 文件管理，与环境变量保持一致；生产环境请将数据库与配置目录挂载至持久化存储，并定期备份 `logs.duckdb`。
- 调试敏感日志前移除机密字段，确保提交记录中不包含真实密码、令牌或客户数据；提交后请自检 `git diff --cached`。
- 部署在公网时务必设置反向代理的 HTTPS，并将 `XLOG_COOKIE_SECURE` 设为 `true`，避免 Cookie 在明文通道泄露。

## 文档与协作
- 修改前阅读 `README.md` 以保持用户文档同步，新增 CLI 参数或 API 字段务必更新表格说明。
- 发现文档缺口可直接在对应段落追加示例或截图，必要时在 Issue 中描述场景并链接 PR，方便追踪历史。
