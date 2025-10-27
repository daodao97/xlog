# xlog

基于 Go + DuckDB 的容器日志采集与检索服务，支持通过 Docker Socket 实时订阅容器标准输出，并将日志按容器名称持久化，同时提供简单的 Web 搜索界面与 REST API。

## 功能特点

- 监听 Docker 中所有容器的 stdout/stderr 日志，自动感知容器的新增/停止。
- 使用 DuckDB 作为嵌入式存储，支持高效的本地查询与持久化。
- 按容器名分组存储日志，保留日志时间、来源流信息。
- 提供前端页面可筛选容器（含关键字过滤）、日志等级/关键字、时间范围及分页浏览。
- 提供 `/api/logs`、`/api/containers` REST 接口，方便二次集成。
- 提供全局统计与自动清理策略，支持按时间或容量定期清理历史日志。
- 支持配置访问密码，未授权访问将跳转验证并写入安全 Cookie。
- 内置 `/manager` 页面，可在线管理容器白名单（支持正则表达式过滤）。

## 快速开始

运行本服务需要：

- Docker 宿主机，并允许容器访问 `/var/run/docker.sock`
- Go 1.21+（如需本地编译运行）

### Docker Compose 部署

```bash
docker compose up -d --build
```

默认会启动两个服务：

- `xlog`：日志采集与查询服务，Web 界面访问 `http://localhost:8080/`
- `demo`：示例 busybox 容器，持续输出日志，便于测试

日志数据会保存在当前目录的 `./data/logs.duckdb`，可持久化到宿主机。

### 本地运行

1. 安装依赖（需要网络）：

   ```bash
   go mod tidy
   ```

2. 启动服务：

   ```bash
   go run ./cmd/xlog
   ```

   或编译后运行：

   ```bash
   go build -o bin/xlog ./cmd/xlog
   ./bin/xlog
   ```

确保当前用户可以访问 Docker 宿主机（默认通过 `/var/run/docker.sock`）。

## 环境变量

| 变量名 | 默认值 | 说明 |
| --- | --- | --- |
| `XLOG_HTTP_ADDR` | `:8080` | HTTP 服务监听地址 |
| `XLOG_DUCKDB_PATH` | `./data/logs.duckdb` | DuckDB 数据文件路径 |
| `XLOG_LOG_TAIL` | `200` | 重启时回溯的日志条数（传 `all` 表示全部） |
| `XLOG_LOG_SINCE` | `15m` | 订阅容器日志的起始时间窗口（Duration 格式，`0` 表示不限制） |
| `XLOG_CLEAN_INTERVAL` | `1h` | 后台清理任务运行间隔 |
| `XLOG_RETENTION` | 空 | 日志保留时长（如 `720h`，为空表示不按时间清理） |
| `XLOG_MAX_STORAGE_BYTES` | 空 | 日志数据库最大占用（支持 `512MB`、`2GB` 等写法，空表示不限制） |
| `XLOG_CONFIG_PATH` | `/data/app.json` | 全局配置文件路径，服务启动后会写回最终配置 |
| `XLOG_VIEW_PASSWORD` | 空 | 访问日志界面的密码，留空表示不启用认证 |
| `XLOG_AUTH_COOKIE_NAME` | `xlog_auth` | 登录状态 Cookie 名称 |
| `XLOG_AUTH_COOKIE_TTL` | `168h` | 登录有效期（Duration 格式），默认 7 天 |
| `XLOG_COOKIE_SECURE` | `false` | 是否仅通过 HTTPS 发送登录 Cookie |
| `XLOG_CONTAINER_ALLOW_PATTERNS` | 空 | 容器白名单正则表达式（逗号或换行分隔），为空表示采集全部容器 |

## 全局配置文件

服务会在启动时读取 `XLOG_CONFIG_PATH` 指向的 JSON 文件（默认 `/data/app.json`），并在合并环境变量与默认值后写回，方便下次启动沿用配置。示例：

```json
{
  "httpAddr": "0.0.0.0:8080",
  "dbPath": "/data/logs.duckdb",
  "tail": "200",
  "since": "15m0s",
  "cleanupInterval": "1h0m0s",
  "retention": "720h0m0s",
  "maxStorageBytes": "2147483648",
  "viewPassword": "your-password",
  "authCookieName": "xlog_auth",
  "authCookieTtl": "168h0m0s",
  "cookieSecure": false,
  "containerAllowPatterns": ["^prod-.*", "^critical-service$"]
}
```

如需修改运行参数，可直接编辑该文件或设置对应环境变量，服务会在启动时自动合并并持久化（若配置文件路径不可写，服务会自动退回到本地 `./data/app.json` / `./app.json` 以兼容非容器环境）。

## 访问控制

- 设置 `XLOG_VIEW_PASSWORD`（或在配置文件 `viewPassword` 字段）后，所有页面与 API 均需要先通过 `/verify` 页面输入密码；如未显式设置，服务会在启动时自动生成随机密码并写入配置文件。
- 验证成功后，服务会写入一个带过期时间的 HttpOnly Cookie（默认 7 天有效），后续访问自动携带。
- 若需要强制用户使用 HTTPS，可将 `XLOG_COOKIE_SECURE=true`，Cookie 将仅在安全连接中发送。
- 清空密码或删除配置文件字段即可关闭认证。

## 容器白名单

- 通过 `/manager` 页面可以在线配置允许采集日志的容器名称列表，支持 Go 正则表达式，每行一条（示例：`^prod-.*`）。
- 同样可通过 `XLOG_CONTAINER_ALLOW_PATTERNS`（逗号或换行分隔）或配置文件的 `containerAllowPatterns` 字段进行配置。
- 白名单为空表示采集所有容器；更新后会立即生效，未匹配的容器会停止采集。

## 日志维护与索引

- 后台维护协程默认每小时运行一次：按 `XLOG_RETENTION` / `XLOG_MAX_STORAGE_BYTES` 清理旧日志，并自动检查索引、刷新统计信息。
- 当日志文件大于 ~512MB（或超过 `XLOG_MAX_STORAGE_BYTES` 的一半）、行数超过 30 万且碎片率较高时，会自动触发重排，将数据按容器+时间重新排序，以提升查询性能。
- 可通过 `duckdb` CLI 手动执行 `ANALYZE logs`、`PRAGMA optimize` 或 `VACUUM` 进一步优化；程序在维护流程中会自动尝试执行。

## API 说明

- `GET /api/logs`
  - 查询参数：
    - `container`：容器名
    - `search`：模糊匹配关键字
    - `stream`：`stdout` 或 `stderr`
    - `level`：日志等级（`trace`、`debug`、`info`、`warn`、`error`、`fatal`、`panic`）
    - `limit`：返回条数（默认 200，上限 1000）
    - `page`：页码（从 1 开始，结合 `limit` 分页）
    - `since`、`until`：支持 RFC3339 时间字符串或 Go Duration 表达式（如 `30m`）
  - 返回示例：

    ```json
    {
      "items": [ { "timestamp": "...", "message": "..." } ],
      "total": 1234,
      "page": 1,
      "limit": 200
    }
    ```
- `GET /api/containers`
  - 返回已存储日志的容器名称列表
- `GET /api/containers/stats`
  - 返回每个容器的日志条数、占用空间及时间范围
- `POST /api/containers/cleanup`
  - 请求体：`{ "containerName": "..." }`（空字符串表示未命名容器）；清理指定容器的全部日志
- `GET /api/stats`
  - 返回当前容器数量、日志总数及 DuckDB 文件大小，用于前端展示
- `POST /api/verify`
  - 请求体：`{ "password": "..." }`
  - 密码正确时返回 200 并写入登录 Cookie，否则返回 401
- `GET /api/config`
  - 返回当前容器白名单配置
- `POST /api/config`
  - 请求体：`{ "allowPatterns": ["regex", ...] }`，成功后立即生效
- `GET /healthz`
  - 健康检查

## 数据库结构

表 `logs` 字段：

- `id`：自增主键
- `timestamp`：日志时间（UTC）
- `container_id`：容器 ID
- `container_name`：容器名称
- `stream`：`stdout`/`stderr`
- `level`：日志等级（小写）
- `message`：原始日志内容

## 注意事项

- 第一次构建镜像或本地编译需要下载 Go 依赖，需确保网络可访问 Golang 模块仓库。
- 服务运行依赖 Docker Socket 权限，请根据安全策略限制访问。
- DuckDB 文件默认存放在 `./data` 目录，可根据需要挂载到其他路径或远程存储。

## 辅助脚本

- `scripts/cleanup.sh`：提供按保留时长与容量阈值的离线清理脚本，执行前确保安装 `duckdb` CLI。常用环境变量：
  - `DB_PATH`（默认 `data/logs.duckdb`）
  - `RETENTION_HOURS`（默认 720）
  - `MAX_BYTES`（默认 2GB）
  - `ARCHIVE_DIR`（默认 `backup/`，导出 Parquet 快照）
- 可配置定时任务运行脚本：`DB_PATH=/data/logs.duckdb MAX_BYTES=$((10*1024*1024*1024)) ./scripts/cleanup.sh`
- 脚本会自动导出最新快照、按批删除最旧日志并执行 `VACUUM` 与过期备份清理。

欢迎根据自身需求扩展筛选条件、认证机制或对接告警系统。
# container-log
