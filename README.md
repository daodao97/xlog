# xlog

基于 Go + DuckDB 的容器日志采集与检索服务，支持通过 Docker Socket 实时订阅容器标准输出，并将日志按容器名称持久化，同时提供简单的 Web 搜索界面与 REST API。

## 功能特点

- 监听 Docker 中所有容器的 stdout/stderr 日志，自动感知容器的新增/停止。
- 使用 DuckDB 作为嵌入式存储，支持高效的本地查询与持久化。
- 按容器名分组存储日志，保留日志时间、来源流信息。
- 提供前端页面可筛选容器（含关键字过滤）、日志等级/关键字、时间范围及分页浏览。
- 提供 `/api/logs`、`/api/containers` REST 接口，方便二次集成。
- 提供全局统计与自动清理策略，支持按时间或容量定期清理历史日志。

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
  "maxStorageBytes": "2147483648"
}
```

如需修改运行参数，可直接编辑该文件或设置对应环境变量，服务会在启动时自动合并并持久化。

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
- `GET /api/stats`
  - 返回当前容器数量、日志总数及 DuckDB 文件大小，用于前端展示
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

欢迎根据自身需求扩展筛选条件、认证机制或对接告警系统。
# container-log
