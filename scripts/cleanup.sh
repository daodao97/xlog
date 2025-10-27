#!/usr/bin/env bash
set -euo pipefail

DB_PATH=${DB_PATH:-"data/logs.duckdb"}
RETENTION_HOURS=${RETENTION_HOURS:-720}
MAX_BYTES=${MAX_BYTES:-2147483648}
ARCHIVE_DIR=${ARCHIVE_DIR:-"backup"}
ARCHIVE_KEEP=${ARCHIVE_KEEP:-7}

timestamp() {
	date -u +"%Y-%m-%dT%H-%M-%SZ"
}

log() {
	echo "[$(timestamp)] $*"
}

if [ ! -f "$DB_PATH" ]; then
	log "数据库文件 $DB_PATH 不存在, 跳过清理"
	exit 0
}

mkdir -p "$(dirname "$ARCHIVE_DIR")"
mkdir -p "$ARCHIVE_DIR"

log "导出当前数据库快照到 $ARCHIVE_DIR"
duckdb "$DB_PATH" <<SQL
EXPORT DATABASE '$ARCHIVE_DIR/$(timestamp)' (FORMAT PARQUET);
SQL

log "按时间清理 (保留 ${RETENTION_HOURS} 小时内日志)"
duckdb "$DB_PATH" <<SQL
DELETE FROM logs
WHERE timestamp < now() - INTERVAL '${RETENTION_HOURS} hours';
SQL

log "检查数据库容量是否超过 ${MAX_BYTES} 字节"
current_bytes=$(duckdb "$DB_PATH" "SELECT CAST(SUM(total_size - free_size) AS BIGINT) FROM pragma_database_size();")
if [ "${current_bytes:-0}" -gt "$MAX_BYTES" ]; then
	log "容量 ${current_bytes} 字节超限, 开始按批删除最旧日志"
	while true; do
		duckdb "$DB_PATH" <<'SQL'
DELETE FROM logs
WHERE id IN (
	SELECT id FROM logs ORDER BY timestamp ASC LIMIT 200000
);
SQL
		current_bytes=$(duckdb "$DB_PATH" "SELECT CAST(SUM(total_size - free_size) AS BIGINT) FROM pragma_database_size();")
		log "当前已使用 ${current_bytes} 字节"
		if [ "${current_bytes:-0}" -le "$MAX_BYTES" ]; then
			break
		fi
		if [ "${current_bytes:-0}" -eq 0 ]; then
			break
		fi
	done
fi

log "执行 VACUUM 回收空间"
duckdb "$DB_PATH" "VACUUM;"

log "清理超过 ${ARCHIVE_KEEP} 天的旧备份"
find "$ARCHIVE_DIR" -type d -mtime +$ARCHIVE_KEEP -print0 | xargs -0r rm -rf

log "清理完成"
