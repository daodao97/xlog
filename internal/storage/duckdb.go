package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	_ "github.com/marcboeker/go-duckdb"
)

// LogEntry 表示一条日志记录。
type LogEntry struct {
	ID            int64     `json:"id"`
	Timestamp     time.Time `json:"timestamp"`
	ContainerID   string    `json:"containerId"`
	ContainerName string    `json:"containerName"`
	Stream        string    `json:"stream"`
	Level         string    `json:"level"`
	Message       string    `json:"message"`
}

// LogQuery 用于筛选日志。
type LogQuery struct {
	ContainerName string
	Search        string
	Stream        string
	Level         string
	Since         *time.Time
	Until         *time.Time
	Limit         int
	Offset        int
}

// LogResult 为查询结果。
type LogResult struct {
	Entries []LogEntry `json:"items"`
	Total   int64      `json:"total"`
}

type ContainerStat struct {
	ContainerName string    `json:"containerName"`
	LogCount      int64     `json:"logCount"`
	MessageBytes  int64     `json:"messageBytes"`
	FirstAt       time.Time `json:"firstTimestamp"`
	LastAt        time.Time `json:"lastTimestamp"`
}

// Stats 汇总信息。
type Stats struct {
	ContainerCount int64 `json:"containers"`
	LogCount       int64 `json:"logs"`
	SizeBytes      int64 `json:"sizeBytes"`
}

// Store 定义日志存储需要实现的接口。
type Store interface {
	Init(ctx context.Context) error
	InsertLog(ctx context.Context, entry LogEntry) error
	QueryLogs(ctx context.Context, q LogQuery) (LogResult, error)
	ListContainers(ctx context.Context) ([]string, error)
	CleanupOlderThan(ctx context.Context, cutoff time.Time) (int64, error)
	CleanupExceedingSize(ctx context.Context, maxBytes int64) (int64, error)
	Stats(ctx context.Context) (Stats, error)
	ContainerStats(ctx context.Context) ([]ContainerStat, error)
	DeleteContainerLogs(ctx context.Context, name string) (int64, error)
	Close() error
}

// DuckDBStore 基于 DuckDB 的实现。
type DuckDBStore struct {
	db   *sql.DB
	path string
}

// NewDuckDB 创建 DuckDB 存储实例。
func NewDuckDB(path string) (*DuckDBStore, error) {
	if err := ensureDir(path); err != nil {
		return nil, err
	}
	absPath, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	db, err := sql.Open("duckdb", fmt.Sprintf("%s?access_mode=read_write", absPath))
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	return &DuckDBStore{db: db, path: absPath}, nil
}

// Init 初始化日志表与索引。
func (s *DuckDBStore) Init(ctx context.Context) error {
	queries := []string{
		`CREATE SEQUENCE IF NOT EXISTS logs_id_seq START 1`,
		`CREATE TABLE IF NOT EXISTS logs (
			id BIGINT DEFAULT nextval('logs_id_seq'),
			timestamp TIMESTAMP,
			container_id VARCHAR,
			container_name VARCHAR,
			stream VARCHAR,
			level VARCHAR,
			message TEXT
		)`,
		`ALTER TABLE logs ADD COLUMN IF NOT EXISTS level VARCHAR`,
		`CREATE INDEX IF NOT EXISTS logs_timestamp_idx ON logs(timestamp)`,
		`CREATE INDEX IF NOT EXISTS logs_container_idx ON logs(container_name)`,
	}
	for _, query := range queries {
		if _, err := s.db.ExecContext(ctx, query); err != nil {
			return err
		}
	}
	return nil
}

// InsertLog 写入一条日志记录。
func (s *DuckDBStore) InsertLog(ctx context.Context, entry LogEntry) error {
	entry.Timestamp = entry.Timestamp.UTC()
	if entry.Level != "" {
		entry.Level = strings.ToLower(entry.Level)
	}
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO logs (timestamp, container_id, container_name, stream, level, message)
		VALUES (?, ?, ?, ?, ?, ?)
	`, entry.Timestamp, entry.ContainerID, entry.ContainerName, entry.Stream, entry.Level, entry.Message)
	return err
}

// QueryLogs 查询日志列表。
func (s *DuckDBStore) QueryLogs(ctx context.Context, q LogQuery) (LogResult, error) {
	var (
		builder strings.Builder
		args    []any
	)
	builder.WriteString("SELECT id, timestamp, container_id, container_name, stream, level, message FROM logs")
	filters := make([]string, 0, 5)
	if q.ContainerName != "" {
		filters = append(filters, "LOWER(container_name) LIKE ?")
		args = append(args, "%"+strings.ToLower(q.ContainerName)+"%")
	}
	if q.Stream != "" {
		filters = append(filters, "stream = ?")
		args = append(args, q.Stream)
	}
	if q.Level != "" {
		filters = append(filters, "LOWER(level) = ?")
		args = append(args, strings.ToLower(q.Level))
	}
	if q.Since != nil {
		filters = append(filters, "timestamp >= ?")
		args = append(args, q.Since.UTC())
	}
	if q.Until != nil {
		filters = append(filters, "timestamp <= ?")
		args = append(args, q.Until.UTC())
	}
	if q.Search != "" {
		filters = append(filters, "LOWER(message) LIKE ?")
		args = append(args, "%"+strings.ToLower(q.Search)+"%")
	}
	whereClause := ""
	if len(filters) > 0 {
		whereClause = " WHERE " + strings.Join(filters, " AND ")
	}

	limit := q.Limit
	if limit <= 0 || limit > 1000 {
		limit = 200
	}
	offset := q.Offset
	if offset < 0 {
		offset = 0
	}

	countQuery := "SELECT COUNT(*) FROM logs" + whereClause
	var total int64
	if err := s.db.QueryRowContext(ctx, countQuery, args...).Scan(&total); err != nil {
		return LogResult{}, err
	}

	dataArgs := make([]any, len(args))
	copy(dataArgs, args)

	builder.WriteString(whereClause)
	builder.WriteString(" ORDER BY timestamp DESC")
	builder.WriteString(" LIMIT ?")
	dataArgs = append(dataArgs, limit)
	if offset > 0 {
		builder.WriteString(" OFFSET ?")
		dataArgs = append(dataArgs, offset)
	}

	rows, err := s.db.QueryContext(ctx, builder.String(), dataArgs...)
	if err != nil {
		return LogResult{}, err
	}
	defer rows.Close()

	var entries []LogEntry
	for rows.Next() {
		var (
			entry LogEntry
			level sql.NullString
		)
		if err := rows.Scan(&entry.ID, &entry.Timestamp, &entry.ContainerID, &entry.ContainerName, &entry.Stream, &level, &entry.Message); err != nil {
			return LogResult{}, err
		}
		if level.Valid {
			entry.Level = strings.ToLower(level.String)
		}
		entry.Timestamp = entry.Timestamp.UTC()
		entries = append(entries, entry)
	}
	if err := rows.Err(); err != nil {
		return LogResult{}, err
	}

	return LogResult{Entries: entries, Total: total}, nil
}

// ListContainers 返回已存在的容器名。
func (s *DuckDBStore) ListContainers(ctx context.Context) ([]string, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT DISTINCT container_name FROM logs ORDER BY container_name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var names []string
	for rows.Next() {
		var name sql.NullString
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		if name.Valid && name.String != "" {
			names = append(names, name.String)
		}
	}
	return names, rows.Err()
}

// CleanupOlderThan 删除在 cutoff 之前的日志。
func (s *DuckDBStore) CleanupOlderThan(ctx context.Context, cutoff time.Time) (int64, error) {
	res, err := s.db.ExecContext(ctx, "DELETE FROM logs WHERE timestamp < ?", cutoff.UTC())
	if err != nil {
		return 0, err
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return 0, err
	}
	if affected > 0 {
		_ = s.vacuum(ctx)
	}
	return affected, nil
}

// CleanupExceedingSize 按容量限制清理历史日志。
func (s *DuckDBStore) CleanupExceedingSize(ctx context.Context, maxBytes int64) (int64, error) {
	if maxBytes <= 0 {
		return 0, nil
	}
	var totalDeleted int64
	const batchSize = 2000
	for {
		size, err := s.fileSize()
		if err != nil {
			return totalDeleted, err
		}
		if size <= maxBytes {
			break
		}
		res, err := s.db.ExecContext(ctx, "DELETE FROM logs WHERE id IN (SELECT id FROM logs ORDER BY timestamp ASC LIMIT ?)", batchSize)
		if err != nil {
			return totalDeleted, err
		}
		affected, err := res.RowsAffected()
		if err != nil {
			return totalDeleted, err
		}
		if affected == 0 {
			break
		}
		totalDeleted += affected
	}
	if totalDeleted > 0 {
		_ = s.vacuum(ctx)
	}
	return totalDeleted, nil
}

// Stats 返回聚合信息。
func (s *DuckDBStore) Stats(ctx context.Context) (Stats, error) {
	var result Stats
	row := s.db.QueryRowContext(ctx, `
		SELECT
			COUNT(DISTINCT CASE WHEN container_name IS NULL OR container_name = '' THEN NULL ELSE container_name END),
			COUNT(*)
		FROM logs
	`)
	if err := row.Scan(&result.ContainerCount, &result.LogCount); err != nil {
		return Stats{}, err
	}
	size, err := s.fileSize()
	if err != nil {
		return Stats{}, err
	}
	result.SizeBytes = size
	return result, nil
}

func (s *DuckDBStore) ContainerStats(ctx context.Context) ([]ContainerStat, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT
			container_name,
			COUNT(*) AS log_count,
			COALESCE(SUM(LENGTH(message)), 0) AS message_bytes,
			MIN(timestamp) AS first_ts,
			MAX(timestamp) AS last_ts
		FROM logs
		GROUP BY container_name
		ORDER BY COALESCE(container_name, '')
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var stats []ContainerStat
	for rows.Next() {
		var (
			name  sql.NullString
			count sql.NullInt64
			bytes sql.NullInt64
			first sql.NullTime
			last  sql.NullTime
		)
		if err := rows.Scan(&name, &count, &bytes, &first, &last); err != nil {
			return nil, err
		}
		stat := ContainerStat{
			ContainerName: name.String,
			LogCount:      count.Int64,
			MessageBytes:  bytes.Int64,
		}
		if first.Valid {
			stat.FirstAt = first.Time.UTC()
		}
		if last.Valid {
			stat.LastAt = last.Time.UTC()
		}
		stats = append(stats, stat)
	}
	return stats, rows.Err()
}

func (s *DuckDBStore) DeleteContainerLogs(ctx context.Context, name string) (int64, error) {
	name = strings.TrimSpace(name)
	res, err := s.db.ExecContext(ctx, `DELETE FROM logs WHERE COALESCE(container_name, '') = ?`, name)
	if err != nil {
		return 0, err
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return 0, err
	}
	if affected > 0 {
		_ = s.vacuum(ctx)
	}
	return affected, nil
}

func (s *DuckDBStore) vacuum(ctx context.Context) error {
	_, err := s.db.ExecContext(ctx, "VACUUM")
	return err
}

func (s *DuckDBStore) fileSize() (int64, error) {
	if s.path == "" {
		return 0, fmt.Errorf("database path unset")
	}
	info, err := os.Stat(s.path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return 0, nil
		}
		return 0, err
	}
	return info.Size(), nil
}

// Close 关闭数据库连接。
func (s *DuckDBStore) Close() error {
	if s.db == nil {
		return nil
	}
	return s.db.Close()
}

func ensureDir(path string) error {
	abspath, err := filepath.Abs(path)
	if err != nil {
		return err
	}
	dir := filepath.Dir(abspath)
	if _, err := os.Stat(dir); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return os.MkdirAll(dir, 0o755)
		}
		return err
	}
	return nil
}
