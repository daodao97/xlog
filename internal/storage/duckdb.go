package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	_ "github.com/marcboeker/go-duckdb/v2"
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

// ReclusterOptions 控制日志表重排的触发条件。
type ReclusterOptions struct {
	MinInterval          time.Duration
	MinTableBytes        int64
	MinFragmentationRate float64
	MinRowCount          int64
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
	DeleteContainerLogsBefore(ctx context.Context, name string, cutoff time.Time) (int64, error)
	EnsureIndexes(ctx context.Context) error
	OptimizeStatistics(ctx context.Context) error
	ReclusterIfNeeded(ctx context.Context, opts ReclusterOptions) (bool, error)
	Close() error
}

// DuckDBStore 基于 DuckDB 的实现。
type DuckDBStore struct {
	db   *sql.DB
	path string

	mu             sync.Mutex
	lastIndexCheck time.Time
	lastAnalyze    time.Time
	lastRecluster  time.Time
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

	// 尝试打开数据库,最多重试一次
	db, err := tryOpenDuckDB(absPath)
	if err != nil {
		return nil, err
	}

	return &DuckDBStore{db: db, path: absPath}, nil
}

// tryOpenDuckDB 尝试打开数据库,失败时自动恢复并重试
func tryOpenDuckDB(absPath string) (*sql.DB, error) {
	// 第一次尝试打开
	db, err := openDuckDBWithRecovery(absPath)
	if err != nil {
		// 如果打开失败,检查是否是损坏错误
		log.Printf("数据库打开失败: %v", err)

		// 尝试恢复损坏的数据库
		log.Printf("检测到数据库可能已损坏,尝试自动恢复...")
		if recoverErr := recoverCorruptedDB(absPath); recoverErr != nil {
			return nil, fmt.Errorf("数据库恢复失败: %w (原始错误: %v)", recoverErr, err)
		}

		// 恢复后重新尝试打开
		log.Printf("正在使用新数据库重新启动...")
		db, err = openDuckDBWithRecovery(absPath)
		if err != nil {
			return nil, fmt.Errorf("恢复后仍无法打开数据库: %w", err)
		}
		log.Printf("数据库已成功恢复并重新创建")
	}

	return db, nil
}

// openDuckDBWithRecovery 使用 recover 捕获 panic
func openDuckDBWithRecovery(absPath string) (db *sql.DB, err error) {
	// 捕获可能的 panic (DuckDB 的一些错误会导致 panic)
	defer func() {
		if r := recover(); r != nil {
			db = nil
			err = fmt.Errorf("数据库打开时发生严重错误: %v", r)
		}
	}()

	return openDuckDB(absPath)
}

// openDuckDB 打开 DuckDB 数据库连接
func openDuckDB(absPath string) (*sql.DB, error) {
	db, err := sql.Open("duckdb", fmt.Sprintf("%s?access_mode=read_write", absPath))
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	// 测试连接是否正常
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, err
	}

	return db, nil
}

// recoverCorruptedDB 恢复损坏的数据库文件
func recoverCorruptedDB(absPath string) error {
	// 检查文件是否存在
	if _, err := os.Stat(absPath); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			// 文件不存在,无需恢复
			return nil
		}
		return err
	}

	// 备份损坏的文件
	backupPath := fmt.Sprintf("%s.corrupted.%d", absPath, time.Now().Unix())
	log.Printf("备份损坏的数据库文件: %s -> %s", absPath, backupPath)

	if err := os.Rename(absPath, backupPath); err != nil {
		return fmt.Errorf("无法备份损坏的数据库文件: %w", err)
	}

	// 同时处理可能存在的 WAL 文件
	walPath := absPath + ".wal"
	if _, err := os.Stat(walPath); err == nil {
		walBackupPath := fmt.Sprintf("%s.corrupted.%d", walPath, time.Now().Unix())
		if err := os.Rename(walPath, walBackupPath); err != nil {
			log.Printf("警告: 无法备份 WAL 文件: %v", err)
		}
	}

	log.Printf("已删除损坏的数据库文件,将创建新的数据库")
	return nil
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
	}
	for _, query := range queries {
		if _, err := s.db.ExecContext(ctx, query); err != nil {
			return err
		}
	}
	if err := s.ensureIDSequence(ctx); err != nil {
		return err
	}
	if err := s.EnsureIndexes(ctx); err != nil {
		return err
	}
	if err := s.OptimizeStatistics(ctx); err != nil {
		log.Printf("初始化统计优化失败: %v", err)
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
	// 删除前健康检查
	if err := s.healthCheck(ctx); err != nil {
		log.Printf("删除前健康检查失败: %v", err)
		return 0, fmt.Errorf("数据库健康检查失败: %w", err)
	}

	res, err := s.db.ExecContext(ctx, "DELETE FROM logs WHERE timestamp < ?", cutoff.UTC())
	if err != nil {
		return 0, fmt.Errorf("删除失败: %w", err)
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return 0, err
	}
	if affected > 0 {
		log.Printf("按时间清理完成: 删除 %d 条日志", affected)
		if err := s.safeCheckpoint(ctx); err != nil {
			log.Printf("checkpoint 失败: %v", err)
		}
		if err := s.safeVacuum(ctx); err != nil {
			log.Printf("VACUUM 失败: %v", err)
		}
		if err := s.ensureIDSequence(ctx); err != nil {
			log.Printf("重建序列失败: %v", err)
		}
	}
	return affected, nil
}

// EnsureIndexes 创建关键索引以提升查询效率。
func (s *DuckDBStore) EnsureIndexes(ctx context.Context) error {
	s.mu.Lock()
	if !s.lastIndexCheck.IsZero() && time.Since(s.lastIndexCheck) < 6*time.Hour {
		s.mu.Unlock()
		return nil
	}
	s.mu.Unlock()
	statements := []string{
		`CREATE INDEX IF NOT EXISTS logs_timestamp_idx ON logs(timestamp)`,
		`CREATE INDEX IF NOT EXISTS logs_container_idx ON logs(container_name)`,
		`CREATE INDEX IF NOT EXISTS logs_container_time_idx ON logs(container_name, timestamp)`,
		`CREATE INDEX IF NOT EXISTS logs_stream_level_idx ON logs(stream, level)`,
	}
	for _, stmt := range statements {
		if _, err := s.db.ExecContext(ctx, stmt); err != nil {
			return err
		}
	}
	s.mu.Lock()
	s.lastIndexCheck = time.Now()
	s.mu.Unlock()
	return nil
}

// OptimizeStatistics 更新分析信息,便于 DuckDB 使用统计数据做更优计划。
func (s *DuckDBStore) OptimizeStatistics(ctx context.Context) error {
	s.mu.Lock()
	if !s.lastAnalyze.IsZero() && time.Since(s.lastAnalyze) < 6*time.Hour {
		s.mu.Unlock()
		return nil
	}
	s.mu.Unlock()
	if _, err := s.db.ExecContext(ctx, "ANALYZE logs"); err != nil {
		return err
	}
	s.tryPragmaOptimize(ctx)
	s.mu.Lock()
	s.lastAnalyze = time.Now()
	s.mu.Unlock()
	return nil
}

func (s *DuckDBStore) tryPragmaOptimize(ctx context.Context) {
	if s.db == nil {
		return
	}
	if _, err := s.db.ExecContext(ctx, "PRAGMA optimize"); err != nil {
		if !strings.Contains(strings.ToLower(err.Error()), "optimize") {
			log.Printf("PRAGMA optimize 执行失败: %v", err)
		}
	}
}

// CleanupExceedingSize 按容量限制清理历史日志。
func (s *DuckDBStore) CleanupExceedingSize(ctx context.Context, maxBytes int64) (int64, error) {
	if maxBytes <= 0 {
		return 0, nil
	}

	// 删除前健康检查
	if err := s.healthCheck(ctx); err != nil {
		log.Printf("删除前健康检查失败: %v", err)
		return 0, fmt.Errorf("数据库健康检查失败: %w", err)
	}

	var totalDeleted int64
	const batchSize = 2000
	checkpointInterval := 0

	for {
		// 检查 context 是否取消
		if err := ctx.Err(); err != nil {
			log.Printf("按大小清理被取消: %v (已删除 %d 条)", err, totalDeleted)
			return totalDeleted, err
		}

		size, err := s.fileSize()
		if err != nil {
			return totalDeleted, fmt.Errorf("获取文件大小失败: %w", err)
		}
		if size <= maxBytes {
			break
		}

		res, err := s.db.ExecContext(ctx, "DELETE FROM logs WHERE id IN (SELECT id FROM logs ORDER BY timestamp ASC LIMIT ?)", batchSize)
		if err != nil {
			return totalDeleted, fmt.Errorf("删除失败 (已删除 %d 条): %w", totalDeleted, err)
		}
		affected, err := res.RowsAffected()
		if err != nil {
			return totalDeleted, err
		}
		if affected == 0 {
			break
		}
		totalDeleted += affected
		checkpointInterval += int(affected)

		// 每删除 10000 条执行 checkpoint
		if checkpointInterval >= 10000 {
			if err := s.safeCheckpoint(ctx); err != nil {
				log.Printf("checkpoint 失败: %v (继续删除)", err)
			}
			checkpointInterval = 0
			log.Printf("按大小清理进度: 已删除 %d 条", totalDeleted)
		}
	}

	if totalDeleted > 0 {
		log.Printf("按大小清理完成: 删除 %d 条日志,正在执行空间回收...", totalDeleted)
		if err := s.safeCheckpoint(ctx); err != nil {
			log.Printf("最终 checkpoint 失败: %v", err)
		}
		if err := s.safeVacuum(ctx); err != nil {
			log.Printf("VACUUM 失败: %v", err)
		} else {
			log.Printf("空间回收完成")
		}
		if err := s.ensureIDSequence(ctx); err != nil {
			log.Printf("重建序列失败: %v", err)
		}
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

	// 删除前执行健康检查
	if err := s.healthCheck(ctx); err != nil {
		log.Printf("删除前健康检查失败: %v", err)
		return 0, fmt.Errorf("数据库健康检查失败: %w", err)
	}

	// 分批删除,避免大事务导致内存压力过大
	const batchSize = 3000 // 减小批次大小,降低内存压力
	var totalDeleted int64
	checkpointInterval := 0

	for {
		// 检查 context 是否已取消
		if err := ctx.Err(); err != nil {
			log.Printf("删除容器日志被取消: %v (已删除 %d 条)", err, totalDeleted)
			return totalDeleted, err
		}

		// 每批删除指定数量的记录
		res, err := s.db.ExecContext(ctx, `
			DELETE FROM logs
			WHERE id IN (
				SELECT id FROM logs
				WHERE COALESCE(container_name, '') = ?
				LIMIT ?
			)
		`, name, batchSize)

		if err != nil {
			// 如果删除过程中出错,返回已删除的数量和错误
			log.Printf("删除容器日志出错: %v (已删除 %d 条)", err, totalDeleted)
			return totalDeleted, fmt.Errorf("删除失败 (已删除 %d 条): %w", totalDeleted, err)
		}

		affected, err := res.RowsAffected()
		if err != nil {
			return totalDeleted, err
		}

		totalDeleted += affected
		checkpointInterval += int(affected)

		// 如果本批删除的数量少于批次大小,说明已经删除完毕
		if affected < batchSize {
			break
		}

		// 更频繁地执行 checkpoint (每删除 10000 条),避免 WAL 文件过大
		if checkpointInterval >= 10000 {
			if err := s.safeCheckpoint(ctx); err != nil {
				log.Printf("checkpoint 失败: %v (继续删除)", err)
			}
			checkpointInterval = 0
			log.Printf("删除容器 %q 日志进度: 已删除 %d 条", name, totalDeleted)
		}
	}

	// 删除完成后执行 checkpoint 和 VACUUM
	if totalDeleted > 0 {
		log.Printf("删除容器 %q 日志完成: 共删除 %d 条,正在执行空间回收...", name, totalDeleted)

		// 执行最终 checkpoint
		if err := s.safeCheckpoint(ctx); err != nil {
			log.Printf("最终 checkpoint 失败: %v", err)
		}

		// 执行 VACUUM 回收空间
		if err := s.safeVacuum(ctx); err != nil {
			log.Printf("VACUUM 失败: %v (空间可能未完全回收)", err)
		} else {
			log.Printf("空间回收完成")
		}

		// 删除后执行健康检查
		if err := s.healthCheck(ctx); err != nil {
			log.Printf("删除后健康检查失败: %v", err)
			return totalDeleted, fmt.Errorf("删除成功但数据库可能已损坏: %w", err)
		}
		if err := s.ensureIDSequence(ctx); err != nil {
			log.Printf("重建序列失败: %v", err)
		}
	}

	return totalDeleted, nil
}

func (s *DuckDBStore) DeleteContainerLogsBefore(ctx context.Context, name string, cutoff time.Time) (int64, error) {
	name = strings.TrimSpace(name)
	cutoff = cutoff.UTC()

	if cutoff.IsZero() {
		return 0, fmt.Errorf("cutoff time is required")
	}

	if err := s.healthCheck(ctx); err != nil {
		log.Printf("删除前健康检查失败: %v", err)
		return 0, fmt.Errorf("数据库健康检查失败: %w", err)
	}

	const batchSize = 3000
	var totalDeleted int64
	checkpointInterval := 0

	for {
		if err := ctx.Err(); err != nil {
			log.Printf("删除容器 %q 指定时间前的日志被取消: %v (已删除 %d 条)", name, err, totalDeleted)
			return totalDeleted, err
		}

		res, err := s.db.ExecContext(ctx, `
			DELETE FROM logs
			WHERE id IN (
				SELECT id FROM logs
				WHERE COALESCE(container_name, '') = ?
				  AND timestamp < ?
				LIMIT ?
			)
		`, name, cutoff, batchSize)
		if err != nil {
			log.Printf("删除容器 %q 指定时间前的日志出错: %v (已删除 %d 条)", name, err, totalDeleted)
			return totalDeleted, fmt.Errorf("删除失败 (已删除 %d 条): %w", totalDeleted, err)
		}

		affected, err := res.RowsAffected()
		if err != nil {
			return totalDeleted, err
		}

		totalDeleted += affected
		checkpointInterval += int(affected)

		if affected < batchSize {
			break
		}

		if checkpointInterval >= 10000 {
			if err := s.safeCheckpoint(ctx); err != nil {
				log.Printf("checkpoint 失败: %v (继续删除)", err)
			}
			checkpointInterval = 0
			log.Printf("删除容器 %q 在 %s 前日志进度: 已删除 %d 条", name, cutoff.Format(time.RFC3339), totalDeleted)
		}
	}

	if totalDeleted > 0 {
		log.Printf("删除容器 %q 在 %s 前的日志完成: 共删除 %d 条, 正在执行空间回收...", name, cutoff.Format(time.RFC3339), totalDeleted)
		if err := s.safeCheckpoint(ctx); err != nil {
			log.Printf("最终 checkpoint 失败: %v", err)
		}
		if err := s.safeVacuum(ctx); err != nil {
			log.Printf("VACUUM 失败: %v (空间可能未完全回收)", err)
		} else {
			log.Printf("空间回收完成")
		}
		if err := s.healthCheck(ctx); err != nil {
			log.Printf("删除后健康检查失败: %v", err)
			return totalDeleted, fmt.Errorf("删除成功但数据库可能已损坏: %w", err)
		}
		if err := s.ensureIDSequence(ctx); err != nil {
			log.Printf("重建序列失败: %v", err)
		}
	}

	return totalDeleted, nil
}

// ReclusterIfNeeded 按需重新排序日志表,减少碎片并提升顺序扫描性能。
func (s *DuckDBStore) ReclusterIfNeeded(ctx context.Context, opts ReclusterOptions) (bool, error) {
	if s.db == nil {
		return false, fmt.Errorf("database not initialized")
	}
	if opts.MinInterval <= 0 {
		opts.MinInterval = 24 * time.Hour
	}
	if opts.MinTableBytes <= 0 {
		opts.MinTableBytes = 512 * 1024 * 1024
	}
	if opts.MinFragmentationRate <= 0 {
		opts.MinFragmentationRate = 0.15
	}
	if opts.MinRowCount <= 0 {
		opts.MinRowCount = 500_000
	}

	s.mu.Lock()
	if !s.lastRecluster.IsZero() && time.Since(s.lastRecluster) < opts.MinInterval {
		s.mu.Unlock()
		return false, nil
	}
	s.mu.Unlock()

	size, err := s.fileSize()
	if err != nil {
		return false, err
	}
	if size < opts.MinTableBytes {
		return false, nil
	}

	var totalRows int64
	if err := s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM logs").Scan(&totalRows); err != nil {
		return false, err
	}
	if totalRows < opts.MinRowCount {
		return false, nil
	}

	var totalBlocks, freeBlocks int64
	if err := s.db.QueryRowContext(ctx, "SELECT COALESCE(SUM(total_blocks), 0), COALESCE(SUM(free_blocks), 0) FROM pragma_database_size()").Scan(&totalBlocks, &freeBlocks); err != nil {
		return false, err
	}
	if totalBlocks == 0 {
		return false, nil
	}
	fragmentation := float64(freeBlocks) / float64(totalBlocks)
	if fragmentation < opts.MinFragmentationRate {
		return false, nil
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	tmpName := fmt.Sprintf("logs_recluster_%d", time.Now().UnixNano())
	createSQL := fmt.Sprintf(`CREATE TABLE %s AS
		SELECT id, timestamp, container_id, container_name, stream, level, message
		FROM logs
		ORDER BY COALESCE(container_name, ''), timestamp, id`, tmpName)
	if _, err := tx.ExecContext(ctx, createSQL); err != nil {
		tx.Rollback()
		return false, err
	}
	if _, err := tx.ExecContext(ctx, "DROP TABLE logs"); err != nil {
		tx.Rollback()
		return false, err
	}
	if _, err := tx.ExecContext(ctx, fmt.Sprintf("ALTER TABLE %s RENAME TO logs", tmpName)); err != nil {
		tx.Rollback()
		return false, err
	}
	var maxID int64
	if err := tx.QueryRowContext(ctx, "SELECT COALESCE(MAX(id), 0) FROM logs").Scan(&maxID); err != nil {
		tx.Rollback()
		return false, err
	}
	if err := tx.Commit(); err != nil {
		return false, err
	}

	s.mu.Lock()
	s.lastIndexCheck = time.Time{}
	s.lastAnalyze = time.Time{}
	s.mu.Unlock()

	if err := s.ensureIDSequence(ctx); err != nil {
		return false, err
	}
	if err := s.EnsureIndexes(ctx); err != nil {
		return false, err
	}
	if err := s.OptimizeStatistics(ctx); err != nil {
		log.Printf("重排后统计更新失败: %v", err)
	}
	if err := s.safeCheckpoint(ctx); err != nil {
		log.Printf("重排后 checkpoint 失败: %v", err)
	}
	if err := s.safeVacuum(ctx); err != nil {
		log.Printf("重排后 VACUUM 失败: %v", err)
	}

	s.mu.Lock()
	s.lastRecluster = time.Now()
	s.mu.Unlock()

	return true, nil
}

func (s *DuckDBStore) ensureCheckpoint() {
	if s == nil || s.db == nil {
		return
	}
	ctx := context.Background()
	if _, err := s.db.ExecContext(ctx, "CHECKPOINT"); err != nil && !errors.Is(err, context.Canceled) {
		log.Printf("duckdb checkpoint failed: %v", err)
	}
}

func (s *DuckDBStore) ensureIDSequence(ctx context.Context) error {
	if s == nil || s.db == nil {
		return fmt.Errorf("database not initialized")
	}
	var maxID sql.NullInt64
	if err := s.db.QueryRowContext(ctx, "SELECT MAX(id) FROM logs").Scan(&maxID); err != nil {
		return err
	}
	start := int64(1)
	if maxID.Valid {
		start = maxID.Int64 + 1
	}
	var seqCount int
	err := s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM information_schema.sequences WHERE lower(sequence_name) = 'logs_id_seq'").Scan(&seqCount)
	if err != nil {
		if strings.Contains(strings.ToLower(err.Error()), "sequences") {
			seqCount = -1
		} else if !errors.Is(err, sql.ErrNoRows) {
			return err
		}
	}
	if seqCount <= 0 {
		createSQL := fmt.Sprintf("CREATE SEQUENCE IF NOT EXISTS logs_id_seq START %d", start)
		if _, err := s.db.ExecContext(ctx, createSQL); err != nil {
			errLower := strings.ToLower(err.Error())
			if !strings.Contains(errLower, "exists") {
				return err
			}
		}
	} else {
		if _, err := s.db.ExecContext(ctx, fmt.Sprintf("ALTER SEQUENCE logs_id_seq RESTART WITH %d", start)); err != nil {
			errLower := strings.ToLower(err.Error())
			if strings.Contains(errLower, "not supported") {
				log.Printf("跳过调整序列起点: %v", err)
			} else {
				return err
			}
		}
	}
	var defaultValue sql.NullString
	row := s.db.QueryRowContext(ctx, "SELECT dflt_value FROM pragma_table_info('logs') WHERE name='id'")
	switch err := row.Scan(&defaultValue); err {
	case nil:
	case sql.ErrNoRows:
		return nil
	default:
		return err
	}
	needsUpdate := true
	if defaultValue.Valid {
		lower := strings.ToLower(defaultValue.String)
		if strings.Contains(lower, "nextval") && strings.Contains(lower, "logs_id_seq") {
			needsUpdate = false
		}
	}
	if needsUpdate {
		if _, err := s.db.ExecContext(ctx, "ALTER TABLE logs ALTER COLUMN id SET DEFAULT nextval('logs_id_seq')"); err != nil {
			if strings.Contains(err.Error(), "Dependency Error") {
				log.Printf("跳过更新 id 默认序列: %v", err)
			} else {
				return err
			}
		}
	}
	return nil
}

// safeCheckpoint 在指定 context 下安全地执行 checkpoint
func (s *DuckDBStore) safeCheckpoint(ctx context.Context) error {
	if s == nil || s.db == nil {
		return fmt.Errorf("database not initialized")
	}

	// 使用带超时的 context,避免 checkpoint 阻塞过久
	checkpointCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	if _, err := s.db.ExecContext(checkpointCtx, "CHECKPOINT"); err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return fmt.Errorf("checkpoint 超时")
		}
		return err
	}
	return nil
}

// safeVacuum 安全地执行 VACUUM,回收删除后的空间
func (s *DuckDBStore) safeVacuum(ctx context.Context) error {
	if s == nil || s.db == nil {
		return fmt.Errorf("database not initialized")
	}

	// VACUUM 可能耗时较长,设置更长的超时
	vacuumCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()

	if _, err := s.db.ExecContext(vacuumCtx, "VACUUM"); err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return fmt.Errorf("VACUUM 超时")
		}
		return err
	}
	return nil
}

// healthCheck 检查数据库健康状态
func (s *DuckDBStore) healthCheck(ctx context.Context) error {
	if s == nil || s.db == nil {
		return fmt.Errorf("database not initialized")
	}

	// 检查数据库连接
	checkCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	if err := s.db.PingContext(checkCtx); err != nil {
		return fmt.Errorf("ping 失败: %w", err)
	}

	// 执行简单查询验证表结构
	var count int64
	if err := s.db.QueryRowContext(checkCtx, "SELECT COUNT(*) FROM logs LIMIT 1").Scan(&count); err != nil {
		return fmt.Errorf("查询验证失败: %w", err)
	}

	return nil
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
