package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/docker/docker/client"

	"xlog/internal/collector"
	"xlog/internal/server"
	"xlog/internal/storage"
)

type config struct {
	HTTPAddr        string
	DBPath          string
	Tail            string
	Since           time.Duration
	CleanupInterval time.Duration
	Retention       time.Duration
	MaxStorageBytes int64
	ConfigPath      string
}

func main() {
	cfg := loadConfig()
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	dockerCli, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		log.Fatalf("初始化 Docker 客户端失败: %v", err)
	}
	defer dockerCli.Close()

	store, err := storage.NewDuckDB(cfg.DBPath)
	if err != nil {
		log.Fatalf("打开 DuckDB 失败: %v", err)
	}
	defer store.Close()

	if err := store.Init(ctx); err != nil {
		log.Fatalf("初始化数据库失败: %v", err)
	}

	if cfg.Retention > 0 || cfg.MaxStorageBytes > 0 {
		startMaintenance(ctx, store, cfg)
	}

	collectorOptions := collector.Options{
		Tail:          cfg.Tail,
		SinceDuration: cfg.Since,
	}
	log.Printf("日志采集启动: tail=%q since=%s", collectorOptions.Tail, collectorOptions.SinceDuration)
	go collector.New(dockerCli, store, collectorOptions).Run(ctx)

	httpServer := server.New(cfg.HTTPAddr, store)
	go func() {
		log.Printf("HTTP 服务监听 %s", cfg.HTTPAddr)
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("HTTP 服务异常退出: %v", err)
		}
	}()

	<-ctx.Done()
	log.Println("收到退出信号, 正在关闭…")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		log.Printf("HTTP 服务关闭异常: %v", err)
	}
	log.Println("退出完成")
}

func loadConfig() config {
	cfg := config{
		HTTPAddr:        ":8080",
		DBPath:          "./data/logs.duckdb",
		Tail:            "200",
		Since:           15 * time.Minute,
		CleanupInterval: time.Hour,
		Retention:       0,
		MaxStorageBytes: 0,
		ConfigPath:      getEnv("XLOG_CONFIG_PATH", "/data/app.json"),
	}

	if fileCfg, err := readConfigFile(cfg.ConfigPath); err == nil {
		applyFileConfig(&cfg, fileCfg)
	} else if err != nil && !errors.Is(err, os.ErrNotExist) {
		log.Printf("读取配置文件失败 (%v), 使用默认配置", err)
	}

	applyEnvOverrides(&cfg)

	if err := saveConfig(cfg); err != nil {
		log.Printf("保存配置文件失败: %v", err)
	}

	log.Printf("服务配置: addr=%s db=%s tail=%s since=%s clean=%s retention=%s max=%d path=%s",
		cfg.HTTPAddr,
		cfg.DBPath,
		cfg.Tail,
		cfg.Since,
		cfg.CleanupInterval,
		cfg.Retention,
		cfg.MaxStorageBytes,
		cfg.ConfigPath,
	)

	return cfg
}

func getEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

type persistedConfig struct {
	HTTPAddr        string `json:"httpAddr"`
	DBPath          string `json:"dbPath"`
	Tail            string `json:"tail"`
	Since           string `json:"since"`
	CleanupInterval string `json:"cleanupInterval"`
	Retention       string `json:"retention"`
	MaxStorageBytes string `json:"maxStorageBytes"`
}

type fileConfig struct {
	HTTPAddr        string `json:"httpAddr"`
	DBPath          string `json:"dbPath"`
	Tail            string `json:"tail"`
	Since           string `json:"since"`
	CleanupInterval string `json:"cleanupInterval"`
	Retention       string `json:"retention"`
	MaxStorageBytes string `json:"maxStorageBytes"`
}

func readConfigFile(path string) (fileConfig, error) {
	var result fileConfig
	if path == "" {
		return result, fmt.Errorf("config path empty")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return result, err
	}
	if err := json.Unmarshal(data, &result); err != nil {
		return result, err
	}
	return result, nil
}

func applyFileConfig(cfg *config, fc fileConfig) {
	if fc.HTTPAddr != "" {
		cfg.HTTPAddr = fc.HTTPAddr
	}
	if fc.DBPath != "" {
		cfg.DBPath = fc.DBPath
	}
	if fc.Tail != "" {
		cfg.Tail = fc.Tail
	}
	if fc.Since != "" {
		if d, err := time.ParseDuration(fc.Since); err == nil {
			cfg.Since = d
		}
	}
	if fc.CleanupInterval != "" {
		if d, err := time.ParseDuration(fc.CleanupInterval); err == nil {
			cfg.CleanupInterval = d
		}
	}
	if fc.Retention != "" {
		if d, err := time.ParseDuration(fc.Retention); err == nil {
			cfg.Retention = d
		}
	}
	if fc.MaxStorageBytes != "" {
		if v, err := parseBytes(fc.MaxStorageBytes); err == nil {
			cfg.MaxStorageBytes = v
		}
	}
}

func applyEnvOverrides(cfg *config) {
	if v := os.Getenv("XLOG_HTTP_ADDR"); v != "" {
		cfg.HTTPAddr = v
	}
	if v := os.Getenv("XLOG_DUCKDB_PATH"); v != "" {
		cfg.DBPath = v
	}
	if v := os.Getenv("XLOG_LOG_TAIL"); v != "" {
		cfg.Tail = v
	}
	if d, ok := durationFromEnv("XLOG_LOG_SINCE"); ok {
		cfg.Since = d
	}
	if d, ok := durationFromEnv("XLOG_CLEAN_INTERVAL"); ok {
		cfg.CleanupInterval = d
	}
	if d, ok := durationFromEnv("XLOG_RETENTION"); ok {
		cfg.Retention = d
	}
	if v, ok := bytesFromEnv("XLOG_MAX_STORAGE_BYTES"); ok {
		cfg.MaxStorageBytes = v
	}
}

func durationFromEnv(key string) (time.Duration, bool) {
	value := os.Getenv(key)
	if value == "" {
		return 0, false
	}
	d, err := time.ParseDuration(value)
	if err != nil {
		log.Printf("环境变量 %s 解析失败 (%v), 忽略", key, err)
		return 0, false
	}
	return d, true
}

func bytesFromEnv(key string) (int64, bool) {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return 0, false
	}
	v, err := parseBytes(value)
	if err != nil {
		log.Printf("环境变量 %s 解析失败 (%v), 忽略", key, err)
		return 0, false
	}
	return v, true
}

func saveConfig(cfg config) error {
	if cfg.ConfigPath == "" {
		return nil
	}
	persist := persistedConfig{
		HTTPAddr:        cfg.HTTPAddr,
		DBPath:          cfg.DBPath,
		Tail:            cfg.Tail,
		Since:           durationToString(cfg.Since),
		CleanupInterval: durationToString(cfg.CleanupInterval),
		Retention:       durationToString(cfg.Retention),
		MaxStorageBytes: strconv.FormatInt(cfg.MaxStorageBytes, 10),
	}
	data, err := json.MarshalIndent(persist, "", "  ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(cfg.ConfigPath), 0o755); err != nil {
		return err
	}
	tmpPath := cfg.ConfigPath + ".tmp"
	if err := os.WriteFile(tmpPath, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmpPath, cfg.ConfigPath)
}

func durationToString(d time.Duration) string {
	if d <= 0 {
		return ""
	}
	return d.String()
}

func parseBytes(value string) (int64, error) {
	v := strings.TrimSpace(strings.ToUpper(value))
	if v == "" {
		return 0, fmt.Errorf("empty bytes value")
	}
	multipliers := map[string]int64{
		"B":  1,
		"KB": 1024,
		"MB": 1024 * 1024,
		"GB": 1024 * 1024 * 1024,
		"TB": 1024 * 1024 * 1024 * 1024,
	}
	for unit, mul := range multipliers {
		if strings.HasSuffix(v, unit) {
			num := strings.TrimSpace(strings.TrimSuffix(v, unit))
			f, err := strconv.ParseFloat(num, 64)
			if err != nil {
				return 0, err
			}
			return int64(f * float64(mul)), nil
		}
	}
	// 无单位时按字节解析
	iv, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return 0, err
	}
	return iv, nil
}

func startMaintenance(ctx context.Context, store storage.Store, cfg config) {
	interval := cfg.CleanupInterval
	if interval <= 0 {
		interval = time.Hour
	}
	log.Printf("日志维护任务启动: interval=%s retention=%s maxSize=%dB", interval, cfg.Retention, cfg.MaxStorageBytes)
	worker := func() {
		maintenanceCtx, cancel := context.WithTimeout(ctx, interval)
		runMaintenance(maintenanceCtx, store, cfg)
		cancel()
	}
	worker()
	ticker := time.NewTicker(interval)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				maintenanceCtx, cancel := context.WithTimeout(ctx, interval)
				runMaintenance(maintenanceCtx, store, cfg)
				cancel()
			}
		}
	}()
}

func runMaintenance(ctx context.Context, store storage.Store, cfg config) {
	if cfg.Retention > 0 {
		cutoff := time.Now().Add(-cfg.Retention)
		if removed, err := store.CleanupOlderThan(ctx, cutoff); err != nil {
			log.Printf("清理过期日志失败: %v", err)
		} else if removed > 0 {
			log.Printf("清理过期日志 %d 条 (截止 %s)", removed, cutoff.Format(time.RFC3339))
		}
	}
	if cfg.MaxStorageBytes > 0 {
		if removed, err := store.CleanupExceedingSize(ctx, cfg.MaxStorageBytes); err != nil {
			log.Printf("按容量清理日志失败: %v", err)
		} else if removed > 0 {
			log.Printf("按容量清理日志 %d 条 (限制 %dB)", removed, cfg.MaxStorageBytes)
		}
	}
}
