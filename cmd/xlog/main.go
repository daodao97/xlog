package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/docker/docker/client"

	"xlog/internal/collector"
	"xlog/internal/server"
	"xlog/internal/storage"
)

type config struct {
	HTTPAddr               string
	DBPath                 string
	Tail                   string
	Since                  time.Duration
	CleanupInterval        time.Duration
	Retention              time.Duration
	MaxStorageBytes        int64
	ConfigPath             string
	ViewPassword           string
	AuthCookieName         string
	AuthCookieTTL          time.Duration
	CookieSecure           bool
	PrimaryConfigPath      string `json:"-"`
	ContainerAllowPatterns []string
}

const defaultConfigPath = "/data/app.json"

var fallbackConfigPaths = []string{"./data/app.json", "./app.json"}

func main() {
	cfg := loadConfig()
	compiledPatterns, err := compileAllowPatterns(cfg.ContainerAllowPatterns)
	if err != nil {
		log.Fatalf("容器白名单配置错误: %v", err)
	}
	cfgManager := newConfigManager(&cfg)
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
		AllowPatterns: compiledPatterns,
	}
	collectorInstance := collector.New(dockerCli, store, collectorOptions)
	log.Printf("日志采集启动: tail=%q since=%s", collectorOptions.Tail, collectorOptions.SinceDuration)
	go collectorInstance.Run(ctx)

	updatePatterns := func(patterns []string) error {
		patterns = normalizePatterns(patterns)
		compiled, err := compileAllowPatterns(patterns)
		if err != nil {
			return err
		}
		if err := cfgManager.SetAllowPatterns(patterns); err != nil {
			return err
		}
		updateCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		collectorInstance.SetAllowPatterns(updateCtx, compiled)
		log.Printf("更新容器白名单: %d 条", len(patterns))
		return nil
	}

	httpServer := server.New(cfg.HTTPAddr, store, server.Options{
		Password:            cfg.ViewPassword,
		CookieName:          cfg.AuthCookieName,
		CookieTTL:           cfg.AuthCookieTTL,
		CookieSecure:        cfg.CookieSecure,
		GetAllowPatterns:    cfgManager.GetAllowPatterns,
		UpdateAllowPatterns: updatePatterns,
	})
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
		ConfigPath:      getEnv("XLOG_CONFIG_PATH", defaultConfigPath),
		ViewPassword:    "",
		AuthCookieName:  getEnv("XLOG_AUTH_COOKIE_NAME", "xlog_auth"),
		AuthCookieTTL:   7 * 24 * time.Hour,
		CookieSecure:    false,
	}
	cfg.PrimaryConfigPath = cfg.ConfigPath

	paths := []string{cfg.ConfigPath}
	if cfg.ConfigPath == defaultConfigPath {
		paths = append(paths, fallbackConfigPaths...)
	}
	passwordProvided := false
	if fileCfg, pathUsed, err := loadConfigFromPaths(paths); err == nil {
		cfg.ConfigPath = pathUsed
		applyFileConfig(&cfg, fileCfg)
		if strings.TrimSpace(fileCfg.ViewPassword) != "" {
			log.Printf("从配置文件加载访问密码 (path=%s)", pathUsed)
			passwordProvided = true
		}
		if len(fileCfg.ContainerAllowPatterns) > 0 {
			cfg.ContainerAllowPatterns = append([]string(nil), fileCfg.ContainerAllowPatterns...)
		}
	} else if err != nil && !errors.Is(err, os.ErrNotExist) {
		log.Printf("读取配置文件失败: %v", err)
	}

	if applyEnvOverrides(&cfg) {
		log.Printf("从环境变量覆盖访问密码")
		passwordProvided = true
	}

	cfg.ContainerAllowPatterns = normalizePatterns(cfg.ContainerAllowPatterns)

	if cfg.ViewPassword == "" {
		for _, path := range []string{cfg.ConfigPath, cfg.PrimaryConfigPath} {
			if strings.TrimSpace(path) == "" {
				continue
			}
			if fileCfg, err := readConfigFile(path); err == nil {
				if v := strings.TrimSpace(fileCfg.ViewPassword); v != "" {
					cfg.ViewPassword = v
					log.Printf("沿用已有访问密码 (path=%s)", path)
					passwordProvided = true
					break
				}
			}
		}
	}

	cfg.ViewPassword = strings.TrimSpace(cfg.ViewPassword)

	generated := false
	if cfg.ViewPassword == "" && !passwordProvided {
		cfg.ViewPassword = generatePassword(16)
		generated = true
	}

	if err := persistConfig(&cfg); err != nil {
		log.Printf("保存配置文件失败: %v", err)
	} else if generated {
		log.Printf("生成随机访问密码: %s", cfg.ViewPassword)
	} else {
		log.Printf("访问密码已就绪 (source=%s)", passwordSource(passwordProvided, generated))
	}

	log.Printf("服务配置: addr=%s db=%s tail=%s since=%s clean=%s retention=%s max=%d path=%s auth=%t cookie=%s ttl=%s secure=%t allow=%d",
		cfg.HTTPAddr,
		cfg.DBPath,
		cfg.Tail,
		cfg.Since,
		cfg.CleanupInterval,
		cfg.Retention,
		cfg.MaxStorageBytes,
		cfg.ConfigPath,
		cfg.ViewPassword != "",
		cfg.AuthCookieName,
		durationToString(cfg.AuthCookieTTL),
		cfg.CookieSecure,
		len(cfg.ContainerAllowPatterns),
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
	HTTPAddr               string   `json:"httpAddr"`
	DBPath                 string   `json:"dbPath"`
	Tail                   string   `json:"tail"`
	Since                  string   `json:"since"`
	CleanupInterval        string   `json:"cleanupInterval"`
	Retention              string   `json:"retention"`
	MaxStorageBytes        string   `json:"maxStorageBytes"`
	ViewPassword           string   `json:"viewPassword"`
	AuthCookieName         string   `json:"authCookieName"`
	AuthCookieTTL          string   `json:"authCookieTtl"`
	CookieSecure           bool     `json:"cookieSecure"`
	ContainerAllowPatterns []string `json:"containerAllowPatterns"`
}

type fileConfig struct {
	HTTPAddr               string   `json:"httpAddr"`
	DBPath                 string   `json:"dbPath"`
	Tail                   string   `json:"tail"`
	Since                  string   `json:"since"`
	CleanupInterval        string   `json:"cleanupInterval"`
	Retention              string   `json:"retention"`
	MaxStorageBytes        string   `json:"maxStorageBytes"`
	ViewPassword           string   `json:"viewPassword"`
	AuthCookieName         string   `json:"authCookieName"`
	AuthCookieTTL          string   `json:"authCookieTtl"`
	CookieSecure           *bool    `json:"cookieSecure"`
	ContainerAllowPatterns []string `json:"containerAllowPatterns"`
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

func loadConfigFromPaths(paths []string) (fileConfig, string, error) {
	var lastErr error
	for _, path := range paths {
		if path == "" {
			continue
		}
		cfg, err := readConfigFile(path)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) || isPermissionError(err) {
				lastErr = err
				continue
			}
			return fileConfig{}, path, err
		}
		return cfg, path, nil
	}
	if len(paths) == 0 {
		return fileConfig{}, "", os.ErrNotExist
	}
	if lastErr == nil {
		lastErr = os.ErrNotExist
	}
	return fileConfig{}, paths[0], lastErr
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
	if fc.ViewPassword != "" {
		cfg.ViewPassword = fc.ViewPassword
	}
	if fc.AuthCookieName != "" {
		cfg.AuthCookieName = fc.AuthCookieName
	}
	if fc.AuthCookieTTL != "" {
		if d, err := time.ParseDuration(fc.AuthCookieTTL); err == nil {
			cfg.AuthCookieTTL = d
		}
	}
	if fc.CookieSecure != nil {
		cfg.CookieSecure = *fc.CookieSecure
	}
	if len(fc.ContainerAllowPatterns) > 0 {
		cfg.ContainerAllowPatterns = append([]string(nil), fc.ContainerAllowPatterns...)
	}
}

func applyEnvOverrides(cfg *config) bool {
	updated := false
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
	if v := os.Getenv("XLOG_VIEW_PASSWORD"); v != "" {
		cfg.ViewPassword = v
		updated = true
	}
	if v := os.Getenv("XLOG_AUTH_COOKIE_NAME"); v != "" {
		cfg.AuthCookieName = v
	}
	if d, ok := durationFromEnv("XLOG_AUTH_COOKIE_TTL"); ok {
		cfg.AuthCookieTTL = d
	}
	if b, ok := boolFromEnv("XLOG_COOKIE_SECURE"); ok {
		cfg.CookieSecure = b
	}
	if patterns := os.Getenv("XLOG_CONTAINER_ALLOW_PATTERNS"); patterns != "" {
		cfg.ContainerAllowPatterns = splitAndTrim(patterns)
	}
	return updated
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

func boolFromEnv(key string) (bool, bool) {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return false, false
	}
	b, err := strconv.ParseBool(value)
	if err != nil {
		log.Printf("环境变量 %s 解析失败 (%v), 忽略", key, err)
		return false, false
	}
	return b, true
}

func passwordSource(envProvided bool, generated bool) string {
	if generated {
		return "generated"
	}
	if envProvided {
		return "env"
	}
	return "config"
}

var rng = rand.New(rand.NewSource(time.Now().UnixNano()))

func generatePassword(length int) string {
	const letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	if length <= 0 {
		length = 16
	}
	b := make([]byte, length)
	for i := range b {
		b[i] = letters[rng.Intn(len(letters))]
	}
	return string(b)
}

func persistConfig(cfg *config) error {
	seen := make(map[string]struct{})
	var attempts []string
	var successPath string

	tryPath := func(path string) bool {
		if path == "" {
			return false
		}
		if _, ok := seen[path]; ok {
			return false
		}
		seen[path] = struct{}{}
		err := saveConfigToPath(path, *cfg)
		if err == nil {
			successPath = path
			return true
		}
		attempts = append(attempts, fmt.Sprintf("%s: %v", path, err))
		return false
	}

	if tryPath(cfg.ConfigPath) {
		cfg.ConfigPath = successPath
		return nil
	}
	if tryPath(cfg.PrimaryConfigPath) {
		cfg.ConfigPath = successPath
		return nil
	}

	if successPath == "" {
		for _, path := range fallbackConfigPaths {
			if tryPath(path) {
				cfg.ConfigPath = successPath
				if cfg.PrimaryConfigPath != "" {
					log.Printf("配置无法写入 %s，已保存到备用路径: %s", cfg.PrimaryConfigPath, successPath)
				} else {
					log.Printf("配置已保存到备用路径: %s", successPath)
				}
				return nil
			}
		}
	}

	if successPath != "" {
		cfg.ConfigPath = successPath
		return nil
	}
	if len(attempts) == 0 {
		return fmt.Errorf("无法保存配置文件: 未提供可写路径")
	}
	return fmt.Errorf("无法保存配置文件 (%s)", strings.Join(attempts, "; "))
}

func saveConfig(cfg config) error {
	if cfg.ConfigPath == "" {
		return nil
	}
	return saveConfigToPath(cfg.ConfigPath, cfg)
}

func saveConfigToPath(path string, cfg config) error {
	if path == "" {
		return fmt.Errorf("config path empty")
	}
	persist := persistedConfig{
		HTTPAddr:               cfg.HTTPAddr,
		DBPath:                 cfg.DBPath,
		Tail:                   cfg.Tail,
		Since:                  durationToString(cfg.Since),
		CleanupInterval:        durationToString(cfg.CleanupInterval),
		Retention:              durationToString(cfg.Retention),
		MaxStorageBytes:        strconv.FormatInt(cfg.MaxStorageBytes, 10),
		ViewPassword:           cfg.ViewPassword,
		AuthCookieName:         cfg.AuthCookieName,
		AuthCookieTTL:          durationToString(cfg.AuthCookieTTL),
		CookieSecure:           cfg.CookieSecure,
		ContainerAllowPatterns: cfg.ContainerAllowPatterns,
	}
	data, err := json.MarshalIndent(persist, "", "  ")
	if err != nil {
		return err
	}
	if err := ensureConfigDir(path); err != nil {
		return err
	}
	tmpPath := path + ".tmp"
	if err := os.WriteFile(tmpPath, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmpPath, path)
}

func ensureConfigDir(path string) error {
	dir := filepath.Dir(path)
	if dir == "." || dir == "" {
		return nil
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	return nil
}

func isPermissionError(err error) bool {
	var pathErr *os.PathError
	if errors.As(err, &pathErr) {
		if pathErr.Err == syscall.EACCES || pathErr.Err == syscall.EROFS || pathErr.Err == syscall.EPERM {
			return true
		}
	}
	return false
}

type configManager struct {
	mu  sync.RWMutex
	cfg *config
}

func newConfigManager(cfg *config) *configManager {
	return &configManager{cfg: cfg}
}

func (m *configManager) GetAllowPatterns() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return append([]string(nil), m.cfg.ContainerAllowPatterns...)
}

func (m *configManager) SetAllowPatterns(patterns []string) error {
	patterns = normalizePatterns(patterns)
	m.mu.Lock()
	defer m.mu.Unlock()
	old := append([]string(nil), m.cfg.ContainerAllowPatterns...)
	m.cfg.ContainerAllowPatterns = append([]string(nil), patterns...)
	if err := persistConfig(m.cfg); err != nil {
		m.cfg.ContainerAllowPatterns = old
		return err
	}
	return nil
}

func splitAndTrim(input string) []string {
	if strings.TrimSpace(input) == "" {
		return nil
	}
	fields := strings.FieldsFunc(input, func(r rune) bool {
		switch r {
		case '\n', '\r', ',', ';':
			return true
		default:
			return false
		}
	})
	return normalizePatterns(fields)
}

func normalizePatterns(patterns []string) []string {
	seen := make(map[string]struct{})
	result := make([]string, 0, len(patterns))
	for _, pattern := range patterns {
		p := strings.TrimSpace(pattern)
		if p == "" {
			continue
		}
		if _, ok := seen[p]; ok {
			continue
		}
		seen[p] = struct{}{}
		result = append(result, p)
	}
	return result
}

func compileAllowPatterns(patterns []string) ([]*regexp.Regexp, error) {
	patterns = normalizePatterns(patterns)
	if len(patterns) == 0 {
		return nil, nil
	}
	compiled := make([]*regexp.Regexp, 0, len(patterns))
	for _, pattern := range patterns {
		re, err := regexp.Compile(pattern)
		if err != nil {
			return nil, fmt.Errorf("无效容器白名单正则 %q: %w", pattern, err)
		}
		compiled = append(compiled, re)
	}
	return compiled, nil
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
