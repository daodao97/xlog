package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/docker/docker/client"

	"xlog/internal/collector"
	"xlog/internal/server"
	"xlog/internal/storage"
)

type config struct {
	HTTPAddr string
	DBPath   string
	Tail     string
	Since    time.Duration
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
	return config{
		HTTPAddr: getEnv("XLOG_HTTP_ADDR", ":8080"),
		DBPath:   getEnv("XLOG_DUCKDB_PATH", "./data/logs.duckdb"),
		Tail:     getEnv("XLOG_LOG_TAIL", "200"),
		Since:    parseDurationEnv("XLOG_LOG_SINCE", 15*time.Minute),
	}
}

func getEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func parseDurationEnv(key string, def time.Duration) time.Duration {
	value := os.Getenv(key)
	if value == "" {
		return def
	}
	d, err := time.ParseDuration(value)
	if err != nil {
		log.Printf("环境变量 %s 解析失败 (%v), 使用默认值 %s", key, err, def)
		return def
	}
	return d
}
