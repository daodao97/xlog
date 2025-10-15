package server

import (
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"xlog/internal/storage"
)

//go:embed static/index.html
var indexPage []byte

// Server 提供查询接口与简单界面。
type Server struct {
	store      storage.Store
	httpServer *http.Server
}

// New 返回 Server 实例。
func New(addr string, store storage.Store) *Server {
	s := &Server{store: store}
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", s.handleHealth)
	mux.HandleFunc("/api/logs", s.handleLogs)
	mux.HandleFunc("/api/containers", s.handleContainers)
	mux.HandleFunc("/api/stats", s.handleStats)
	mux.HandleFunc("/", s.handleIndex)

	s.httpServer = &http.Server{
		Addr:         addr,
		Handler:      withLogging(mux),
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 60 * time.Second,
		IdleTimeout:  120 * time.Second,
	}
	return s
}

// ListenAndServe 启动 HTTP 服务。
func (s *Server) ListenAndServe() error {
	return s.httpServer.ListenAndServe()
}

// Shutdown 优雅关闭。
func (s *Server) Shutdown(ctx context.Context) error {
	return s.httpServer.Shutdown(ctx)
}

func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) handleIndex(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if _, err := w.Write(indexPage); err != nil {
		log.Printf("write index failed: %v", err)
	}
}

func (s *Server) handleLogs(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	query := storage.LogQuery{}
	params := r.URL.Query()
	query.ContainerName = strings.TrimSpace(params.Get("container"))
	query.Stream = strings.TrimSpace(params.Get("stream"))
	query.Level = strings.ToLower(strings.TrimSpace(params.Get("level")))
	query.Search = strings.TrimSpace(params.Get("search"))
	page := 1
	if pageStr := params.Get("page"); pageStr != "" {
		if p, err := strconv.Atoi(pageStr); err == nil && p > 0 {
			page = p
		}
	}
	if limitStr := params.Get("limit"); limitStr != "" {
		if limit, err := strconv.Atoi(limitStr); err == nil {
			query.Limit = limit
		}
	}
	if sinceStr := params.Get("since"); sinceStr != "" {
		if t, err := parseTimeInput(sinceStr); err == nil {
			query.Since = &t
		}
	}
	if untilStr := params.Get("until"); untilStr != "" {
		if t, err := parseTimeInput(untilStr); err == nil {
			query.Until = &t
		}
	}
	limit := query.Limit
	if limit <= 0 || limit > 1000 {
		limit = 200
	}
	query.Limit = limit
	query.Offset = (page - 1) * limit
	if query.Offset < 0 {
		query.Offset = 0
	}

	result, err := s.store.QueryLogs(r.Context(), query)
	if err != nil {
		log.Printf("query logs failed: %v", err)
		http.Error(w, "query failed", http.StatusInternalServerError)
		return
	}
	response := map[string]any{
		"items": result.Entries,
		"total": result.Total,
		"page":  page,
		"limit": limit,
	}
	writeJSON(w, http.StatusOK, response)
}

func (s *Server) handleContainers(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	containers, err := s.store.ListContainers(r.Context())
	if err != nil {
		log.Printf("list containers failed: %v", err)
		http.Error(w, "query failed", http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, containers)
}

func (s *Server) handleStats(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	stats, err := s.store.Stats(r.Context())
	if err != nil {
		log.Printf("stats query failed: %v", err)
		http.Error(w, "query failed", http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, stats)
}

func withLogging(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rl := &responseLogger{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rl, r)
		log.Printf("%s %s %d %s", r.Method, r.URL.Path, rl.status, time.Since(start))
	})
}

type responseLogger struct {
	http.ResponseWriter
	status int
}

func (r *responseLogger) WriteHeader(statusCode int) {
	r.status = statusCode
	r.ResponseWriter.WriteHeader(statusCode)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		log.Printf("write json failed: %v", err)
	}
}

func parseTimeInput(value string) (time.Time, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return time.Time{}, fmt.Errorf("empty time")
	}
	if d, err := time.ParseDuration(value); err == nil {
		return time.Now().Add(-d).UTC(), nil
	}
	if t, err := time.Parse(time.RFC3339Nano, value); err == nil {
		return t.UTC(), nil
	}
	if t, err := time.Parse(time.RFC3339, value); err == nil {
		return t.UTC(), nil
	}
	return time.Time{}, fmt.Errorf("invalid time format")
}
