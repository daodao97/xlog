package server

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	_ "embed"
	"encoding/hex"
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

//go:embed static/verify.html
var verifyPage []byte

//go:embed static/manager.html
var managerPage []byte

// Server 提供查询接口与简单界面。
type Server struct {
	store               storage.Store
	httpServer          *http.Server
	authEnabled         bool
	passwordHash        string
	cookieName          string
	cookieTTL           time.Duration
	cookieSecure        bool
	getAllowPatterns    func() []string
	updateAllowPatterns func([]string) error
}

type Options struct {
	Password            string
	CookieName          string
	CookieTTL           time.Duration
	CookieSecure        bool
	GetAllowPatterns    func() []string
	UpdateAllowPatterns func([]string) error
}

// New 返回 Server 实例。
func New(addr string, store storage.Store, opts Options) *Server {
	authEnabled := strings.TrimSpace(opts.Password) != ""
	passwordHash := ""
	if authEnabled {
		passwordHash = hashPassword(opts.Password)
	}
	cookieName := opts.CookieName
	if cookieName == "" {
		cookieName = "xlog_auth"
	}
	cookieTTL := opts.CookieTTL
	if cookieTTL <= 0 {
		cookieTTL = 7 * 24 * time.Hour
	}
	s := &Server{
		store:               store,
		authEnabled:         authEnabled,
		passwordHash:        passwordHash,
		cookieName:          cookieName,
		cookieTTL:           cookieTTL,
		cookieSecure:        opts.CookieSecure,
		getAllowPatterns:    opts.GetAllowPatterns,
		updateAllowPatterns: opts.UpdateAllowPatterns,
	}
	if s.getAllowPatterns == nil {
		s.getAllowPatterns = func() []string { return nil }
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", s.handleHealth)
	mux.Handle("/api/logs", s.authRequired(http.HandlerFunc(s.handleLogs)))
	mux.Handle("/api/containers", s.authRequired(http.HandlerFunc(s.handleContainers)))
	mux.Handle("/api/containers/stats", s.authRequired(http.HandlerFunc(s.handleContainerStats)))
	mux.Handle("/api/containers/cleanup", s.authRequired(http.HandlerFunc(s.handleContainerCleanup)))
	mux.Handle("/api/stats", s.authRequired(http.HandlerFunc(s.handleStats)))
	mux.HandleFunc("/api/verify", s.handleVerify)
	mux.HandleFunc("/verify", s.handleVerifyPage)
	mux.HandleFunc("/manager", s.handleManager)
	mux.Handle("/api/config", s.authRequired(http.HandlerFunc(s.handleConfig)))
	mux.HandleFunc("/", s.handleIndex)

	s.httpServer = &http.Server{
		Addr:         addr,
		Handler:      withLogging(mux),
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 6 * time.Minute, // 延长写超时,支持大数据量删除操作
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
	if s.authEnabled && !s.isAuthorized(r) {
		http.Redirect(w, r, "/verify", http.StatusFound)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if _, err := w.Write(indexPage); err != nil {
		log.Printf("write index failed: %v", err)
	}
}

func (s *Server) handleVerifyPage(w http.ResponseWriter, r *http.Request) {
	if !s.authEnabled {
		http.Redirect(w, r, "/", http.StatusFound)
		return
	}
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if s.isAuthorized(r) {
		http.Redirect(w, r, "/", http.StatusFound)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if _, err := w.Write(verifyPage); err != nil {
		log.Printf("write verify page failed: %v", err)
	}
}

func (s *Server) handleManager(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if s.authEnabled && !s.isAuthorized(r) {
		http.Redirect(w, r, "/verify", http.StatusFound)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if _, err := w.Write(managerPage); err != nil {
		log.Printf("write manager page failed: %v", err)
	}
}

func (s *Server) handleConfig(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		patterns := s.getAllowPatterns()
		if patterns == nil {
			patterns = []string{}
		}
		writeJSON(w, http.StatusOK, map[string]any{"allowPatterns": normalizePatternList(patterns)})
	case http.MethodPost:
		if s.updateAllowPatterns == nil {
			http.Error(w, "config update disabled", http.StatusForbidden)
			return
		}
		var payload struct {
			AllowPatterns []string `json:"allowPatterns"`
		}
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			http.Error(w, "invalid payload", http.StatusBadRequest)
			return
		}
		patterns := normalizePatternList(payload.AllowPatterns)
		if err := s.updateAllowPatterns(patterns); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"allowPatterns": patterns})
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *Server) handleContainerStats(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	stats, err := s.store.ContainerStats(r.Context())
	if err != nil {
		log.Printf("container stats query failed: %v", err)
		http.Error(w, "query failed", http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, stats)
}

func (s *Server) handleContainerCleanup(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var payload struct {
		ContainerName *string `json:"containerName"`
	}
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		http.Error(w, "invalid payload", http.StatusBadRequest)
		return
	}
	if payload.ContainerName == nil {
		http.Error(w, "containerName required", http.StatusBadRequest)
		return
	}
	name := strings.TrimSpace(*payload.ContainerName)

	// 为大数据量删除操作设置更长的超时时间 (5分钟)
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Minute)
	defer cancel()

	deleted, err := s.store.DeleteContainerLogs(ctx, name)
	if err != nil {
		if ctx.Err() == context.DeadlineExceeded {
			log.Printf("cleanup container %s timeout (deleted: %d)", name, deleted)
			http.Error(w, fmt.Sprintf("cleanup timeout, partially deleted: %d rows", deleted), http.StatusRequestTimeout)
		} else {
			log.Printf("cleanup container %s failed: %v (deleted: %d)", name, err, deleted)
			http.Error(w, "cleanup failed", http.StatusInternalServerError)
		}
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"deleted": deleted,
	})
}

func (s *Server) handleVerify(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if !s.authEnabled {
		writeJSON(w, http.StatusOK, map[string]any{"authorized": true})
		return
	}
	var payload struct {
		Password string `json:"password"`
	}
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		http.Error(w, "invalid payload", http.StatusBadRequest)
		return
	}
	if strings.TrimSpace(payload.Password) == "" {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	if subtle.ConstantTimeCompare([]byte(hashPassword(payload.Password)), []byte(s.passwordHash)) != 1 {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	s.setAuthCookie(w)
	writeJSON(w, http.StatusOK, map[string]any{"authorized": true})
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

func (s *Server) authRequired(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !s.authEnabled || s.isAuthorized(r) {
			next.ServeHTTP(w, r)
			return
		}
		http.Error(w, "unauthorized", http.StatusUnauthorized)
	})
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

func (s *Server) isAuthorized(r *http.Request) bool {
	if !s.authEnabled {
		return true
	}
	cookie, err := r.Cookie(s.cookieName)
	if err != nil {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(cookie.Value), []byte(s.passwordHash)) == 1
}

func (s *Server) setAuthCookie(w http.ResponseWriter) {
	if !s.authEnabled {
		return
	}
	cookie := &http.Cookie{
		Name:     s.cookieName,
		Value:    s.passwordHash,
		Path:     "/",
		HttpOnly: true,
		SameSite: http.SameSiteLaxMode,
		Secure:   s.cookieSecure,
	}
	if s.cookieTTL > 0 {
		expires := time.Now().Add(s.cookieTTL)
		cookie.Expires = expires
		cookie.MaxAge = int(s.cookieTTL.Seconds())
	}
	http.SetCookie(w, cookie)
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

func normalizePatternList(patterns []string) []string {
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

func hashPassword(password string) string {
	trimmed := strings.TrimSpace(password)
	sum := sha256.Sum256([]byte(trimmed))
	return hex.EncodeToString(sum[:])
}
