package collector

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/events"
	"github.com/docker/docker/client"
	"github.com/docker/docker/pkg/stdcopy"

	"xlog/internal/storage"
)

// Options 控制采集行为。
type Options struct {
	Tail          string
	SinceDuration time.Duration
	AllowPatterns []*regexp.Regexp
}

// Collector 持续监听 docker 日志并写入存储。
type Collector struct {
	cli     *client.Client
	store   storage.Store
	options Options

	mu            sync.RWMutex
	streams       map[string]context.CancelFunc
	names         map[string]string
	allowPatterns []*regexp.Regexp
}

// New 创建收集器。
func New(cli *client.Client, store storage.Store, opts Options) *Collector {
	return &Collector{
		cli:           cli,
		store:         store,
		options:       opts,
		streams:       make(map[string]context.CancelFunc),
		names:         make(map[string]string),
		allowPatterns: append([]*regexp.Regexp(nil), opts.AllowPatterns...),
	}
}

// Run 启动日志采集，直到 ctx 结束。
func (c *Collector) Run(ctx context.Context) {
	c.bootstrap(ctx)
	go c.watchEvents(ctx)

	<-ctx.Done()
	c.stopAll()
}

func (c *Collector) bootstrap(ctx context.Context) {
	containers, err := c.cli.ContainerList(ctx, types.ContainerListOptions{All: true})
	if err != nil {
		log.Printf("collector bootstrap failed: %v", err)
		return
	}
	for _, container := range containers {
		name := normalizeContainerName(container.Names)
		name = preferredContainerName(container.Labels, name)
		if name == "" {
			name = container.ID[:12]
		}
		c.startStream(ctx, container.ID, name)
	}
}

func (c *Collector) watchEvents(ctx context.Context) {
	for {
		eventsCh, errsCh := c.cli.Events(ctx, types.EventsOptions{})
	loop:
		for {
			select {
			case <-ctx.Done():
				return
			case err, ok := <-errsCh:
				if !ok {
					break loop
				}
				if err != nil && !errors.Is(err, context.Canceled) {
					log.Printf("docker events error: %v", err)
				}
				break loop
			case event, ok := <-eventsCh:
				if !ok {
					break loop
				}
				c.handleEvent(ctx, event)
			}
		}
		if ctx.Err() != nil {
			return
		}
		time.Sleep(2 * time.Second)
	}
}

func (c *Collector) handleEvent(ctx context.Context, event events.Message) {
	if event.Type != events.ContainerEventType {
		return
	}
	switch event.Action {
	case "start", "restart", "unpause":
		name := event.Actor.Attributes["name"]
		if name == "" {
			name = event.ID[:12]
		}
		name = preferredContainerName(event.Actor.Attributes, name)
		c.startStream(ctx, event.ID, name)
	case "die", "stop", "pause", "destroy":
		c.stopStream(event.ID)
	}
}

func (c *Collector) startStream(ctx context.Context, containerID, containerName string) {
	if !c.isAllowed(containerName) {
		return
	}
	c.mu.Lock()
	if _, exists := c.streams[containerID]; exists {
		c.mu.Unlock()
		return
	}
	childCtx, cancel := context.WithCancel(ctx)
	c.streams[containerID] = cancel
	c.names[containerID] = containerName
	c.mu.Unlock()

	go func() {
		defer c.stopStream(containerID)
		c.streamContainer(childCtx, containerID, containerName)
	}()
}

func (c *Collector) stopStream(containerID string) {
	c.mu.Lock()
	cancel, ok := c.streams[containerID]
	if ok {
		delete(c.streams, containerID)
	}
	delete(c.names, containerID)
	c.mu.Unlock()
	if ok {
		cancel()
	}
}

func (c *Collector) stopAll() {
	c.mu.Lock()
	ids := make([]string, 0, len(c.streams))
	for id := range c.streams {
		ids = append(ids, id)
	}
	c.names = make(map[string]string)
	c.mu.Unlock()
	for _, id := range ids {
		c.stopStream(id)
	}
}

func (c *Collector) streamContainer(ctx context.Context, containerID, containerName string) {
	options := types.ContainerLogsOptions{
		ShowStdout: true,
		ShowStderr: true,
		Follow:     true,
		Tail:       c.options.Tail,
		Timestamps: true,
	}
	if c.options.SinceDuration > 0 {
		options.Since = time.Now().Add(-c.options.SinceDuration).UTC().Format(time.RFC3339)
	}
	reader, err := c.cli.ContainerLogs(ctx, containerID, options)
	if err != nil {
		if !errors.Is(err, context.Canceled) {
			log.Printf("stream container %s failed: %v", containerName, err)
		}
		return
	}
	defer reader.Close()

	stdoutWriter := newLogWriter(ctx, c.store, containerID, containerName, "stdout")
	stderrWriter := newLogWriter(ctx, c.store, containerID, containerName, "stderr")
	if _, err := stdcopy.StdCopy(stdoutWriter, stderrWriter, reader); err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, io.EOF) {
		log.Printf("copy logs for %s failed: %v", containerName, err)
	}
}

func normalizeContainerName(names []string) string {
	for _, name := range names {
		if simplified := simplifyInstanceName(name); simplified != "" {
			return simplified
		}
	}
	return ""
}

func preferredContainerName(labels map[string]string, fallback string) string {
	candidates := []string{
		labels["com.docker.swarm.service.name"],
		labels["com.docker.compose.service"],
	}
	for _, candidate := range candidates {
		if candidate != "" {
			if name := simplifyInstanceName(candidate); name != "" {
				return name
			}
		}
	}

	// 确保 fallback 不为空
	if fallback == "" {
		return ""
	}

	simplified := simplifyInstanceName(fallback)
	if simplified == "" {
		// 如果简化后为空，返回原始 fallback（去除前导斜杠）
		// 这种情况理论上不应该发生，但作为安全保障
		return strings.TrimPrefix(fallback, "/")
	}

	return simplified
}

func simplifyInstanceName(name string) string {
	name = strings.TrimSpace(name)
	if name == "" {
		return ""
	}
	name = strings.TrimPrefix(name, "/")

	// 处理 Docker Swarm 容器名格式: service.1.xxxxx
	// 只有在恰好有 2 个点，且第二部分是纯数字时才简化为服务名
	// 其他格式（如 docker compose 的 project.service.replica）保持不变
	if strings.Count(name, ".") == 2 {
		parts := strings.SplitN(name, ".", 3)
		if len(parts) == 3 && isAllDigits(parts[1]) {
			return parts[0]
		}
	}

	return name
}

func isAllDigits(value string) bool {
	if value == "" {
		return false
	}
	for _, ch := range value {
		if ch < '0' || ch > '9' {
			return false
		}
	}
	return true
}

func (c *Collector) isAllowed(name string) bool {
	c.mu.RLock()
	patterns := c.allowPatterns
	c.mu.RUnlock()
	if len(patterns) == 0 {
		return true
	}
	for _, re := range patterns {
		if re.MatchString(name) {
			return true
		}
	}
	return false
}

func (c *Collector) SetAllowPatterns(ctx context.Context, patterns []*regexp.Regexp) {
	c.mu.Lock()
	c.allowPatterns = append([]*regexp.Regexp(nil), patterns...)
	c.mu.Unlock()
	c.enforceAllowList(ctx)
}

func (c *Collector) enforceAllowList(ctx context.Context) {
	containers, err := c.cli.ContainerList(ctx, types.ContainerListOptions{All: true})
	if err != nil && !errors.Is(err, context.Canceled) {
		log.Printf("enforce allow list failed: %v", err)
	}
	for _, container := range containers {
		name := normalizeContainerName(container.Names)
		name = preferredContainerName(container.Labels, name)
		if name == "" {
			name = container.ID[:12]
		}
		if c.isAllowed(name) {
			c.startStream(ctx, container.ID, name)
		}
	}
	c.mu.RLock()
	toStop := make([]string, 0)
	for id, name := range c.names {
		if !c.isAllowed(name) {
			toStop = append(toStop, id)
		}
	}
	c.mu.RUnlock()
	for _, id := range toStop {
		c.stopStream(id)
	}
}

type logWriter struct {
	ctx           context.Context
	store         storage.Store
	containerID   string
	containerName string
	stream        string

	buf bytes.Buffer
	mu  sync.Mutex
}

func newLogWriter(ctx context.Context, store storage.Store, containerID, containerName, stream string) *logWriter {
	return &logWriter{
		ctx:           ctx,
		store:         store,
		containerID:   containerID,
		containerName: containerName,
		stream:        stream,
	}
}

func (w *logWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	total := len(p)
	data := p
	for len(data) > 0 {
		idx := bytes.IndexByte(data, '\n')
		if idx == -1 {
			w.buf.Write(data)
			break
		}
		w.buf.Write(data[:idx])
		line := strings.TrimRight(w.buf.String(), "\r")
		w.buf.Reset()
		w.flushLine(line)
		data = data[idx+1:]
	}
	return total, nil
}

func (w *logWriter) flushLine(line string) {
	if line == "" {
		return
	}
	ts, level, message := parseLine(line)
	entry := storage.LogEntry{
		Timestamp:     ts,
		ContainerID:   w.containerID,
		ContainerName: w.containerName,
		Stream:        w.stream,
		Level:         level,
		Message:       message,
	}
	if err := w.store.InsertLog(w.ctx, entry); err != nil {
		log.Printf("insert log failed: %v", err)
	}
}

func parseLine(line string) (time.Time, string, string) {
	parts := strings.SplitN(line, " ", 2)
	if len(parts) == 0 {
		return time.Now().UTC(), "", line
	}
	ts, err := time.Parse(time.RFC3339Nano, parts[0])
	if err != nil {
		return time.Now().UTC(), "", line
	}
	message := ""
	if len(parts) > 1 {
		message = parts[1]
	}
	level, cleaned := detectLevel(message)
	return ts.UTC(), level, cleaned
}

func detectLevel(message string) (string, string) {
	trimmed := strings.TrimSpace(message)
	if trimmed == "" {
		return "", message
	}

	// 前缀形式 [INFO] message
	if strings.HasPrefix(trimmed, "[") {
		if end := strings.Index(trimmed, "]"); end > 1 {
			candidate := trimmed[1:end]
			if level := normalizeLevel(candidate); level != "" {
				return level, strings.TrimSpace(trimmed[end+1:])
			}
		}
	}

	// 前缀形式 INFO message / INFO: message
	if first, rest, has := strings.Cut(trimmed, " "); has {
		token := strings.Trim(first, "[]:.-")
		if level := normalizeLevel(token); level != "" {
			return level, strings.TrimSpace(rest)
		}
	}
	if idx := strings.Index(trimmed, ":"); idx > 0 {
		token := strings.Trim(trimmed[:idx], "[]:.- ")
		if level := normalizeLevel(token); level != "" {
			return level, strings.TrimSpace(trimmed[idx+1:])
		}
	}

	if level, cleaned, ok := detectKeyValueLevel(trimmed); ok {
		return level, cleaned
	}
	if level, cleaned, ok := detectJSONLevel(trimmed); ok {
		return level, cleaned
	}

	return "", message
}

func detectKeyValueLevel(text string) (string, string, bool) {
	lower := strings.ToLower(text)
	keys := []string{"level=", "lvl=", "severity=", "log.level=", "loglevel=", "priority="}
	for _, key := range keys {
		idx := strings.Index(lower, key)
		if idx == -1 {
			continue
		}
		start := idx + len(key)
		token, _ := extractLevelToken(text, start)
		if level := normalizeLevel(token); level != "" {
			return level, text, true
		}
	}
	return "", text, false
}

func detectJSONLevel(text string) (string, string, bool) {
	var payload map[string]any
	if err := json.Unmarshal([]byte(text), &payload); err != nil {
		return "", text, false
	}
	keys := []string{"level", "lvl", "severity", "log_level", "loglevel", "priority"}
	for _, key := range keys {
		if value, ok := payload[key]; ok {
			if str, ok := value.(string); ok {
				if level := normalizeLevel(str); level != "" {
					return level, text, true
				}
			}
		}
	}
	return "", text, false
}

func extractLevelToken(text string, start int) (string, int) {
	if start >= len(text) {
		return "", start
	}

	i := start
	// 跳过引号与空格
	for i < len(text) && (text[i] == ' ' || text[i] == '\t' || text[i] == '"' || text[i] == '\'') {
		i++
	}

	j := i
	for j < len(text) {
		c := text[j]
		if c == '"' || c == '\'' || c == ' ' || c == '\t' || c == ',' || c == ';' || c == '}' || c == ']' {
			break
		}
		j++
	}

	token := text[i:j]
	token = strings.Trim(token, "\"' ,;]")
	return token, j
}

func normalizeLevel(value string) string {
	switch strings.ToUpper(strings.TrimSpace(value)) {
	case "TRACE":
		return "trace"
	case "DEBUG":
		return "debug"
	case "INFO", "INFORMATION", "NOTICE":
		return "info"
	case "WARN", "WARNING":
		return "warn"
	case "ERROR", "ERR":
		return "error"
	case "FATAL", "CRITICAL":
		return "fatal"
	case "PANIC":
		return "panic"
	default:
		return ""
	}
}
