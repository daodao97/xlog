package collector

import (
	"testing"
)

func TestSimplifyInstanceName(t *testing.T) {
	tests := []struct {
		input    string
		expected string
		desc     string
	}{
		{
			input:    "nginx.1.abc123",
			expected: "nginx",
			desc:     "Swarm 格式容器名应该被简化",
		},
		{
			input:    "web.2.xyz789",
			expected: "web",
			desc:     "Swarm 格式容器名应该被简化",
		},
		{
			input:    "/nginx.1.abc123",
			expected: "nginx",
			desc:     "带前导斜杠的 Swarm 格式容器名",
		},
		{
			input:    "my-service",
			expected: "my-service",
			desc:     "普通容器名应该保持不变",
		},
		{
			input:    "/my-service",
			expected: "my-service",
			desc:     "带前导斜杠的普通容器名应该去除斜杠",
		},
		{
			input:    "app.prod.db",
			expected: "app.prod.db",
			desc:     "包含2个点但中间不是数字的名称应该保持不变 (docker compose 常见格式)",
		},
		{
			input:    "project.web.1",
			expected: "project.web.1",
			desc:     "最后部分是数字但不是第二部分的应该保持不变",
		},
		{
			input:    "",
			expected: "",
			desc:     "空字符串应该返回空",
		},
		{
			input:    "  ",
			expected: "",
			desc:     "只有空格应该返回空",
		},
		{
			input:    "simple-name-no-dots",
			expected: "simple-name-no-dots",
			desc:     "没有点的名称应该保持不变",
		},
	}

	for _, tt := range tests {
		t.Run(tt.desc, func(t *testing.T) {
			result := simplifyInstanceName(tt.input)
			if result != tt.expected {
				t.Errorf("simplifyInstanceName(%q) = %q, expected %q",
					tt.input, result, tt.expected)
			}
		})
	}
}

func TestPreferredContainerName(t *testing.T) {
	tests := []struct {
		labels   map[string]string
		fallback string
		expected string
		desc     string
	}{
		{
			labels: map[string]string{
				"com.docker.compose.service": "nginx",
			},
			fallback: "project_nginx_1",
			expected: "nginx",
			desc:     "应该优先使用 compose service 名称",
		},
		{
			labels: map[string]string{
				"com.docker.compose.project": "demo",
				"com.docker.compose.service": "nginx",
			},
			fallback: "demo-nginx-1",
			expected: "demo.nginx",
			desc:     "compose 项目名与服务名应该组合返回",
		},
		{
			labels: map[string]string{
				"com.docker.swarm.service.name": "web",
			},
			fallback: "web.1.abc123",
			expected: "web",
			desc:     "应该优先使用 swarm service 名称",
		},
		{
			labels: map[string]string{
				"com.docker.swarm.service.name": "api",
				"com.docker.compose.service":    "backend",
			},
			fallback: "random_name",
			expected: "api",
			desc:     "swarm 名称优先级高于 compose",
		},
		{
			labels:   map[string]string{},
			fallback: "my-container",
			expected: "my-container",
			desc:     "没有标签时应该使用 fallback",
		},
		{
			labels:   map[string]string{},
			fallback: "/my-container",
			expected: "my-container",
			desc:     "fallback 带斜杠应该被去除",
		},
		{
			labels:   map[string]string{},
			fallback: "app.prod.db",
			expected: "app.prod.db",
			desc:     "docker compose 的容器名格式应该保持不变",
		},
		{
			labels: map[string]string{
				"com.docker.compose.service": "",
			},
			fallback: "project_web_1",
			expected: "project_web_1",
			desc:     "标签为空时应该使用 fallback",
		},
	}

	for _, tt := range tests {
		t.Run(tt.desc, func(t *testing.T) {
			result := preferredContainerName(tt.labels, tt.fallback)
			if result != tt.expected {
				t.Errorf("preferredContainerName(labels, %q) = %q, expected %q",
					tt.fallback, result, tt.expected)
			}
		})
	}
}

func TestIsAllDigits(t *testing.T) {
	tests := []struct {
		input    string
		expected bool
	}{
		{"123", true},
		{"0", true},
		{"1a", false},
		{"a1", false},
		{"", false},
		{"12.34", false},
		{"  ", false},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := isAllDigits(tt.input)
			if result != tt.expected {
				t.Errorf("isAllDigits(%q) = %v, expected %v",
					tt.input, result, tt.expected)
			}
		})
	}
}
