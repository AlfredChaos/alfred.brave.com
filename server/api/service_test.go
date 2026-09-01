package api

import "testing"

// TestGetServiceByRandom 服务列表为空时必须返回空串而不是 panic
// （回归 AGENTS.md 记录过的 rand.Intn(0) panic，同时验证 rand.Seed 移除后行为不变）。
func TestGetServiceByRandom(t *testing.T) {
	orig := Services
	defer func() { Services = orig }()

	Services = []string{}
	if got := getServiceByRandom(); got != "" {
		t.Fatalf("empty service list: got %q, want empty", got)
	}

	Services = []string{"cs-1", "cs-2"}
	for i := 0; i < 20; i++ {
		got := getServiceByRandom()
		if got != "cs-1" && got != "cs-2" {
			t.Fatalf("got %q, want one of [cs-1 cs-2]", got)
		}
	}
}
