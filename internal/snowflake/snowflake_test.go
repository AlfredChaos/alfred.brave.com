package snowflake

import (
	"strconv"
	"sync"
	"testing"
)

// TestNextUniqueAndIncreasing 唯一性 + 趋势递增（D16 的两个承诺）。
func TestNextUniqueAndIncreasing(t *testing.T) {
	g, _ := New(1)
	seen := make(map[string]bool)
	var prev int64
	for i := 0; i < 10000; i++ {
		idStr, err := g.Next()
		if err != nil {
			t.Fatalf("next: %v", err)
		}
		if seen[idStr] {
			t.Fatalf("duplicate id %s at %d", idStr, i)
		}
		seen[idStr] = true
		id, err := strconv.ParseInt(idStr, 10, 64)
		if err != nil {
			t.Fatalf("parse %s: %v", idStr, err)
		}
		if id <= prev {
			t.Fatalf("id not increasing: %d <= %d", id, prev)
		}
		prev = id
	}
}

// TestConcurrentUnique 并发 50 goroutine 各 200 个，全局无重复。
func TestConcurrentUnique(t *testing.T) {
	g, _ := New(2)
	var mu sync.Mutex
	seen := make(map[string]bool)
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			local := make([]string, 0, 200)
			for j := 0; j < 200; j++ {
				id, err := g.Next()
				if err != nil {
					t.Errorf("next: %v", err)
					return
				}
				local = append(local, id)
			}
			mu.Lock()
			defer mu.Unlock()
			for _, id := range local {
				if seen[id] {
					t.Errorf("duplicate id %s", id)
					return
				}
				seen[id] = true
			}
		}()
	}
	wg.Wait()
}

// TestWorkerIDRange 非法 workerID 拒绝。
func TestWorkerIDRange(t *testing.T) {
	if _, err := New(-1); err == nil {
		t.Fatal("negative worker id must be rejected")
	}
	if _, err := New(8); err == nil {
		t.Fatal("worker id > 7 must be rejected")
	}
}
