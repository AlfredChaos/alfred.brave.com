// tools/stress 压测客户端骨架（dev-task §3：只搭骨架不执行，不产出任何性能数字）。
//
// 两种模式（参考 kernel-tuning.md 的压测→调优→记录闭环）：
//
//	hold  —— 连接保持：N 连接分批建立、周期心跳、到时后报告存活/断开数与进程指标
//	storm —— 消息风暴：M 在线连接互相向固定会话发消息，报告发送/接收/失败计数
//
// 用法示例（本地栈）：
//
//	go run ./tools/stress -mode hold  -gateway http://127.0.0.1:37001 -users 2000 -batch 200 -duration 10m
//	go run ./tools/stress -mode storm -gateway http://127.0.0.1:37001 -users 200 -rate 100 -duration 5m
//
// 前置：批量注册账号由 -seed 生成（register 前缀 + 序号），密码固定 Stress123。
// 输出：仅结构性计数（连接数/心跳/收发/失败），性能结论必须配合 kernel-tuning.md
// 的资源采集模板（CPU/内存/FD/goroutine/lag）人工记录——本工具不生成"性能数字"。
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/gorilla/websocket"
)

var (
	mode     = flag.String("mode", "hold", "hold | storm")
	gateway  = flag.String("gateway", "http://127.0.0.1:37001", "gateway base url")
	seed     = flag.String("seed", "stress", "账号前缀（批量注册 register）")
	users    = flag.Int("users", 100, "连接数")
	batch    = flag.Int("batch", 50, "分批建立：每批数量（批间隔 batch-delay）")
	batchDel = flag.Duration("batch-delay", 2*time.Second, "批间隔")
	duration = flag.Duration("duration", 5*time.Minute, "持续时间")
	rate     = flag.Int("rate", 50, "storm 模式：全局发送速率 msg/s")
)

type wsClient struct {
	conn *websocket.Conn
	uid  string
}

func main() {
	flag.Parse()
	fmt.Printf("stress skeleton: mode=%s users=%d duration=%s\n", *mode, *users, *duration)
	fmt.Println("NOTE: structural counters only; record CPU/mem/FD per kernel-tuning.md manually.")

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	deadline := time.After(*duration)

	var clients []*wsClient
	var mu sync.Mutex
	var connected, dropped, sent, recv, failed atomic.Int64

	// 注册并建立 N 个连接（分批）
	for i := 0; i < *users; i += *batch {
		end := i + *batch
		if end > *users {
			end = *users
		}
		for j := i; j < end; j++ {
			name := fmt.Sprintf("%s%d", *seed, j)
			uid, wsAddr, err := registerAndLogin(name)
			if err != nil {
				failed.Add(1)
				continue
			}
			conn, _, err := websocket.DefaultDialer.Dial("ws://"+wsAddr+"/ws/"+uid, nil)
			if err != nil {
				failed.Add(1)
				continue
			}
			mu.Lock()
			clients = append(clients, &wsClient{conn: conn, uid: uid})
			mu.Unlock()
			connected.Add(1)
			// 读循环（丢弃 + 计数）
			go func(c *websocket.Conn) {
				for {
					if _, _, err := c.ReadMessage(); err != nil {
						dropped.Add(1)
						return
					}
					recv.Add(1)
				}
			}(conn)
		}
		fmt.Printf("batch: %d/%d connected (failed=%d)\n", len(clients), *users, failed.Load())
		time.Sleep(*batchDel)
	}

	// 心跳（每连接 30s，D14 客户端模式）
	heartbeatStop := make(chan struct{})
	go func() {
		t := time.NewTicker(30 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-heartbeatStop:
				return
			case <-t.C:
				mu.Lock()
				for _, c := range clients {
					if c.conn.WriteMessage(websocket.TextMessage, []byte(`{"cmd":"heartbeat","data":{}}`)) == nil {
						sent.Add(1)
					}
				}
				mu.Unlock()
			}
		}
	}()
	defer close(heartbeatStop)

	// storm 模式：全局速率向第 0 个用户发消息（单聊风暴）
	if *mode == "storm" && len(clients) >= 2 {
		go func() {
			interval := time.Second / time.Duration(*rate)
			t := time.NewTicker(interval)
			defer t.Stop()
			i := 0
			for {
				select {
				case <-heartbeatStop:
					return
				case <-t.C:
					mu.Lock()
					from := clients[i%len(clients)]
					to := clients[(i+1)%len(clients)]
					mu.Unlock()
					frame := fmt.Sprintf(`{"cmd":"msg","data":{"to_uid":%q,"cli_msg_id":"s-%d","content":{"text":"hi"}}}`, to.uid, i)
					if from.conn.WriteMessage(websocket.TextMessage, []byte(frame)) == nil {
						sent.Add(1)
					} else {
						failed.Add(1)
					}
					i++
				}
			}
		}()
	}

	// 周期报告
	reportStop := make(chan struct{})
	go func() {
		t := time.NewTicker(10 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-reportStop:
				return
			case <-t.C:
				fmt.Printf("[report] connected=%d dropped=%d sent=%d recv=%d failed=%d\n",
					connected.Load(), dropped.Load(), sent.Load(), recv.Load(), failed.Load())
			}
		}
	}()
	defer close(reportStop)

	select {
	case <-stop:
		fmt.Println("interrupted")
	case <-deadline:
		fmt.Println("duration reached")
	}
	fmt.Printf("[final] connected=%d dropped=%d sent=%d recv=%d failed=%d\n",
		connected.Load(), dropped.Load(), sent.Load(), recv.Load(), failed.Load())
}

// registerAndLogin 注册（幂等：已注册则忽略错误）+ 登录拿 ws_addr。
func registerAndLogin(name string) (uid, wsAddr string, err error) {
	body, _ := json.Marshal(map[string]string{"user_name": name, "email": name + "@stress.local", "password": "Stress123"})
	resp, err := http.Post(*gateway+"/v1/register", "application/json", bytes.NewReader(body))
	if err == nil {
		resp.Body.Close()
	}
	lb, _ := json.Marshal(map[string]string{"user_name": name, "password": "Stress123"})
	resp, err = http.Post(*gateway+"/v1/login", "application/json", bytes.NewReader(lb))
	if err != nil {
		return "", "", err
	}
	defer resp.Body.Close()
	var out struct {
		UID    string `json:"uid"`
		WsAddr string `json:"ws_addr"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return "", "", err
	}
	return out.UID, out.WsAddr, nil
}
