// tools/stress 压测客户端（stress-plan.md §4.1）。
//
// 三模式：
//
//	hold       —— 连接保持：N 连接限速分批建立、周期心跳，报告存活/断开
//	storm      —— 单向风暴：配对连接互发带纳秒戳的消息，收端算单向延迟分位数
//	storm-echo —— 往返风暴：收端立即回显，发端算 RTT/2，交叉验证单向值
//
// 产出（-out 目录）：
//
//	summary.csv —— 10s 周期快照（ts,connected,sent,recv,failed,latP50/P95/P99ms）
//	report.json —— 结束汇总：计数、分位数、投递对账（丢失/重复/乱序）
//
// 诚实边界（stress-plan.md §0）：本工具产出客观数据；容量结论必须配合
// collect.sh 资源采集与 kernel-tuning.md 记录模板，在真实调优后的机器上得出。
package main

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/gorilla/websocket"
)

var (
	mode     = flag.String("mode", "hold", "hold | storm | storm-echo")
	gateway  = flag.String("gateway", "http://127.0.0.1:37001", "gateway base url")
	seed     = flag.String("seed", "stress", "账号前缀（多机压测时每机唯一）")
	users    = flag.Int("users", 100, "连接数（storm 模式取偶数配对）")
	batch    = flag.Int("batch", 50, "分批建立：每批数量")
	batchDel = flag.Duration("batch-delay", 2*time.Second, "批间隔")
	rate     = flag.Int("rate", 50, "storm：全局发送速率 msg/s；hold/建连阶段：每秒新建连接数上限")
	duration = flag.Duration("duration", 5*time.Minute, "持续时间")
	wsOver   = flag.String("ws-override", "", "强制直连该 WS 地址（纯净单点压测用），空=用登录返回的 ws_addr")
	outDir   = flag.String("out", "", "结果目录（空=不落盘，仅控制台）")
)

// ---------- 延迟直方图：10ms 桶，上限 30s（3 万桶 int64，轻量无依赖） ----------
const latMaxIdx = 3000 // 30s / 10ms

type histogram struct {
	mu    sync.Mutex
	bins  []int64
	total int64
}

func newHistogram() *histogram { return &histogram{bins: make([]int64, latMaxIdx+1)} }

func (h *histogram) add(d time.Duration) {
	idx := int(d / (10 * time.Millisecond))
	if idx > latMaxIdx {
		idx = latMaxIdx
	}
	h.mu.Lock()
	h.bins[idx]++
	h.total++
	h.mu.Unlock()
}

// quantile 返回 q∈[0,1] 分位数（10ms 桶粒度）。
func (h *histogram) quantile(q float64) time.Duration {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.total == 0 {
		return -1
	}
	target := int64(q*float64(h.total)) + 1
	var acc int64
	for i, c := range h.bins {
		acc += c
		if acc >= target {
			return time.Duration(i) * 10 * time.Millisecond
		}
	}
	return latMaxIdx * 10 * time.Millisecond
}

// ---------- 投递对账：cli_msg_id 集合 ----------
type ledger struct {
	mu      sync.Mutex
	sent    map[string]bool // 已发送（发端记录）
	recv    map[string]int  // 实收次数（>1 即重复）
	ooo     int64           // 乱序计数（按会话 seq 回退）
	lastSeq map[string]int64
}

func newLedger() *ledger {
	return &ledger{sent: map[string]bool{}, recv: map[string]int{}, lastSeq: map[string]int64{}}
}

// write 串行化该连接的所有写（gorilla 限制）。
func (c *wsClient) write(msgType int, data []byte) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	return c.conn.WriteMessage(msgType, data)
}

type wsClient struct {
	conn    *websocket.Conn
	uid     string
	writeMu sync.Mutex // gorilla 连接写不并发安全：心跳/风暴/回显统一走 write()
	// storm-echo：收到带 echo 标记的消息时回显
	pair *wsClient
	// 最近一次发送时刻（纳秒，atomic）：ack 到达时近似 ack 延迟下界。
	// 风暴下同连接消息可能交叉，口径为"最近一条"，report.json 已注明
	lastSendNano atomic.Int64
}

var (
	clientsMu                                  sync.Mutex
	clients                                    []*wsClient
	connected, dropped, sent, recv, sendFailed atomic.Int64
	sendLat                                    = newHistogram() // 单向（storm：收端视角）
	rttLat                                     = newHistogram() // 往返（storm-echo：发端视角）
	ackLat                                     = newHistogram() // 发送方 msg→ack
	led                                        = newLedger()
)

// 消息载荷：text 内嵌发送纳秒戳与 msg_id；echo 标记回显时保留原始戳
type stormPayload struct {
	Cmd  string `json:"cmd"`
	Data struct {
		ToUID    string          `json:"to_uid"`
		CliMsgID string          `json:"cli_msg_id"`
		Content  json.RawMessage `json:"content"`
	} `json:"data"`
}

func main() {
	flag.Parse()
	fmt.Printf("stress: mode=%s users=%d rate=%d duration=%s seed=%s out=%s\n",
		*mode, *users, *rate, *duration, *seed, *outDir)

	var csvFile *os.File
	var csvw *csv.Writer
	if *outDir != "" {
		if err := os.MkdirAll(*outDir, 0o755); err != nil {
			fatal("mkdir out: %v", err)
		}
		f, err := os.Create(filepath.Join(*outDir, "summary.csv"))
		if err != nil {
			fatal("create csv: %v", err)
		}
		csvFile, csvw = f, csv.NewWriter(f)
		csvw.Write([]string{"ts", "connected", "sent", "recv", "send_failed", "dropped", "latency_p50_ms", "latency_p95_ms", "latency_p99_ms"})
		defer func() { csvw.Flush(); csvFile.Close() }()
	}

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)

	// ---- 建连（限速：token bucket，rate conn/s；batch 批间隔控制突发） ----
	if *mode != "hold" && *users%2 != 0 {
		*users++ // 配对需要偶数
		fmt.Printf("storm needs even users, bumped to %d\n", *users)
	}
	connectTicker := time.NewTicker(time.Second / time.Duration(max(*rate, 1)))
	defer connectTicker.Stop()
	for i := 0; i < *users; i++ {
		if i > 0 && i%*batch == 0 {
			fmt.Printf("batch: %d/%d connected (failed=%d)\n", len(clients), *users, sendFailed.Load())
			time.Sleep(*batchDel)
		}
		<-connectTicker.C // 限速
		name := fmt.Sprintf("%s%d", *seed, i)
		uid, wsAddr, err := registerAndLogin(name)
		if err != nil {
			sendFailed.Add(1)
			continue
		}
		if *wsOver != "" {
			wsAddr = *wsOver
		}
		conn, _, err := websocket.DefaultDialer.Dial("ws://"+wsAddr+"/ws/"+uid, nil)
		if err != nil {
			sendFailed.Add(1)
			continue
		}
		c := &wsClient{conn: conn, uid: uid}
		clientsMu.Lock()
		clients = append(clients, c)
		// storm 配对：偶数下标与下一位互为 pair
		if *mode != "hold" && len(clients)%2 == 0 {
			c.pair = clients[len(clients)-2]
			clients[len(clients)-2].pair = c
		}
		clientsMu.Unlock()
		connected.Add(1)
		go readLoop(c)
	}
	fmt.Printf("connect phase done: connected=%d failed=%d\n", connected.Load(), sendFailed.Load())

	// ---- 心跳（每连接 30s，D14 客户端模式；分片错峰） ----
	runStop := make(chan struct{})
	var wg sync.WaitGroup
	go func() {
		t := time.NewTicker(30 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-runStop:
				return
			case <-t.C:
				clientsMu.Lock()
				for _, c := range clients {
					_ = c.write(websocket.TextMessage, []byte(`{"cmd":"heartbeat","data":{}}`))
				}
				clientsMu.Unlock()
			}
		}
	}()

	// ---- storm / storm-echo：全局限速发送 ----
	if *mode != "hold" && len(clients) >= 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			interval := time.Second / time.Duration(max(*rate, 1))
			t := time.NewTicker(interval)
			defer t.Stop()
			i := 0
			for {
				select {
				case <-runStop:
					return
				case <-t.C:
					clientsMu.Lock()
					if len(clients) < 2 {
						clientsMu.Unlock()
						continue
					}
					from := clients[i%len(clients)]
					clientsMu.Unlock()
					if from.pair == nil {
						i++
						continue
					}
					id := fmt.Sprintf("%s-%d-%d", *seed, os.Getpid(), i)
					// 单向延迟：纳秒戳随 content 下发；同 worker 时钟同源，收端直接差值
					content := fmt.Sprintf(`{"text":"s","ts":%d,"mid":%q}`, time.Now().UnixNano(), id)
					frame := fmt.Sprintf(`{"cmd":"msg","data":{"to_uid":%q,"cli_msg_id":%q,"content":%s}}`,
						from.pair.uid, id, content)
					led.mu.Lock()
					led.sent[id] = true
					led.mu.Unlock()
					if from.write(websocket.TextMessage, []byte(frame)) == nil {
						sent.Add(1)
						from.lastSendNano.Store(time.Now().UnixNano())
					} else {
						sendFailed.Add(1)
					}
					i++
				}
			}
		}()
	}

	// ---- 周期报告 + CSV ----
	go func() {
		t := time.NewTicker(10 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-runStop:
				return
			case <-t.C:
				line := fmt.Sprintf("[report] connected=%d sent=%d recv=%d sendFailed=%d dropped=%d latP50=%s latP95=%s latP99=%s",
					connected.Load(), sent.Load(), recv.Load(), sendFailed.Load(), dropped.Load(),
					msStr(sendLat.quantile(0.5)), msStr(sendLat.quantile(0.95)), msStr(sendLat.quantile(0.99)))
				fmt.Println(line)
				if csvw != nil {
					csvw.Write([]string{
						time.Now().Format(time.RFC3339),
						fmt.Sprint(connected.Load()), fmt.Sprint(sent.Load()), fmt.Sprint(recv.Load()),
						fmt.Sprint(sendFailed.Load()), fmt.Sprint(dropped.Load()),
						msStr(sendLat.quantile(0.5)), msStr(sendLat.quantile(0.95)), msStr(sendLat.quantile(0.99)),
					})
					csvw.Flush()
				}
			}
		}
	}()

	select {
	case <-stop:
		fmt.Println("interrupted")
	case <-time.After(*duration):
		fmt.Println("duration reached")
	}
	close(runStop)
	// 在途宽限：停止发送后给 2s 让链路上的消息落账，lost 口号=真丢而非"未及收到"
	time.Sleep(2 * time.Second)
	writeReport()
}

// readLoop 收帧：msg 计延迟/对账（回显 echo），ack 计发送方延迟。
func readLoop(c *wsClient) {
	for {
		_, raw, err := c.conn.ReadMessage()
		if err != nil {
			dropped.Add(1)
			return
		}
		var frame struct {
			Cmd  string `json:"cmd"`
			Code int    `json:"code"`
			Data struct {
				CliMsgID string `json:"cli_msg_id"`
				ConvID   string `json:"conv_id"`
				Seq      int64  `json:"seq"`
				Content  struct {
					Text string `json:"text"`
					Ts   int64  `json:"ts"`
					Mid  string `json:"mid"`
				} `json:"content"`
			} `json:"data"`
		}
		if json.Unmarshal(raw, &frame) != nil || frame.Cmd == "" {
			continue // 受理回执等
		}
		switch frame.Cmd {
		case "msg":
			recv.Add(1)
			// 单向延迟：内容里的 ts 是发送方时钟（同 worker 同源）。
			// 只统计原始消息（text=s）：echo 帧（text=e）携带的是历史戳，计入会
			// 产生"延迟越滚越大"的假象（冒烟实测踩坑：回显被再次回显形成循环雪崩）
			if frame.Data.Content.Ts > 0 && frame.Data.Content.Text == "s" {
				sendLat.add(time.Since(time.Unix(0, frame.Data.Content.Ts)))
			}
			mid := frame.Data.Content.Mid
			// 对账/有序性只认原始帧（text=s）：echo 帧复用同一 mid，计入会污染
			// lost/dup/ooo 统计（dup 天然翻倍不是想要测的"投重复"）
			if mid != "" && frame.Data.Content.Text == "s" {
				led.mu.Lock()
				led.recv[mid]++
				if frame.Data.Seq > 0 {
					if last, ok := led.lastSeq[frame.Data.ConvID]; ok && frame.Data.Seq <= last {
						led.ooo++
					}
					led.lastSeq[frame.Data.ConvID] = frame.Data.Seq
				}
				led.mu.Unlock()
			}
			// storm-echo：仅原始消息回显一次（text s→e），echo 不再回显，杜绝循环
			if *mode == "storm-echo" && frame.Data.Content.Text == "s" && c.pair != nil && mid != "" {
				echo := fmt.Sprintf(`{"cmd":"msg","data":{"to_uid":%q,"cli_msg_id":"e-%s","content":{"text":"e","ts":%d,"mid":"%s"}}}`,
					c.pair.uid, mid, frame.Data.Content.Ts, mid)
				_ = c.write(websocket.TextMessage, []byte(echo))
			}
		case "ack":
			// 发送方收到 ack：与该连接最近一次发送时刻作差（近似下界，口径见 report 注）
			if n := c.lastSendNano.Load(); n > 0 {
				ackLat.add(time.Since(time.Unix(0, n)))
			}
		}
	}
}

// writeReport 结束汇总：计数 + 分位数 + 对账。
func writeReport() {
	led.mu.Lock()
	lost, dup := 0, 0
	for id := range led.sent {
		if led.recv[id] == 0 {
			lost++
		}
	}
	for _, n := range led.recv {
		if n > 1 {
			dup++
		}
	}
	oSnow := led.ooo
	led.mu.Unlock()

	rep := map[string]interface{}{
		"mode": *mode, "gateway": *gateway, "seed": *seed,
		"users_target": *users, "rate_limit": *rate, "duration_s": duration.Seconds(),
		"connected": connected.Load(), "send_failed": sendFailed.Load(), "dropped": dropped.Load(),
		"msg_sent": sent.Load(), "msg_recv_total": recv.Load(),
		"ledger": map[string]interface{}{
			"sent_ids": len(led.sent), "lost": lost, "received_dup": dup, "out_of_order": oSnow,
			"note": "storm-echo 模式 recv 计数含回显帧（原始 mid 复用），dup 翻倍为预期；单向延迟以收端视角为准",
		},
		"latency": map[string]interface{}{
			"one_way_p50_ms": ms(sendLat.quantile(0.5)), "one_way_p95_ms": ms(sendLat.quantile(0.95)),
			"one_way_p99_ms": ms(sendLat.quantile(0.99)), "one_way_max_ms": ms(sendLat.quantile(1.0)),
			"ack_p50_ms": ms(ackLat.quantile(0.5)), "ack_p99_ms": ms(ackLat.quantile(0.99)),
			"bucket_ms": 10,
		},
		"finished_at": time.Now().Format(time.RFC3339),
	}
	raw, _ := json.MarshalIndent(rep, "", "  ")
	fmt.Println(string(raw))
	if *outDir != "" {
		_ = os.WriteFile(filepath.Join(*outDir, "report.json"), raw, 0o644)
	}
}

func ms(d time.Duration) interface{} {
	if d < 0 {
		return nil
	}
	return d.Milliseconds()
}
func msStr(d time.Duration) string {
	if d < 0 {
		return "na"
	}
	return fmt.Sprint(d.Milliseconds())
}
func fatal(f string, a ...interface{}) { fmt.Printf("stress fatal: "+f+"\n", a...); os.Exit(1) }
func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// registerAndLogin 注册（幂等：已注册则忽略）+ 登录拿 ws_addr。
func registerAndLogin(name string) (uid, wsAddr string, err error) {
	body, _ := json.Marshal(map[string]string{"user_name": name, "email": name + "@stress.local", "password": "Stress123"})
	if resp, err := http.Post(*gateway+"/v1/register", "application/json", bytes.NewReader(body)); err == nil {
		resp.Body.Close() // 409 已注册视为成功
	}
	lb, _ := json.Marshal(map[string]string{"user_name": name, "password": "Stress123"})
	resp, err := http.Post(*gateway+"/v1/login", "application/json", bytes.NewReader(lb))
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
