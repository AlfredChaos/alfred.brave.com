// 端到端验收脚本（T08）：注册→登录→拿 ws_addr→双用户连不同 CS→跨节点收发→ACK→
// 历史落库→重放不重复。用法：go run ./tools/e2e [-gateway http://127.0.0.1:37001]
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/gorilla/websocket"
)

var gateway = flag.String("gateway", "http://127.0.0.1:37001", "gateway base url")

type loginResp struct {
	UID    string `json:"uid"`
	Token  string `json:"token"`
	WsAddr string `json:"ws_addr"`
}

type convResp struct {
	ConvID  string   `json:"conv_id"`
	Type    string   `json:"type"`
	Members []string `json:"members"`
}

type histResp struct {
	Messages []struct {
		MsgID  string          `json:"msg_id"`
		Seq    int64           `json:"seq"`
		ConvID string          `json:"conv_id"`
		From   string          `json:"from_uid"`
		Cont   json.RawMessage `json:"content"`
	} `json:"messages"`
}

func post(path string, body interface{}, token string, out interface{}) error {
	raw, _ := json.Marshal(body)
	req, _ := http.NewRequest(http.MethodPost, *gateway+path, bytes.NewReader(raw))
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return fmt.Errorf("POST %s -> %d", path, resp.StatusCode)
	}
	if out != nil {
		return json.NewDecoder(resp.Body).Decode(out)
	}
	return nil
}

func get(path, token string, out interface{}) error {
	req, _ := http.NewRequest(http.MethodGet, *gateway+path, nil)
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return fmt.Errorf("GET %s -> %d", path, resp.StatusCode)
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

func main() {
	flag.Parse()
	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	userA := "e2ea" + suffix
	userB := "e2eb" + suffix

	step("register two users")
	must(post("/v1/register", map[string]string{"user_name": userA, "email": userA + "@e2e.io", "password": "passw0rd1"}, "", nil))
	must(post("/v1/register", map[string]string{"user_name": userB, "email": userB + "@e2e.io", "password": "passw0rd1"}, "", nil))

	step("login A (get ws_addr + token)")
	var la loginResp
	must(post("/v1/login", map[string]string{"user_name": userA, "password": "passw0rd1"}, "", &la))
	if la.Token == "" || la.WsAddr == "" {
		fatalf("login A missing token/ws_addr: %+v", la)
	}

	// 反复登录 B 直到与 A 不同 CS（网关随机选点；本地双节点几次内必中）
	step("login B until different chat server")
	var lb loginResp
	for i := 0; i < 20; i++ {
		must(post("/v1/login", map[string]string{"user_name": userB, "password": "passw0rd1"}, "", &lb))
		if lb.WsAddr != la.WsAddr {
			break
		}
	}
	if lb.WsAddr == la.WsAddr {
		fatalf("B never landed on a different CS than A (%s)", la.WsAddr)
	}
	fmt.Printf("  A -> %s, B -> %s (cross-CS verified)\n", la.WsAddr, lb.WsAddr)

	step("open websockets to different chat servers")
	connA := dialWS(la.WsAddr, la.UID)
	defer connA.Close()
	connB := dialWS(lb.WsAddr, lb.UID)
	defer connB.Close()

	step("A creates conversation with B")
	var conv convResp
	must(post("/v1/conversations", map[string]string{"to_uid": lb.UID}, la.Token, &conv))
	if conv.ConvID == "" || len(conv.Members) != 2 {
		fatalf("conversation create bad resp: %+v", conv)
	}

	cliMsgID := "e2e-msg-1"
	step("A sends message (cli_msg_id=%s)", cliMsgID)
	sendFrame(connA, map[string]interface{}{
		"cmd": "msg",
		"data": map[string]interface{}{
			"conv_id":    conv.ConvID,
			"to_uid":     lb.UID,
			"cli_msg_id": cliMsgID,
			"content":    map[string]string{"text": "hello cross-cs"},
		},
	})

	step("B receives message frame in real time")
	msg := readFrame(connB, 15*time.Second)
	if msg["cmd"] != "msg" {
		fatalf("B expected msg frame, got %v", msg["cmd"])
	}
	data := msg["data"].(map[string]interface{})
	if data["cli_msg_id"] != cliMsgID {
		fatalf("B got wrong cli_msg_id: %v", data["cli_msg_id"])
	}
	fmt.Printf("  B received: conv=%s seq=%v from=%s\n", data["conv_id"], data["seq"], data["from_uid"])

	step("A receives ack (delivered)")
	ack := readFrame(connA, 15*time.Second)
	if ack["cmd"] != "ack" {
		fatalf("A expected ack frame, got %v", ack["cmd"])
	}
	ackData := ack["data"].(map[string]interface{})
	if ackData["cli_msg_id"] != cliMsgID {
		fatalf("A ack wrong cli_msg_id: %v", ackData["cli_msg_id"])
	}
	fmt.Printf("  A ack: msg_id=%s seq=%v\n", ackData["msg_id"], ackData["seq"])

	step("replay same cli_msg_id (idempotency)")
	sendFrame(connA, map[string]interface{}{
		"cmd": "msg",
		"data": map[string]interface{}{
			"conv_id":    conv.ConvID,
			"to_uid":     lb.UID,
			"cli_msg_id": cliMsgID,
			"content":    map[string]string{"text": "hello cross-cs"},
		},
	})
	// at-least-once：B 可能再收到一帧重复投递（客户端按 msg_id 幂等丢弃）
	drainFrames(connB, 5*time.Second)

	step("history shows exactly one row (replay not duplicated)")
	var hist histResp
	must(get("/v1/conversations/"+conv.ConvID+"/messages?after_seq=0&limit=50", la.Token, &hist))
	if len(hist.Messages) != 1 {
		fatalf("history rows = %d, want 1 (replay must not duplicate)", len(hist.Messages))
	}
	if hist.Messages[0].Seq != 1 || hist.Messages[0].From != la.UID {
		fatalf("stored message mismatch: %+v", hist.Messages[0])
	}
	fmt.Printf("  PG row: msg_id=%s seq=%d content=%s\n", hist.Messages[0].MsgID, hist.Messages[0].Seq, string(hist.Messages[0].Cont))

	fmt.Println("E2E PASS: register/login/ws_addr/cross-cs deliver/ack/persist/replay-idempotent")
}

func step(format string, args ...interface{}) {
	fmt.Printf("== %s\n", fmt.Sprintf(format, args...))
}

func must(err error) {
	if err != nil {
		fatalf("%v", err)
	}
}

func fatalf(format string, args ...interface{}) {
	fmt.Printf("E2E FAIL: "+format+"\n", args...)
	os.Exit(1)
}

func dialWS(addr, uid string) *websocket.Conn {
	url := "ws://" + addr + "/ws/" + uid
	conn, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		fatalf("dial %s: %v", url, err)
	}
	return conn
}

func sendFrame(conn *websocket.Conn, frame interface{}) {
	raw, _ := json.Marshal(frame)
	if err := conn.WriteMessage(websocket.TextMessage, raw); err != nil {
		fatalf("send: %v", err)
	}
}

// readFrame 阻塞读一帧目标帧（cmd=msg/ack）。
// gorilla 语义：ReadMessage 一旦返回错误（含 deadline），读侧即失败、再次读会 panic——
// 因此不做"出错后重试"循环，一次读拿满整个超时窗口；仅对成功读到的非目标帧（受理回执）跳过续读。
func readFrame(conn *websocket.Conn, timeout time.Duration) map[string]interface{} {
	deadline := time.Now().Add(timeout)
	for {
		conn.SetReadDeadline(deadline)
		_, raw, err := conn.ReadMessage()
		if err != nil {
			fatalf("read frame: %v (no target frame within %s)", err, timeout)
		}
		var frame struct {
			Cmd  string                 `json:"cmd"`
			Code *uint32                `json:"code"`
			Data map[string]interface{} `json:"data"`
		}
		if json.Unmarshal(raw, &frame) != nil || frame.Cmd == "" {
			continue // 受理回执 {code:200} 等非目标帧：连接仍健康，续读
		}
		return map[string]interface{}{"cmd": frame.Cmd, "data": frame.Data}
	}
}

func drainFrames(conn *websocket.Conn, window time.Duration) {
	deadline := time.Now().Add(window)
	for time.Now().Before(deadline) {
		conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
		_, raw, err := conn.ReadMessage()
		if err != nil {
			return
		}
		if !strings.Contains(string(raw), `"cmd":"msg"`) {
			continue
		}
		fmt.Printf("  (at-least-once duplicate push seen: %s)\n", raw)
	}
}
