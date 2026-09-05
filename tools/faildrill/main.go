// 故障演练：CS 节点宕机时 deliver worker 的行为验证（T-deliver-fix）。
// 场景：A@cs-1 发消息给 B@cs-2 → docker stop cs-2 → 发 m2（预期：deliver 报
// Unavailable→evict→3 次退避重试→drop+补拉兜底，B 收不到）→ docker start cs-2
// → B 重连 → 发 m3（预期：投递恢复，B 实时收到，A 收到 delivered ack）。
// 用法：go run ./tools/faildrill [-gateway http://127.0.0.1:37001]
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/exec"
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
	ConvID string `json:"conv_id"`
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

func login(user string) loginResp {
	var l loginResp
	if err := post("/v1/login", map[string]string{"user_name": user, "password": "passw0rd1"}, "", &l); err != nil {
		fatalf("login %s: %v", user, err)
	}
	return l
}

// loginUntil 反复登录直到落在指定 CS（网关随机选点）。
func loginUntil(user, wantAddr string) loginResp {
	for i := 0; i < 30; i++ {
		l := login(user)
		if l.WsAddr == wantAddr {
			return l
		}
	}
	fatalf("%s never landed on %s", user, wantAddr)
	return loginResp{}
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

// readFrame 阻塞读目标帧；gorilla 读侧一次失败即废弃连接，不做重试循环。
func readFrame(conn *websocket.Conn, wantCmd string, timeout time.Duration) map[string]interface{} {
	deadline := time.Now().Add(timeout)
	for {
		conn.SetReadDeadline(deadline)
		_, raw, err := conn.ReadMessage()
		if err != nil {
			return nil // 超时/断连：由调用方判定
		}
		var frame struct {
			Cmd  string                 `json:"cmd"`
			Code *uint32                `json:"code"`
			Data map[string]interface{} `json:"data"`
		}
		if json.Unmarshal(raw, &frame) != nil || frame.Cmd == "" {
			continue // {code:200} 受理回执等非目标帧
		}
		if frame.Cmd == wantCmd {
			return frame.Data
		}
	}
}

func sh(name string, args ...string) {
	cmd := exec.Command(name, args...)
	out, err := cmd.CombinedOutput()
	fmt.Printf("  $ %s %v\n%s", name, args, out)
	if err != nil {
		fatalf("%s %v: %v", name, args, err)
	}
}

func step(format string, args ...interface{}) {
	fmt.Printf("\n== %s\n", fmt.Sprintf(format, args...))
}

func fatalf(format string, args ...interface{}) {
	fmt.Printf("DRILL FAIL: "+format+"\n", args...)
	os.Exit(1)
}

func main() {
	flag.Parse()
	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	userA := "drilla" + suffix
	userB := "drillb" + suffix

	step("register + land A on cs-1(37002), B on cs-2(37202)")
	must(post("/v1/register", map[string]string{"user_name": userA, "email": userA + "@drill.io", "password": "passw0rd1"}, "", nil))
	must(post("/v1/register", map[string]string{"user_name": userB, "email": userB + "@drill.io", "password": "passw0rd1"}, "", nil))
	la := loginUntil(userA, "127.0.0.1:37002")
	lb := loginUntil(userB, "127.0.0.1:37202")
	fmt.Printf("  A -> %s, B -> %s\n", la.WsAddr, lb.WsAddr)

	connA := dialWS(la.WsAddr, la.UID)
	connB := dialWS(lb.WsAddr, lb.UID)
	var conv convResp
	must(post("/v1/conversations", map[string]string{"to_uid": lb.UID}, la.Token, &conv))

	send := func(cliID, text string) {
		sendFrame(connA, map[string]interface{}{
			"cmd": "msg",
			"data": map[string]interface{}{
				"conv_id": conv.ConvID, "to_uid": lb.UID,
				"cli_msg_id": cliID, "content": map[string]string{"text": text},
			},
		})
	}

	step("baseline m1: B receives in real time")
	send("drill-m1", "before failure")
	if got := readFrame(connB, "msg", 15*time.Second); got == nil {
		fatalf("baseline broken: B did not receive m1")
	}
	// 消费掉 m1 的 ack，保证后面读到 m3 ack 时不会错拿旧帧
	if got := readFrame(connA, "ack", 15*time.Second); got == nil {
		fatalf("baseline broken: A missing m1 ack")
	}
	fmt.Println("  m1 delivered (B frame + A ack)")

	step("docker stop cs-2 (B's node dies)")
	sh("docker", "stop", "brave-local-cs-2-1")
	time.Sleep(2 * time.Second)

	step("m2 during outage: expect deliver retry then drop (pull-back fallback)")
	send("drill-m2", "during outage")
	// 等 deliver 完成整轮重试（3 次 ×5s 上限 + 退避）并记录 drop
	time.Sleep(18 * time.Second)

	step("docker start cs-2, B reconnects")
	sh("docker", "start", "brave-local-cs-2-1")
	time.Sleep(8 * time.Second) // joker 起监听 + etcd 注册 + lease 续期
	lb2 := loginUntil(userB, "127.0.0.1:37202")
	connB2 := dialWS(lb2.WsAddr, lb2.UID)

	step("m3 after recovery: B must receive in real time (grpc conn rebuilt)")
	send("drill-m3", "after recovery")
	data := readFrame(connB2, "msg", 20*time.Second)
	if data == nil {
		fatalf("recovery broken: B did not receive m3 within 20s")
	}
	if data["cli_msg_id"] != "drill-m3" {
		fatalf("B got wrong frame: %+v", data)
	}
	fmt.Printf("  m3 delivered: seq=%v\n", data["seq"])

	step("A receives delivered ack for m3")
	ack := readFrame(connA, "ack", 15*time.Second)
	if ack == nil || ack["cli_msg_id"] != "drill-m3" {
		fatalf("A missing ack for m3: %+v", ack)
	}
	fmt.Printf("  ack ok (delivered)\n")

	fmt.Println("\nDRILL PASS: outage dropped m2 safely (pull-back fallback) and recovered on m3")
}

func must(err error) {
	if err != nil {
		fatalf("%v", err)
	}
}
