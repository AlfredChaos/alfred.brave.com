package exchange

import (
	"encoding/json"
	"testing"
	"time"
)

// readResponse 从 Send 通道读出一条响应并解析，超时 fail。
func readResponse(t *testing.T, c *Client) MessageResponse {
	t.Helper()
	select {
	case raw := <-c.Send:
		var resp MessageResponse
		if err := json.Unmarshal(raw, &resp); err != nil {
			t.Fatalf("unmarshal response %s: %v", raw, err)
		}
		return resp
	case <-time.After(time.Second):
		t.Fatal("no response in Send chan")
		return MessageResponse{}
	}
}

// TestPoccessMessageDispatch cmd 分发：heartbeat 正常应答、未知 cmd 拒绝。
func TestPoccessMessageDispatch(t *testing.T) {
	c := NewClient(NewManager(), "u1", nil)

	// heartbeat 帧应答 200
	poccessMessage(c, []byte(`{"cmd":"heartbeat","data":{}}`))
	if resp := readResponse(t, c); resp.Code != OK {
		t.Fatalf("heartbeat resp code = %d, want %d", resp.Code, OK)
	}

	// 未知 cmd 应答参数不合法（1001），不能静默吞掉
	poccessMessage(c, []byte(`{"cmd":"unknown","data":{}}`))
	if resp := readResponse(t, c); resp.Code != ParameterIllegal {
		t.Fatalf("unknown cmd resp code = %d, want %d", resp.Code, ParameterIllegal)
	}

	// 非法 JSON 应答参数不合法
	poccessMessage(c, []byte(`not-json`))
	if resp := readResponse(t, c); resp.Code != ParameterIllegal {
		t.Fatalf("bad json resp code = %d, want %d", resp.Code, ParameterIllegal)
	}
}

// TestRegisterCmd 注册式路由：新注册的 cmd 走新 handler，不改分发主逻辑。
func TestRegisterCmd(t *testing.T) {
	called := false
	RegisterCmd("test-cmd", func(c *Client, data json.RawMessage) {
		called = true
		c.SendResponse(OK, "", nil)
	})
	c := NewClient(NewManager(), "u1", nil)
	poccessMessage(c, []byte(`{"cmd":"test-cmd"}`))
	if !called {
		t.Fatal("registered handler not called")
	}
	readResponse(t, c)
}
