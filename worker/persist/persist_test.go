package persist

import "testing"

// TestDeriveMsgID 确定性 msg_id：同输入同 ID（重放幂等根基），异输入异 ID。
func TestDeriveMsgID(t *testing.T) {
	a := deriveMsgID("conv-1", "u1", "cli-1")
	b := deriveMsgID("conv-1", "u1", "cli-1")
	if a != b {
		t.Fatalf("same input must derive same id: %s vs %s", a, b)
	}
	if c := deriveMsgID("conv-1", "u1", "cli-2"); c == a {
		t.Fatal("different cli_msg_id must derive different id")
	}
	if c := deriveMsgID("conv-2", "u1", "cli-1"); c == a {
		t.Fatal("different conv must derive different id")
	}
	if deriveMsgID("conv", "u", "") == deriveMsgID("conv", "u", "") {
		// 空 cli_msg_id 退化为随机 ID：两次不同
		t.Log("note: empty cli_msg_id falls back to random uuid")
	}
}
