//go:build integration

// 集成测试：@ 机制校验链（§5）——persist 权威校验 + 投递 mention 标记展开。
package persist

import (
	"context"
	"testing"

	"alfred.brave.com/internal/chat"
)

// TestMentionAllOwnerOnly @all 权威校验：owner 放行且全员 mention=true；member 拒收。
func TestMentionAllOwnerOnly(t *testing.T) {
	f := newGroupFixture(t)
	ctx := context.Background()

	// member @all → 拒收（整条消息不落库）
	err := f.w.Handle(ctx, &chat.Msg{
		ConvID: f.gid, FromUID: f.m1, Type: chat.TypeGroup, CliMsgID: "ma-1",
		Mentions: nil, MentionAll: true, Content: map[string]string{"text": "@all"},
	})
	if !IsPoison(err) {
		t.Fatalf("member mention_all must be poison, got %v", err)
	}

	// owner @all → 放行；在线成员的 push mention=true（全员标记，§5 投递展开）
	if err := f.w.Handle(ctx, &chat.Msg{
		ConvID: f.gid, FromUID: f.owner, Type: chat.TypeGroup, CliMsgID: "ma-2",
		MentionAll: true, Content: map[string]string{"text": "@所有人 开会"},
	}); err != nil {
		t.Fatalf("owner mention_all: %v", err)
	}
	pushes, _ := f.collector().snapshot()
	found := false
	for _, p := range pushes {
		if p.CliMsgID == "ma-2" && p.ToUID == f.m1 {
			found = true
			if !p.Mention {
				t.Fatal("mention_all push must carry mention=true")
			}
		}
	}
	if !found {
		t.Fatal("owner @all push to online member missing")
	}
}

// TestMentionMembersOnly mentions ⊆ 当前成员：@ 陌生人拒收；@ 成员放行且被 @ 者 mention=true。
func TestMentionMembersOnly(t *testing.T) {
	f := newGroupFixture(t)
	ctx := context.Background()

	// @ 非成员 → 整条拒收
	err := f.w.Handle(ctx, &chat.Msg{
		ConvID: f.gid, FromUID: f.m1, Type: chat.TypeGroup, CliMsgID: "mm-1",
		Mentions: []string{f.outside}, Content: map[string]string{"text": "@stranger"},
	})
	if !IsPoison(err) {
		t.Fatalf("mention non-member must be poison, got %v", err)
	}

	// @ 在线成员 → 放行，被 @ 者 push 带 mention=true
	if err := f.w.Handle(ctx, &chat.Msg{
		ConvID: f.gid, FromUID: f.owner, Type: chat.TypeGroup, CliMsgID: "mm-2",
		Mentions: []string{f.m1}, Content: map[string]string{"text": "@m1 看这个"},
	}); err != nil {
		t.Fatalf("mention member: %v", err)
	}
	pushes, _ := f.collector().snapshot()
	for _, p := range pushes {
		if p.CliMsgID == "mm-2" && p.ToUID == f.m1 && !p.Mention {
			t.Fatal("mentioned member must receive mention=true push")
		}
	}
}

// TestMentionNoMarkWithoutAt 无 @ 的普通群消息 push mention=false。
func TestMentionNoMarkWithoutAt(t *testing.T) {
	f := newGroupFixture(t)
	ctx := context.Background()
	if err := f.w.Handle(ctx, &chat.Msg{
		ConvID: f.gid, FromUID: f.owner, Type: chat.TypeGroup, CliMsgID: "plain",
		Content: map[string]string{"text": "plain"},
	}); err != nil {
		t.Fatalf("plain msg: %v", err)
	}
	pushes, _ := f.collector().snapshot()
	for _, p := range pushes {
		if p.CliMsgID == "plain" && p.Mention {
			t.Fatal("plain message must not carry mention=true")
		}
	}
}
