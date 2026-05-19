package resilix_test

import (
	"context"
	"errors"
	"testing"
	"time"

	resilix "github.com/Wembie/Resilix/sdk/go"
	"github.com/Wembie/Resilix/sdk/go/resilixtest"
)

// --- helpers ---

func setup(t *testing.T) (*resilixtest.MockBackend, *resilix.RedisClient) {
	t.Helper()
	mock := resilixtest.NewMockBackend()
	client := resilixtest.NewClient(t, mock)
	return mock, client
}

var ctx = context.Background()

// ============================================================
// KV
// ============================================================

func TestKV_SetGet(t *testing.T) {
	_, client := setup(t)
	if err := client.KV().Set(ctx, "key", "value", 0); err != nil {
		t.Fatal(err)
	}
	got, err := client.KV().Get(ctx, "key")
	if err != nil {
		t.Fatal(err)
	}
	if got != "value" {
		t.Fatalf("got %q want %q", got, "value")
	}
}

func TestKV_GetMissing(t *testing.T) {
	_, client := setup(t)
	_, err := client.KV().Get(ctx, "missing")
	if !errors.Is(err, resilix.ErrKeyNotFound) {
		t.Fatalf("want ErrKeyNotFound, got %v", err)
	}
}

func TestKV_GetValidationError(t *testing.T) {
	_, client := setup(t)
	_, err := client.KV().Get(ctx, "")
	if err == nil {
		t.Fatal("want validation error for empty key")
	}
}

func TestKV_SetWithTTL(t *testing.T) {
	mock, client := setup(t)
	if err := client.KV().Set(ctx, "expiring", "val", time.Millisecond); err != nil {
		t.Fatal(err)
	}
	// Set directly with already-expired time to simulate instant expiry
	mock.SetKVWithTTL("expiring", "val", time.Now().Add(-time.Millisecond))
	_, err := client.KV().Get(ctx, "expiring")
	if !errors.Is(err, resilix.ErrKeyNotFound) {
		t.Fatalf("want ErrKeyNotFound after expiry, got %v", err)
	}
}

func TestKV_MGetMSet(t *testing.T) {
	_, client := setup(t)
	values := map[string]any{"a": "1", "b": "2"}
	if err := client.KV().MSet(ctx, values, 0); err != nil {
		t.Fatal(err)
	}
	got, err := client.KV().MGet(ctx, "a", "b", "missing")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 {
		t.Fatalf("want 3 results, got %d", len(got))
	}
	if got[0] != "1" || got[1] != "2" {
		t.Fatalf("unexpected values: %v", got)
	}
	if got[2] != nil {
		t.Fatalf("want nil for missing key, got %v", got[2])
	}
}

func TestKV_Del(t *testing.T) {
	mock, client := setup(t)
	mock.SetKV("a", "1")
	mock.SetKV("b", "2")
	n, err := client.KV().Del(ctx, "a", "b", "c")
	if err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Fatalf("want 2 deleted, got %d", n)
	}
}

func TestKV_Exists(t *testing.T) {
	mock, client := setup(t)
	mock.SetKV("a", "1")
	n, err := client.KV().Exists(ctx, "a", "missing")
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("want 1, got %d", n)
	}
}

func TestKV_Expire_TTL(t *testing.T) {
	mock, client := setup(t)
	mock.SetKV("k", "v")
	ok, err := client.KV().Expire(ctx, "k", 10*time.Second)
	if err != nil || !ok {
		t.Fatalf("Expire failed: ok=%v err=%v", ok, err)
	}
	ttl, err := client.KV().TTL(ctx, "k")
	if err != nil {
		t.Fatal(err)
	}
	if ttl <= 0 || ttl > 11*time.Second {
		t.Fatalf("unexpected TTL: %v", ttl)
	}
}

func TestKV_IncrDecr(t *testing.T) {
	_, client := setup(t)
	n, err := client.KV().Incr(ctx, "counter")
	if err != nil || n != 1 {
		t.Fatalf("Incr: got %d %v", n, err)
	}
	n, err = client.KV().Incr(ctx, "counter")
	if err != nil || n != 2 {
		t.Fatalf("Incr: got %d %v", n, err)
	}
	n, err = client.KV().Decr(ctx, "counter")
	if err != nil || n != 1 {
		t.Fatalf("Decr: got %d %v", n, err)
	}
}

func TestKV_BackendError(t *testing.T) {
	mock, client := setup(t)
	boom := errors.New("backend failure")
	mock.InjectError("Get", boom)
	_, err := client.KV().Get(ctx, "k")
	if err == nil {
		t.Fatal("want error, got nil")
	}
}

// ============================================================
// Hash
// ============================================================

func TestHash_SetGet(t *testing.T) {
	_, client := setup(t)
	n, err := client.Hash().Set(ctx, "h", map[string]any{"f1": "v1", "f2": "v2"})
	if err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Fatalf("want 2 new fields, got %d", n)
	}
	val, err := client.Hash().Get(ctx, "h", "f1")
	if err != nil || val != "v1" {
		t.Fatalf("get f1: val=%q err=%v", val, err)
	}
}

func TestHash_GetAll(t *testing.T) {
	mock, client := setup(t)
	mock.SetHash("h", map[string]string{"a": "1", "b": "2"})
	all, err := client.Hash().GetAll(ctx, "h")
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 2 || all["a"] != "1" || all["b"] != "2" {
		t.Fatalf("unexpected GetAll: %v", all)
	}
}

func TestHash_MGet(t *testing.T) {
	mock, client := setup(t)
	mock.SetHash("h", map[string]string{"a": "1", "b": "2"})
	vals, err := client.Hash().MGet(ctx, "h", "a", "missing")
	if err != nil {
		t.Fatal(err)
	}
	if vals[0] != "1" || vals[1] != nil {
		t.Fatalf("unexpected MGet: %v", vals)
	}
}

func TestHash_Del(t *testing.T) {
	mock, client := setup(t)
	mock.SetHash("h", map[string]string{"a": "1", "b": "2"})
	n, err := client.Hash().Del(ctx, "h", "a")
	if err != nil || n != 1 {
		t.Fatalf("Del: n=%d err=%v", n, err)
	}
}

// ============================================================
// List
// ============================================================

func TestList_PushPop(t *testing.T) {
	_, client := setup(t)
	n, err := client.List().RPush(ctx, "list", "a", "b", "c")
	if err != nil || n != 3 {
		t.Fatalf("RPush: n=%d err=%v", n, err)
	}
	val, err := client.List().LPop(ctx, "list")
	if err != nil || val != "a" {
		t.Fatalf("LPop: val=%q err=%v", val, err)
	}
	val, err = client.List().RPop(ctx, "list")
	if err != nil || val != "c" {
		t.Fatalf("RPop: val=%q err=%v", val, err)
	}
}

func TestList_LPush(t *testing.T) {
	_, client := setup(t)
	client.List().RPush(ctx, "list", "b")
	client.List().LPush(ctx, "list", "a")
	vals, err := client.List().Range(ctx, "list", 0, -1)
	if err != nil {
		t.Fatal(err)
	}
	if len(vals) != 2 || vals[0] != "a" || vals[1] != "b" {
		t.Fatalf("unexpected range: %v", vals)
	}
}

func TestList_Len(t *testing.T) {
	mock, client := setup(t)
	mock.PushList("list", "a", "b", "c")
	n, err := client.List().Len(ctx, "list")
	if err != nil || n != 3 {
		t.Fatalf("Len: %d %v", n, err)
	}
}

func TestList_PopEmpty(t *testing.T) {
	_, client := setup(t)
	_, err := client.List().LPop(ctx, "empty")
	if !errors.Is(err, resilix.ErrKeyNotFound) {
		t.Fatalf("want ErrKeyNotFound, got %v", err)
	}
}

// ============================================================
// Set
// ============================================================

func TestSet_AddMembersRemove(t *testing.T) {
	_, client := setup(t)
	n, err := client.Set().Add(ctx, "s", "a", "b", "c")
	if err != nil || n != 3 {
		t.Fatalf("Add: n=%d err=%v", n, err)
	}
	members, err := client.Set().Members(ctx, "s")
	if err != nil || len(members) != 3 {
		t.Fatalf("Members: %v err=%v", members, err)
	}
	ok, err := client.Set().IsMember(ctx, "s", "b")
	if err != nil || !ok {
		t.Fatalf("IsMember b: %v %v", ok, err)
	}
	n, err = client.Set().Remove(ctx, "s", "b")
	if err != nil || n != 1 {
		t.Fatalf("Remove: n=%d err=%v", n, err)
	}
	ok, err = client.Set().IsMember(ctx, "s", "b")
	if err != nil || ok {
		t.Fatalf("IsMember after remove: %v %v", ok, err)
	}
}

func TestSet_Card(t *testing.T) {
	mock, client := setup(t)
	mock.AddSet("s", "a", "b")
	n, err := client.Set().Card(ctx, "s")
	if err != nil || n != 2 {
		t.Fatalf("Card: %d %v", n, err)
	}
}

// ============================================================
// SortedSet
// ============================================================

func TestSortedSet_AddRange(t *testing.T) {
	_, client := setup(t)
	members := []resilix.ZMember{
		{Score: 1, Member: "a"},
		{Score: 3, Member: "c"},
		{Score: 2, Member: "b"},
	}
	n, err := client.SortedSet().Add(ctx, "zs", members...)
	if err != nil || n != 3 {
		t.Fatalf("ZAdd: n=%d err=%v", n, err)
	}
	got, err := client.SortedSet().Range(ctx, "zs", 0, -1)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 || got[0].Member != "a" || got[2].Member != "c" {
		t.Fatalf("unexpected range: %v", got)
	}
}

func TestSortedSet_RevRange(t *testing.T) {
	mock, client := setup(t)
	mock.AddZSet("zs",
		resilix.ZMember{Score: 1, Member: "low"},
		resilix.ZMember{Score: 2, Member: "mid"},
		resilix.ZMember{Score: 3, Member: "high"},
	)
	got, err := client.SortedSet().RevRange(ctx, "zs", 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].Member != "high" {
		t.Fatalf("unexpected rev range: %v", got)
	}
}

func TestSortedSet_Score(t *testing.T) {
	mock, client := setup(t)
	mock.AddZSet("zs", resilix.ZMember{Score: 42, Member: "x"})
	score, err := client.SortedSet().Score(ctx, "zs", "x")
	if err != nil || score != 42 {
		t.Fatalf("Score: %v %v", score, err)
	}
}

func TestSortedSet_Remove(t *testing.T) {
	mock, client := setup(t)
	mock.AddZSet("zs", resilix.ZMember{Score: 1, Member: "a"}, resilix.ZMember{Score: 2, Member: "b"})
	n, err := client.SortedSet().Remove(ctx, "zs", "a")
	if err != nil || n != 1 {
		t.Fatalf("ZRem: n=%d err=%v", n, err)
	}
}

// ============================================================
// Stream
// ============================================================

func TestStream_AddRead(t *testing.T) {
	_, client := setup(t)
	if err := client.Stream().CreateGroup(ctx, "stream", "grp", "0"); err != nil {
		t.Fatal(err)
	}
	id, err := client.Stream().Add(ctx, "stream", map[string]any{"k": "v"}, 0, false)
	if err != nil || id == "" {
		t.Fatalf("XAdd: id=%q err=%v", id, err)
	}
	responses, err := client.Stream().ReadGroup(ctx, resilix.StreamReadRequest{
		Group: "grp", Consumer: "c1",
		Streams: []string{"stream"}, IDs: []string{">"},
		Count: 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(responses) == 0 || len(responses[0].Entries) == 0 {
		t.Fatal("want at least one stream entry")
	}
}

func TestStream_AckDel(t *testing.T) {
	_, client := setup(t)
	client.Stream().CreateGroup(ctx, "s", "g", "0")
	id, _ := client.Stream().Add(ctx, "s", map[string]any{"x": "y"}, 0, false)

	n, err := client.Stream().Ack(ctx, "s", "g", id)
	if err != nil || n != 1 {
		t.Fatalf("Ack: n=%d err=%v", n, err)
	}
	n, err = client.Stream().Delete(ctx, "s", id)
	if err != nil || n != 1 {
		t.Fatalf("XDel: n=%d err=%v", n, err)
	}
}

// ============================================================
// Script
// ============================================================

func TestScript_LoadEvalSHA(t *testing.T) {
	_, client := setup(t)
	sha, err := client.Script().Load(ctx, "return 1")
	if err != nil || sha == "" {
		t.Fatalf("ScriptLoad: sha=%q err=%v", sha, err)
	}
	result, err := client.Script().EvalSHA(ctx, sha, nil)
	if err != nil {
		t.Fatal(err)
	}
	_ = result
}

func TestScript_Eval(t *testing.T) {
	_, client := setup(t)
	result, err := client.Script().Eval(ctx, "return 42", nil)
	if err != nil {
		t.Fatal(err)
	}
	_ = result
}

// ============================================================
// Bulk
// ============================================================

func TestBulk_GetDelete(t *testing.T) {
	mock, client := setup(t)
	for i := 0; i < 5; i++ {
		mock.SetKV(string(rune('a'+i)), "val")
	}
	keys := []string{"a", "b", "c", "d", "e"}
	result, err := client.Bulk().Get(ctx, keys, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(result) != 5 {
		t.Fatalf("want 5 results, got %d", len(result))
	}
	n, err := client.Bulk().Delete(ctx, keys, 2)
	if err != nil || n != 5 {
		t.Fatalf("Bulk.Delete: n=%d err=%v", n, err)
	}
}

func TestBulk_Expire(t *testing.T) {
	mock, client := setup(t)
	mock.SetKV("x", "1")
	mock.SetKV("y", "2")
	n, err := client.Bulk().Expire(ctx, []string{"x", "y"}, 10*time.Second, 10)
	if err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Fatalf("want 2 expired, got %d", n)
	}
}

// ============================================================
// HyperLogLog
// ============================================================

func TestHLL_AddCountMerge(t *testing.T) {
	_, client := setup(t)
	changed, err := client.HyperLogLog().Add(ctx, "hll1", "a", "b", "c")
	if err != nil || !changed {
		t.Fatalf("PFAdd: changed=%v err=%v", changed, err)
	}
	// adding same elements again → not changed
	changed, err = client.HyperLogLog().Add(ctx, "hll1", "a", "b", "c")
	if err != nil || changed {
		t.Fatalf("PFAdd dup: changed=%v err=%v", changed, err)
	}
	count, err := client.HyperLogLog().Count(ctx, "hll1")
	if err != nil || count != 3 {
		t.Fatalf("PFCount: %d %v", count, err)
	}
	client.HyperLogLog().Add(ctx, "hll2", "d", "e")
	if err := client.HyperLogLog().Merge(ctx, "merged", "hll1", "hll2"); err != nil {
		t.Fatal(err)
	}
	count, err = client.HyperLogLog().Count(ctx, "merged")
	if err != nil || count != 5 {
		t.Fatalf("merged count: %d %v", count, err)
	}
}

// ============================================================
// Pipeline
// ============================================================

func TestPipeline(t *testing.T) {
	_, client := setup(t)
	results, err := client.Pipeline(ctx, func(b resilix.PipelineBuilder) {
		b.Set("pk", "pv", 0)
		b.Get("pk")
		b.Incr("pc")
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 3 {
		t.Fatalf("want 3 results, got %d", len(results))
	}
	for i, r := range results {
		if r.Err != nil {
			t.Fatalf("pipeline result[%d]: %v", i, r.Err)
		}
	}
}

// ============================================================
// Error injection
// ============================================================

func TestErrorInjection_AllOps(t *testing.T) {
	mock, client := setup(t)
	boom := errors.New("injected")
	mock.InjectError("", boom)
	if err := client.Ping(ctx); err == nil {
		t.Fatal("want error")
	}
	mock.ClearErrors()
	if err := client.Ping(ctx); err != nil {
		t.Fatalf("want nil after clear: %v", err)
	}
}
