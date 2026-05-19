// Package resilixtest provides a thread-safe in-memory Backend for use in tests.
package resilixtest

import (
	"context"
	"crypto/sha1"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/Wembie/Resilix/sdk/go/internal/contract"
)

// MockBackend is a thread-safe, in-memory implementation of contract.Backend.
// Create it with NewMockBackend. Use InjectError to simulate failures, and
// direct setter methods to pre-populate state without going through the client.
type MockBackend struct {
	mu sync.Mutex

	kv      map[string]kvEntry
	hashes  map[string]map[string]string
	lists   map[string][]string
	sets    map[string]map[string]struct{}
	zsets   map[string][]contract.ZMember
	streams map[string][]contract.StreamEntry
	groups  map[string]map[string]int // stream → group → last-delivered index
	scripts map[string]string         // sha1hex → script body

	pubsub pubsubState

	errors  map[string][]error // operation → queue of injected errors (consumed FIFO)
	calls   map[string]int
	streamSeq int64
	closed  bool
}

type kvEntry struct {
	value  string
	expiry time.Time // zero = no expiry
}

func (e kvEntry) expired() bool {
	return !e.expiry.IsZero() && time.Now().After(e.expiry)
}

type pubsubState struct {
	mu   sync.Mutex
	subs map[string][]*mockSub // channel → subscribers
}

func NewMockBackend() *MockBackend {
	m := &MockBackend{
		kv:      make(map[string]kvEntry),
		hashes:  make(map[string]map[string]string),
		lists:   make(map[string][]string),
		sets:    make(map[string]map[string]struct{}),
		zsets:   make(map[string][]contract.ZMember),
		streams: make(map[string][]contract.StreamEntry),
		groups:  make(map[string]map[string]int),
		scripts: make(map[string]string),
		errors:  make(map[string][]error),
		calls:   make(map[string]int),
	}
	m.pubsub.subs = make(map[string][]*mockSub)
	return m
}

// InjectError causes the next call to the named operation to return err.
// Use "" to fail all operations.
func (m *MockBackend) InjectError(operation string, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.errors[operation] = append(m.errors[operation], err)
}

// ClearErrors removes all injected errors.
func (m *MockBackend) ClearErrors() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.errors = make(map[string][]error)
}

// CallCount returns how many times operation was invoked.
func (m *MockBackend) CallCount(operation string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.calls[operation]
}

// SetKV directly sets a key-value pair with no TTL. Safe for test setup.
func (m *MockBackend) SetKV(key, value string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.kv[key] = kvEntry{value: value}
}

// SetKVWithTTL directly sets a key-value pair with an expiry time.
func (m *MockBackend) SetKVWithTTL(key, value string, expiry time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.kv[key] = kvEntry{value: value, expiry: expiry}
}

// SetHash directly sets a hash key's fields.
func (m *MockBackend) SetHash(key string, fields map[string]string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	h := make(map[string]string, len(fields))
	for k, v := range fields {
		h[k] = v
	}
	m.hashes[key] = h
}

// PushList directly sets a list's values (index 0 = head).
func (m *MockBackend) PushList(key string, values ...string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.lists[key] = append(m.lists[key], values...)
}

// AddSet directly adds members to a set.
func (m *MockBackend) AddSet(key string, members ...string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.sets[key] == nil {
		m.sets[key] = make(map[string]struct{})
	}
	for _, member := range members {
		m.sets[key][member] = struct{}{}
	}
}

// AddZSet directly adds members to a sorted set.
func (m *MockBackend) AddZSet(key string, members ...contract.ZMember) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.zsets[key] = append(m.zsets[key], members...)
	sortZSet(m.zsets[key])
}

// --- internals ---

func (m *MockBackend) checkError(op string) error {
	m.calls[op]++
	if q := m.errors[op]; len(q) > 0 {
		err := q[0]
		m.errors[op] = q[1:]
		return err
	}
	if q := m.errors[""]; len(q) > 0 {
		return q[0]
	}
	return nil
}

func (m *MockBackend) getKV(key string) (string, bool) {
	entry, ok := m.kv[key]
	if !ok {
		return "", false
	}
	if entry.expired() {
		delete(m.kv, key)
		return "", false
	}
	return entry.value, true
}

// --- contract.Backend implementation ---

func (m *MockBackend) Ping(_ context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.checkError("Ping")
}

func (m *MockBackend) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closed = true
	m.pubsub.mu.Lock()
	defer m.pubsub.mu.Unlock()
	for _, subs := range m.pubsub.subs {
		for _, sub := range subs {
			sub.close()
		}
	}
	m.pubsub.subs = make(map[string][]*mockSub)
	return nil
}

func (m *MockBackend) PoolStats() contract.PoolStats {
	return contract.PoolStats{TotalConns: 1, IdleConns: 1}
}

func (m *MockBackend) Get(_ context.Context, key string) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.checkError("Get"); err != nil {
		return "", err
	}
	val, ok := m.getKV(key)
	if !ok {
		return "", contract.ErrKeyNotFound
	}
	return val, nil
}

func (m *MockBackend) Set(_ context.Context, key string, value any, ttlSeconds float64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.checkError("Set"); err != nil {
		return err
	}
	entry := kvEntry{value: fmt.Sprintf("%v", value)}
	if ttlSeconds > 0 {
		entry.expiry = time.Now().Add(time.Duration(ttlSeconds * float64(time.Second)))
	}
	m.kv[key] = entry
	return nil
}

func (m *MockBackend) MGet(_ context.Context, keys ...string) ([]any, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.checkError("MGet"); err != nil {
		return nil, err
	}
	result := make([]any, len(keys))
	for i, key := range keys {
		if val, ok := m.getKV(key); ok {
			result[i] = val
		}
	}
	return result, nil
}

func (m *MockBackend) MSet(_ context.Context, values map[string]any) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.checkError("MSet"); err != nil {
		return err
	}
	for k, v := range values {
		m.kv[k] = kvEntry{value: fmt.Sprintf("%v", v)}
	}
	return nil
}

func (m *MockBackend) Del(_ context.Context, keys ...string) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.checkError("Del"); err != nil {
		return 0, err
	}
	var count int64
	for _, key := range keys {
		if _, ok := m.getKV(key); ok {
			delete(m.kv, key)
			count++
		}
	}
	return count, nil
}

func (m *MockBackend) Exists(_ context.Context, keys ...string) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.checkError("Exists"); err != nil {
		return 0, err
	}
	var count int64
	for _, key := range keys {
		if _, ok := m.getKV(key); ok {
			count++
		}
	}
	return count, nil
}

func (m *MockBackend) Expire(_ context.Context, key string, ttlSeconds float64) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.checkError("Expire"); err != nil {
		return false, err
	}
	entry, ok := m.kv[key]
	if !ok || entry.expired() {
		return false, nil
	}
	entry.expiry = time.Now().Add(time.Duration(ttlSeconds * float64(time.Second)))
	m.kv[key] = entry
	return true, nil
}

func (m *MockBackend) TTL(_ context.Context, key string) (float64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.checkError("TTL"); err != nil {
		return 0, err
	}
	entry, ok := m.kv[key]
	if !ok || entry.expired() {
		return -2, nil // key does not exist
	}
	if entry.expiry.IsZero() {
		return -1, nil // no expiry
	}
	return time.Until(entry.expiry).Seconds(), nil
}

func (m *MockBackend) Incr(_ context.Context, key string) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.checkError("Incr"); err != nil {
		return 0, err
	}
	var n int64
	if val, ok := m.getKV(key); ok {
		fmt.Sscanf(val, "%d", &n)
	}
	n++
	m.kv[key] = kvEntry{value: fmt.Sprintf("%d", n)}
	return n, nil
}

func (m *MockBackend) Decr(_ context.Context, key string) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.checkError("Decr"); err != nil {
		return 0, err
	}
	var n int64
	if val, ok := m.getKV(key); ok {
		fmt.Sscanf(val, "%d", &n)
	}
	n--
	m.kv[key] = kvEntry{value: fmt.Sprintf("%d", n)}
	return n, nil
}

// --- Hash ---

func (m *MockBackend) HSet(_ context.Context, key string, values map[string]any) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.checkError("HSet"); err != nil {
		return 0, err
	}
	if m.hashes[key] == nil {
		m.hashes[key] = make(map[string]string)
	}
	var added int64
	for f, v := range values {
		if _, exists := m.hashes[key][f]; !exists {
			added++
		}
		m.hashes[key][f] = fmt.Sprintf("%v", v)
	}
	return added, nil
}

func (m *MockBackend) HGet(_ context.Context, key, field string) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.checkError("HGet"); err != nil {
		return "", err
	}
	if m.hashes[key] == nil {
		return "", contract.ErrKeyNotFound
	}
	val, ok := m.hashes[key][field]
	if !ok {
		return "", contract.ErrKeyNotFound
	}
	return val, nil
}

func (m *MockBackend) HGetAll(_ context.Context, key string) (map[string]string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.checkError("HGetAll"); err != nil {
		return nil, err
	}
	result := make(map[string]string)
	for k, v := range m.hashes[key] {
		result[k] = v
	}
	return result, nil
}

func (m *MockBackend) HMGet(_ context.Context, key string, fields ...string) ([]any, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.checkError("HMGet"); err != nil {
		return nil, err
	}
	result := make([]any, len(fields))
	for i, f := range fields {
		if m.hashes[key] != nil {
			if val, ok := m.hashes[key][f]; ok {
				result[i] = val
			}
		}
	}
	return result, nil
}

func (m *MockBackend) HDel(_ context.Context, key string, fields ...string) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.checkError("HDel"); err != nil {
		return 0, err
	}
	var count int64
	for _, f := range fields {
		if m.hashes[key] != nil {
			if _, ok := m.hashes[key][f]; ok {
				delete(m.hashes[key], f)
				count++
			}
		}
	}
	return count, nil
}

// --- List ---

func (m *MockBackend) LPush(_ context.Context, key string, values ...any) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.checkError("LPush"); err != nil {
		return 0, err
	}
	strs := toStringSlice(values)
	m.lists[key] = append(strs, m.lists[key]...)
	return int64(len(m.lists[key])), nil
}

func (m *MockBackend) RPush(_ context.Context, key string, values ...any) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.checkError("RPush"); err != nil {
		return 0, err
	}
	m.lists[key] = append(m.lists[key], toStringSlice(values)...)
	return int64(len(m.lists[key])), nil
}

func (m *MockBackend) LPop(_ context.Context, key string) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.checkError("LPop"); err != nil {
		return "", err
	}
	if len(m.lists[key]) == 0 {
		return "", contract.ErrKeyNotFound
	}
	val := m.lists[key][0]
	m.lists[key] = m.lists[key][1:]
	return val, nil
}

func (m *MockBackend) RPop(_ context.Context, key string) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.checkError("RPop"); err != nil {
		return "", err
	}
	lst := m.lists[key]
	if len(lst) == 0 {
		return "", contract.ErrKeyNotFound
	}
	val := lst[len(lst)-1]
	m.lists[key] = lst[:len(lst)-1]
	return val, nil
}

func (m *MockBackend) LRange(_ context.Context, key string, start, stop int64) ([]string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.checkError("LRange"); err != nil {
		return nil, err
	}
	lst := m.lists[key]
	n := int64(len(lst))
	if start < 0 {
		start = n + start
	}
	if stop < 0 {
		stop = n + stop
	}
	if start < 0 {
		start = 0
	}
	if stop >= n {
		stop = n - 1
	}
	if start > stop || n == 0 {
		return []string{}, nil
	}
	result := make([]string, stop-start+1)
	copy(result, lst[start:stop+1])
	return result, nil
}

func (m *MockBackend) LLen(_ context.Context, key string) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.checkError("LLen"); err != nil {
		return 0, err
	}
	return int64(len(m.lists[key])), nil
}

// --- Set ---

func (m *MockBackend) SAdd(_ context.Context, key string, members ...string) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.checkError("SAdd"); err != nil {
		return 0, err
	}
	if m.sets[key] == nil {
		m.sets[key] = make(map[string]struct{})
	}
	var added int64
	for _, member := range members {
		if _, exists := m.sets[key][member]; !exists {
			m.sets[key][member] = struct{}{}
			added++
		}
	}
	return added, nil
}

func (m *MockBackend) SRem(_ context.Context, key string, members ...string) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.checkError("SRem"); err != nil {
		return 0, err
	}
	var removed int64
	for _, member := range members {
		if m.sets[key] != nil {
			if _, ok := m.sets[key][member]; ok {
				delete(m.sets[key], member)
				removed++
			}
		}
	}
	return removed, nil
}

func (m *MockBackend) SMembers(_ context.Context, key string) ([]string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.checkError("SMembers"); err != nil {
		return nil, err
	}
	result := make([]string, 0, len(m.sets[key]))
	for member := range m.sets[key] {
		result = append(result, member)
	}
	sort.Strings(result)
	return result, nil
}

func (m *MockBackend) SIsMember(_ context.Context, key, member string) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.checkError("SIsMember"); err != nil {
		return false, err
	}
	if m.sets[key] == nil {
		return false, nil
	}
	_, ok := m.sets[key][member]
	return ok, nil
}

func (m *MockBackend) SCard(_ context.Context, key string) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.checkError("SCard"); err != nil {
		return 0, err
	}
	return int64(len(m.sets[key])), nil
}

// --- Sorted Set ---

func (m *MockBackend) ZAdd(_ context.Context, key string, members ...contract.ZMember) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.checkError("ZAdd"); err != nil {
		return 0, err
	}
	var added int64
	for _, member := range members {
		found := false
		for i, existing := range m.zsets[key] {
			if existing.Member == member.Member {
				m.zsets[key][i].Score = member.Score
				found = true
				break
			}
		}
		if !found {
			m.zsets[key] = append(m.zsets[key], member)
			added++
		}
	}
	sortZSet(m.zsets[key])
	return added, nil
}

func (m *MockBackend) ZRange(_ context.Context, key string, start, stop int64) ([]contract.ZMember, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.checkError("ZRange"); err != nil {
		return nil, err
	}
	return sliceZSet(m.zsets[key], start, stop, false), nil
}

func (m *MockBackend) ZRevRange(_ context.Context, key string, start, stop int64) ([]contract.ZMember, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.checkError("ZRevRange"); err != nil {
		return nil, err
	}
	return sliceZSet(m.zsets[key], start, stop, true), nil
}

func (m *MockBackend) ZRem(_ context.Context, key string, members ...string) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.checkError("ZRem"); err != nil {
		return 0, err
	}
	set := make(map[string]bool, len(members))
	for _, member := range members {
		set[member] = true
	}
	var removed int64
	filtered := m.zsets[key][:0]
	for _, existing := range m.zsets[key] {
		if set[existing.Member] {
			removed++
		} else {
			filtered = append(filtered, existing)
		}
	}
	m.zsets[key] = filtered
	return removed, nil
}

func (m *MockBackend) ZScore(_ context.Context, key, member string) (float64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.checkError("ZScore"); err != nil {
		return 0, err
	}
	for _, existing := range m.zsets[key] {
		if existing.Member == member {
			return existing.Score, nil
		}
	}
	return 0, contract.ErrKeyNotFound
}

// --- PubSub ---

func (m *MockBackend) Publish(_ context.Context, channel string, payload any) (int64, error) {
	m.mu.Lock()
	if err := m.checkError("Publish"); err != nil {
		m.mu.Unlock()
		return 0, err
	}
	m.mu.Unlock()

	m.pubsub.mu.Lock()
	defer m.pubsub.mu.Unlock()
	subs := m.pubsub.subs[channel]
	msg := contract.Message{Channel: channel, Payload: fmt.Sprintf("%v", payload)}
	for _, sub := range subs {
		select {
		case sub.ch <- msg:
		default:
		}
	}
	return int64(len(subs)), nil
}

func (m *MockBackend) Subscribe(_ context.Context, channels ...string) (contract.Subscription, error) {
	m.mu.Lock()
	if err := m.checkError("Subscribe"); err != nil {
		m.mu.Unlock()
		return nil, err
	}
	m.mu.Unlock()

	sub := &mockSub{
		ch:       make(chan contract.Message, 128),
		channels: channels,
		pubsub:   &m.pubsub,
	}

	m.pubsub.mu.Lock()
	defer m.pubsub.mu.Unlock()
	for _, ch := range channels {
		m.pubsub.subs[ch] = append(m.pubsub.subs[ch], sub)
	}
	return sub, nil
}

// --- Stream ---

func (m *MockBackend) XAdd(_ context.Context, stream string, values map[string]any, _ int64, _ bool) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.checkError("XAdd"); err != nil {
		return "", err
	}
	m.streamSeq++
	id := fmt.Sprintf("%d-0", time.Now().UnixMilli()*1000+m.streamSeq)
	entry := contract.StreamEntry{ID: id, Values: values}
	m.streams[stream] = append(m.streams[stream], entry)
	return id, nil
}

func (m *MockBackend) XReadGroup(_ context.Context, req contract.StreamReadRequest) ([]contract.StreamReadResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.checkError("XReadGroup"); err != nil {
		return nil, err
	}
	var responses []contract.StreamReadResponse
	for i, streamKey := range req.Streams {
		id := ">"
		if i < len(req.IDs) {
			id = req.IDs[i]
		}
		entries := m.streams[streamKey]
		if m.groups[streamKey] == nil {
			m.groups[streamKey] = make(map[string]int)
		}
		startIdx := m.groups[streamKey][req.Group]
		if id != ">" {
			startIdx = 0
		}
		var count int64
		var result []contract.StreamEntry
		for j := startIdx; j < len(entries); j++ {
			result = append(result, entries[j])
			count++
			if req.Count > 0 && count >= req.Count {
				m.groups[streamKey][req.Group] = j + 1
				break
			}
		}
		if id == ">" && count == 0 {
			continue
		}
		responses = append(responses, contract.StreamReadResponse{Stream: streamKey, Entries: result})
	}
	return responses, nil
}

func (m *MockBackend) XAck(_ context.Context, _, _ string, _ ...string) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.checkError("XAck"); err != nil {
		return 0, err
	}
	return 1, nil
}

func (m *MockBackend) XDel(_ context.Context, stream string, ids ...string) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.checkError("XDel"); err != nil {
		return 0, err
	}
	set := make(map[string]bool, len(ids))
	for _, id := range ids {
		set[id] = true
	}
	var removed int64
	filtered := m.streams[stream][:0]
	for _, entry := range m.streams[stream] {
		if set[entry.ID] {
			removed++
		} else {
			filtered = append(filtered, entry)
		}
	}
	m.streams[stream] = filtered
	return removed, nil
}

func (m *MockBackend) XGroupCreateMkStream(_ context.Context, stream, group, _ string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.checkError("XGroupCreateMkStream"); err != nil {
		return err
	}
	if m.groups[stream] == nil {
		m.groups[stream] = make(map[string]int)
	}
	m.groups[stream][group] = 0
	return nil
}

// --- Script ---

func (m *MockBackend) Eval(_ context.Context, script string, _ []string, _ ...any) (any, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.checkError("Eval"); err != nil {
		return nil, err
	}
	return script, nil
}

func (m *MockBackend) EvalSHA(_ context.Context, sha string, _ []string, _ ...any) (any, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.checkError("EvalSHA"); err != nil {
		return nil, err
	}
	script, ok := m.scripts[sha]
	if !ok {
		return nil, fmt.Errorf("resilix: NOSCRIPT: %s", sha)
	}
	return script, nil
}

func (m *MockBackend) ScriptLoad(_ context.Context, script string) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.checkError("ScriptLoad"); err != nil {
		return "", err
	}
	h := sha1.Sum([]byte(script))
	sha := fmt.Sprintf("%x", h)
	m.scripts[sha] = script
	return sha, nil
}

// --- Pipeline / Transaction ---

func (m *MockBackend) Pipeline(ctx context.Context, commands []contract.PipelineCommand) ([]contract.CommandResult, error) {
	m.mu.Lock()
	if err := m.checkError("Pipeline"); err != nil {
		m.mu.Unlock()
		return nil, err
	}
	m.mu.Unlock()
	return m.execCommands(ctx, commands), nil
}

func (m *MockBackend) Transaction(ctx context.Context, _ []string, commands []contract.PipelineCommand) ([]contract.CommandResult, error) {
	m.mu.Lock()
	if err := m.checkError("Transaction"); err != nil {
		m.mu.Unlock()
		return nil, err
	}
	m.mu.Unlock()
	return m.execCommands(ctx, commands), nil
}

func (m *MockBackend) execCommands(ctx context.Context, commands []contract.PipelineCommand) []contract.CommandResult {
	results := make([]contract.CommandResult, 0, len(commands))
	for _, cmd := range commands {
		result := contract.CommandResult{Name: cmd.Name, Key: cmd.Key}
		switch cmd.Name {
		case "SET":
			result.Err = m.Set(ctx, cmd.Key, cmd.Value, cmd.TTL.Seconds())
		case "GET":
			result.Value, result.Err = m.Get(ctx, cmd.Key)
		case "DEL":
			result.Value, result.Err = m.Del(ctx, cmd.Keys...)
		case "INCR":
			result.Value, result.Err = m.Incr(ctx, cmd.Key)
		case "DECR":
			result.Value, result.Err = m.Decr(ctx, cmd.Key)
		case "EXPIRE":
			result.Value, result.Err = m.Expire(ctx, cmd.Key, cmd.TTL.Seconds())
		case "HSET":
			result.Value, result.Err = m.HSet(ctx, cmd.Key, cmd.Values)
		case "SADD":
			result.Value, result.Err = m.SAdd(ctx, cmd.Key, cmd.Members...)
		case "ZADD":
			result.Value, result.Err = m.ZAdd(ctx, cmd.Key, cmd.ZMembers...)
		case "MSET":
			result.Err = m.MSet(ctx, cmd.Values)
		default:
			result.Err = fmt.Errorf("resilix: unsupported pipeline command in mock: %s", cmd.Name)
		}
		results = append(results, result)
	}
	return results
}

func (m *MockBackend) Scan(_ context.Context, cursor uint64, pattern string, count int64) ([]string, uint64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.checkError("Scan"); err != nil {
		return nil, 0, err
	}
	var keys []string
	for key, entry := range m.kv {
		if !entry.expired() {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)

	start := int(cursor)
	if start >= len(keys) {
		return nil, 0, nil
	}
	end := start + int(count)
	if count <= 0 || end > len(keys) {
		end = len(keys)
	}
	var nextCursor uint64
	if end < len(keys) {
		nextCursor = uint64(end)
	}
	_ = pattern // simplified: pattern matching skipped in mock
	return keys[start:end], nextCursor, nil
}

// --- Geo (stub — no geospatial math in mock) ---

func (m *MockBackend) GeoAdd(_ context.Context, _ string, _ ...contract.GeoMember) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return 0, m.checkError("GeoAdd")
}

func (m *MockBackend) GeoDist(_ context.Context, _, _, _, _ string) (float64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return 0, m.checkError("GeoDist")
}

func (m *MockBackend) GeoPos(_ context.Context, _ string, members ...string) ([]*contract.GeoPosition, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.checkError("GeoPos"); err != nil {
		return nil, err
	}
	return make([]*contract.GeoPosition, len(members)), nil
}

func (m *MockBackend) GeoSearch(_ context.Context, _ string, _ contract.GeoSearchQuery) ([]contract.GeoSearchResult, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.checkError("GeoSearch"); err != nil {
		return nil, err
	}
	return nil, nil
}

// --- HyperLogLog (stub) ---

func (m *MockBackend) PFAdd(_ context.Context, key string, elements ...any) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.checkError("PFAdd"); err != nil {
		return false, err
	}
	// Treat as a set for approximate uniqueness
	if m.sets[key] == nil {
		m.sets[key] = make(map[string]struct{})
	}
	changed := false
	for _, el := range elements {
		s := fmt.Sprintf("%v", el)
		if _, exists := m.sets[key][s]; !exists {
			m.sets[key][s] = struct{}{}
			changed = true
		}
	}
	return changed, nil
}

func (m *MockBackend) PFCount(_ context.Context, keys ...string) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.checkError("PFCount"); err != nil {
		return 0, err
	}
	seen := make(map[string]struct{})
	for _, key := range keys {
		for member := range m.sets[key] {
			seen[member] = struct{}{}
		}
	}
	return int64(len(seen)), nil
}

func (m *MockBackend) PFMerge(_ context.Context, dest string, keys ...string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.checkError("PFMerge"); err != nil {
		return err
	}
	if m.sets[dest] == nil {
		m.sets[dest] = make(map[string]struct{})
	}
	for _, key := range keys {
		for member := range m.sets[key] {
			m.sets[dest][member] = struct{}{}
		}
	}
	return nil
}

// --- Bit operations (stub) ---

func (m *MockBackend) SetBit(_ context.Context, _ string, _ int64, _ int) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return 0, m.checkError("SetBit")
}

func (m *MockBackend) GetBit(_ context.Context, _ string, _ int64) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return 0, m.checkError("GetBit")
}

func (m *MockBackend) BitCount(_ context.Context, _ string, _, _ int64) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return 0, m.checkError("BitCount")
}

func (m *MockBackend) BitOp(_ context.Context, _, _ string, _ ...string) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return 0, m.checkError("BitOp")
}

func (m *MockBackend) BitPos(_ context.Context, _ string, _ int, _ ...int64) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return 0, m.checkError("BitPos")
}

// --- Multi-pop (stub) ---

func (m *MockBackend) LMPop(_ context.Context, count int64, direction string, keys ...string) (string, []string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.checkError("LMPop"); err != nil {
		return "", nil, err
	}
	for _, key := range keys {
		lst := m.lists[key]
		if len(lst) == 0 {
			continue
		}
		var result []string
		if direction == "LEFT" {
			n := int(count)
			if n > len(lst) {
				n = len(lst)
			}
			result = lst[:n]
			m.lists[key] = lst[n:]
		} else {
			n := int(count)
			if n > len(lst) {
				n = len(lst)
			}
			result = lst[len(lst)-n:]
			m.lists[key] = lst[:len(lst)-n]
		}
		return key, result, nil
	}
	return "", nil, nil
}

func (m *MockBackend) ZMPop(_ context.Context, count int64, order string, keys ...string) (string, []contract.ZMember, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.checkError("ZMPop"); err != nil {
		return "", nil, err
	}
	for _, key := range keys {
		zset := m.zsets[key]
		if len(zset) == 0 {
			continue
		}
		n := int(count)
		if n > len(zset) {
			n = len(zset)
		}
		var result []contract.ZMember
		if order == "MIN" {
			result = zset[:n]
			m.zsets[key] = zset[n:]
		} else {
			result = zset[len(zset)-n:]
			m.zsets[key] = zset[:len(zset)-n]
		}
		return key, result, nil
	}
	return "", nil, nil
}

// --- mockSub ---

type mockSub struct {
	ch       chan contract.Message
	channels []string
	pubsub   *pubsubState
	once     sync.Once
}

func (s *mockSub) Messages() <-chan contract.Message { return s.ch }

func (s *mockSub) Close() error {
	s.close()
	return nil
}

func (s *mockSub) close() {
	s.once.Do(func() {
		s.pubsub.mu.Lock()
		defer s.pubsub.mu.Unlock()
		for _, ch := range s.channels {
			subs := s.pubsub.subs[ch]
			filtered := subs[:0]
			for _, sub := range subs {
				if sub != s {
					filtered = append(filtered, sub)
				}
			}
			s.pubsub.subs[ch] = filtered
		}
		close(s.ch)
	})
}

// --- helpers ---

func toStringSlice(values []any) []string {
	result := make([]string, len(values))
	for i, v := range values {
		result[i] = fmt.Sprintf("%v", v)
	}
	return result
}

func sortZSet(members []contract.ZMember) {
	sort.Slice(members, func(i, j int) bool {
		return members[i].Score < members[j].Score
	})
}

func sliceZSet(members []contract.ZMember, start, stop int64, reverse bool) []contract.ZMember {
	n := int64(len(members))
	if reverse {
		cp := make([]contract.ZMember, n)
		copy(cp, members)
		for i, j := 0, len(cp)-1; i < j; i, j = i+1, j-1 {
			cp[i], cp[j] = cp[j], cp[i]
		}
		members = cp
	}
	if start < 0 {
		start = n + start
	}
	if stop < 0 {
		stop = n + stop
	}
	if start < 0 {
		start = 0
	}
	if stop >= n {
		stop = n - 1
	}
	if start > stop || n == 0 {
		return []contract.ZMember{}
	}
	result := make([]contract.ZMember, stop-start+1)
	copy(result, members[start:stop+1])
	return result
}
