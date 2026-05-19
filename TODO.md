# Resilix — TODO

Track progress toward full senior-grade Redis SDK.

Legend: `[ ]` pending · `[x]` done · `[-]` in progress

---

## 1. Testing

### 1.1 Unit — Service methods
- [x] KV service tests (Get, Set, MGet, MSet, Del, Exists, Expire, TTL, Incr, Decr)
- [x] Hash service tests (Set, Get, GetAll, MGet, Del)
- [x] List service tests (LPush, RPush, LPop, RPop, Range, Len)
- [x] Set service tests (Add, Remove, Members, IsMember, Card)
- [x] SortedSet service tests (Add, Range, RevRange, Remove, Score)
- [x] Stream service tests (Add, ReadGroup, Ack, Delete, CreateGroup)
- [ ] PubSub service tests (Publish, Subscribe) — requires goroutine coordination
- [x] Script service tests (Eval, EvalSHA, Load)
- [x] Bulk service tests (Get, Delete, Expire)
- [x] HyperLogLog service tests (Add, Count, Merge)
- [x] Pipeline tests

### 1.2 Unit — Client / cross-cutting
- [x] Middleware chain order and abort
- [x] BeforeExecute / AfterExecute hooks
- [x] Admission controller backpressure
- [x] Graceful shutdown drain (`Close()` waits for in-flight)
- [x] `ErrClientClosed` after `Close()`
- [x] Configurable retry classifier
- [x] `OperationError` wrapping and unwrapping
- [x] `ValidationError` on empty key

### 1.3 Unit — Internals
- [ ] Driver adapter tests (`internal/driver/goredis/`) — requires live Redis or httptest
- [ ] Middleware chain execution tests (covered by client_test.go)
- [x] Error normalization and wrapping tests
- [ ] Telemetry recorder tests (metrics emitted, spans created)

### 1.4 Mock backend
- [x] `resilixtest.NewMockBackend()` — in-memory Backend implementation
- [x] `resilixtest.NewClient(t, mock)` — test client helper
- [x] Error injection (`InjectError`, `ClearErrors`)
- [x] Call count tracking (`CallCount`)
- [x] Direct state setters (`SetKV`, `SetHash`, `PushList`, `AddSet`, `AddZSet`)

### 1.5 Integration
- [ ] Integration tests: KV full round-trip (live Redis)
- [ ] Integration tests: Pipeline + Transaction
- [ ] Integration tests: PubSub message delivery
- [ ] Integration tests: Stream consumer group flow
- [ ] Integration tests: Circuit breaker trips under failure injection

---

## 2. Resiliencia / Arquitectura

- [x] Graceful shutdown with drain — `Close()` waits for inflight requests before exit
- [x] Configurable retry classifier — `Options.Retry.Classifier func(error) bool`
- [ ] Sentinel mode — verify failover callbacks and master election work end-to-end
- [ ] Cluster mode — verify slot routing, moved/ask error handling, resharding
- [ ] Read replica routing — route reads to replicas in cluster/sentinel topologies
- [ ] Key prefixing / namespacing — multi-tenant support via prefix option (use `keybuilder` package for now)
- [ ] Context deadline → Redis timeout — go-redis respects ctx deadlines; explicitly document this behavior

---

## 3. Redis Feature Parity

- [x] Geo commands — `GeoAdd`, `GeoDist`, `GeoPos`, `GeoSearch`
- [x] HyperLogLog — `PFAdd`, `PFCount`, `PFMerge`
- [x] Bit operations — `SetBit`, `GetBit`, `BitCount`, `BitOp`, `BitPos`
- [x] `LMPop` / `ZMPop` — atomic multi-pop (Redis 7.0+)
- [x] Distributed lock — Redlock algorithm (`sdk/go/lock/`)
- [ ] Key expiry notifications — keyspace notification subscriptions
- [ ] `OBJECT` introspection — `OBJECT ENCODING`, `OBJECT FREQ`, `OBJECT IDLETIME`
- [ ] Client-side caching — Redis 6+ client tracking protocol (`CLIENT TRACKING`)

---

## 4. Observabilidad

- [x] Circuit breaker state transitions → Prometheus metric (`resilix_redis_circuit_breaker_state`)
- [x] Connection pool stats → Prometheus metrics (pool_hits, pool_misses, pool_timeouts, pool_total_conns)
- [x] Per-error-type label on `operations_total` counter (`error_type` label)
- [x] Grafana dashboards — provisioned in `observability/grafana/`
  - [x] Operations/sec by operation
  - [x] Error rate % by operation
  - [x] P99 / P95 latency
  - [x] Inflight operations
  - [x] Slow queries/min
  - [x] Circuit breaker state timeline
  - [x] Pool utilization

---

## 5. DX / Ecosystem

- [x] Key builder helper — `sdk/go/keybuilder/` with `New()`, `Build()`, `Sub()`, `Pattern()`
- [ ] CLI — `resilixctl keys scan` (cursor-based key listing)
- [ ] CLI — `resilixctl info` (server info sections)
- [ ] CLI — `resilixctl monitor` (MONITOR stream)
- [ ] Benchmark comparison — Resilix vs `go-redis` raw vs `rueidis` (publish results)
- [ ] Python SDK — implement core SDK in `sdk/python/`

---

## 6. Docs / Config

- [ ] `docs/sample-config.yaml` — document all fields with comments and valid ranges
- [ ] `docs/guides/testing.md` — how to use mock backend in user tests
- [ ] `docs/guides/cluster.md` — cluster + sentinel setup walkthrough
- [ ] CHANGELOG.md — start tracking changes per version
