# Resilix — TODO

Track progress toward full senior-grade Redis SDK.

Legend: `[ ]` pending · `[x]` done · `[-]` in progress

---

## 1. Testing (crítico)

### 1.1 Unit — Service methods
- [ ] KV service tests (Get, Set, MGet, MSet, Del, Exists, Expire, TTL, Incr, Decr)
- [ ] Hash service tests (Set, Get, GetAll, MGet, Del)
- [ ] List service tests (LPush, RPush, LPop, RPop, Range, Len)
- [ ] Set service tests (Add, Remove, Members, IsMember, Card)
- [ ] SortedSet service tests (Add, Range, RevRange, Remove, Score)
- [ ] Stream service tests (Add, ReadGroup, Ack, Delete, CreateGroup)
- [ ] PubSub service tests (Publish, Subscribe)
- [ ] Script service tests (Eval, EvalSHA, Load)
- [ ] Bulk service tests (Get, Delete, Expire)

### 1.2 Unit — Internals
- [ ] Driver adapter tests (`internal/driver/goredis/`)
- [ ] Middleware chain execution tests
- [ ] Hooks tests (BeforeExecute / AfterExecute)
- [ ] Error normalization and wrapping tests
- [ ] Admission controller (inflightGate) tests
- [ ] Telemetry recorder tests (metrics emitted, spans created)

### 1.3 Integration
- [ ] Integration tests: KV full round-trip (live Redis)
- [ ] Integration tests: Pipeline + Transaction
- [ ] Integration tests: PubSub message delivery
- [ ] Integration tests: Stream consumer group flow
- [ ] Integration tests: Circuit breaker trips under failure injection

### 1.4 Mock backend
- [ ] `resilixtest.NewMockBackend()` — in-memory Backend implementation
- [ ] Expose mock as public package so users can test their own code without Redis

---

## 2. Resiliencia / Arquitectura

- [ ] Graceful shutdown with drain — `Close()` waits for inflight requests before exit
- [ ] Sentinel mode — verify failover callbacks and master election work end-to-end
- [ ] Cluster mode — verify slot routing, moved/ask error handling, resharding
- [ ] Read replica routing — route reads to replicas in cluster/sentinel topologies
- [ ] Key prefixing / namespacing — multi-tenant support via prefix option
- [ ] Configurable retry classifier — let users register custom `ShouldRetry(err) bool`
- [ ] Context deadline → Redis timeout — propagate `ctx.Deadline()` as dynamic command timeout

---

## 3. Redis Feature Parity

- [ ] Distributed lock — Redlock algorithm (`sdk/go/lock/`)
- [ ] Geo commands — `GEOADD`, `GEODIST`, `GEOPOS`, `GEOSEARCH`, `GEOSEARCHSTORE`
- [ ] HyperLogLog — `PFADD`, `PFCOUNT`, `PFMERGE`
- [ ] Bit operations — `SETBIT`, `GETBIT`, `BITCOUNT`, `BITOP`, `BITPOS`
- [ ] Key expiry notifications — keyspace notification subscriptions
- [ ] `LMPOP` / `ZMPOP` — atomic multi-pop (Redis 7.0+)
- [ ] `OBJECT` introspection — `OBJECT ENCODING`, `OBJECT FREQ`, `OBJECT IDLETIME`
- [ ] Client-side caching — Redis 6+ client tracking protocol (`CLIENT TRACKING`)

---

## 4. Observabilidad

- [ ] Circuit breaker state transitions → Prometheus metric (`breaker_state_changes_total`)
- [ ] Connection pool stats → Prometheus metrics (pool_hits, pool_misses, pool_timeouts, pool_idle)
- [ ] Per-error-type label on `operations_total` counter (`error_type` label)
- [ ] Grafana dashboards — provision dashboards in `observability/grafana/`
  - [ ] Operations latency (p50/p95/p99)
  - [ ] Error rates by operation
  - [ ] Circuit breaker state timeline
  - [ ] Pool utilization

---

## 5. DX / Ecosystem

- [ ] CLI — `resilixctl keys scan` (cursor-based key listing)
- [ ] CLI — `resilixctl info` (server info sections)
- [ ] CLI — `resilixctl monitor` (MONITOR stream)
- [ ] Key builder helper — structured namespacing (`user:{id}:profile` pattern)
- [ ] Benchmark comparison — Resilix vs `go-redis` raw vs `rueidis` (publish results)
- [ ] Python SDK — implement core SDK in `sdk/python/`

---

## 6. Docs / Config

- [ ] `docs/sample-config.yaml` — document all fields with comments and valid ranges
- [ ] `docs/guides/testing.md` — how to use mock backend in user tests
- [ ] `docs/guides/cluster.md` — cluster + sentinel setup walkthrough
- [ ] CHANGELOG.md — start tracking changes per version
