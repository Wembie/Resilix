package contract

import "context"

type Backend interface {
	Ping(ctx context.Context) error
	Close() error
	PoolStats() PoolStats

	Get(ctx context.Context, key string) (string, error)
	Set(ctx context.Context, key string, value any, ttlSeconds float64) error
	MGet(ctx context.Context, keys ...string) ([]any, error)
	MSet(ctx context.Context, values map[string]any) error
	Del(ctx context.Context, keys ...string) (int64, error)
	Exists(ctx context.Context, keys ...string) (int64, error)
	Expire(ctx context.Context, key string, ttlSeconds float64) (bool, error)
	TTL(ctx context.Context, key string) (float64, error)
	Incr(ctx context.Context, key string) (int64, error)
	Decr(ctx context.Context, key string) (int64, error)

	HSet(ctx context.Context, key string, values map[string]any) (int64, error)
	HGet(ctx context.Context, key, field string) (string, error)
	HGetAll(ctx context.Context, key string) (map[string]string, error)
	HMGet(ctx context.Context, key string, fields ...string) ([]any, error)
	HDel(ctx context.Context, key string, fields ...string) (int64, error)

	LPush(ctx context.Context, key string, values ...any) (int64, error)
	RPush(ctx context.Context, key string, values ...any) (int64, error)
	LPop(ctx context.Context, key string) (string, error)
	RPop(ctx context.Context, key string) (string, error)
	LRange(ctx context.Context, key string, start, stop int64) ([]string, error)
	LLen(ctx context.Context, key string) (int64, error)

	SAdd(ctx context.Context, key string, members ...string) (int64, error)
	SRem(ctx context.Context, key string, members ...string) (int64, error)
	SMembers(ctx context.Context, key string) ([]string, error)
	SIsMember(ctx context.Context, key, member string) (bool, error)
	SCard(ctx context.Context, key string) (int64, error)

	ZAdd(ctx context.Context, key string, members ...ZMember) (int64, error)
	ZRange(ctx context.Context, key string, start, stop int64) ([]ZMember, error)
	ZRevRange(ctx context.Context, key string, start, stop int64) ([]ZMember, error)
	ZRem(ctx context.Context, key string, members ...string) (int64, error)
	ZScore(ctx context.Context, key, member string) (float64, error)

	Publish(ctx context.Context, channel string, payload any) (int64, error)
	Subscribe(ctx context.Context, channels ...string) (Subscription, error)

	XAdd(ctx context.Context, stream string, values map[string]any, maxLen int64, approximate bool) (string, error)
	XReadGroup(ctx context.Context, request StreamReadRequest) ([]StreamReadResponse, error)
	XAck(ctx context.Context, stream, group string, ids ...string) (int64, error)
	XDel(ctx context.Context, stream string, ids ...string) (int64, error)
	XGroupCreateMkStream(ctx context.Context, stream, group, start string) error

	Eval(ctx context.Context, script string, keys []string, args ...any) (any, error)
	EvalSHA(ctx context.Context, sha string, keys []string, args ...any) (any, error)
	ScriptLoad(ctx context.Context, script string) (string, error)

	Pipeline(ctx context.Context, commands []PipelineCommand) ([]CommandResult, error)
	Transaction(ctx context.Context, watchKeys []string, commands []PipelineCommand) ([]CommandResult, error)
	Scan(ctx context.Context, cursor uint64, pattern string, count int64) ([]string, uint64, error)
}
