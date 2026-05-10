package resilix

import (
	"context"
	"time"

	"github.com/resilix/resilix/sdk/go/internal/contract"
)

type KV interface {
	Get(ctx context.Context, key string) (string, error)
	Set(ctx context.Context, key string, value any, ttl time.Duration) error
	MGet(ctx context.Context, keys ...string) ([]any, error)
	MSet(ctx context.Context, values map[string]any, ttl time.Duration) error
	Del(ctx context.Context, keys ...string) (int64, error)
	Exists(ctx context.Context, keys ...string) (int64, error)
	Expire(ctx context.Context, key string, ttl time.Duration) (bool, error)
	TTL(ctx context.Context, key string) (time.Duration, error)
	Incr(ctx context.Context, key string) (int64, error)
	Decr(ctx context.Context, key string) (int64, error)
}

type Hash interface {
	Set(ctx context.Context, key string, values map[string]any) (int64, error)
	Get(ctx context.Context, key, field string) (string, error)
	GetAll(ctx context.Context, key string) (map[string]string, error)
	MGet(ctx context.Context, key string, fields ...string) ([]any, error)
	Del(ctx context.Context, key string, fields ...string) (int64, error)
}

type List interface {
	LPush(ctx context.Context, key string, values ...any) (int64, error)
	RPush(ctx context.Context, key string, values ...any) (int64, error)
	LPop(ctx context.Context, key string) (string, error)
	RPop(ctx context.Context, key string) (string, error)
	Range(ctx context.Context, key string, start, stop int64) ([]string, error)
	Len(ctx context.Context, key string) (int64, error)
}

type SetStore interface {
	Add(ctx context.Context, key string, members ...string) (int64, error)
	Remove(ctx context.Context, key string, members ...string) (int64, error)
	Members(ctx context.Context, key string) ([]string, error)
	IsMember(ctx context.Context, key, member string) (bool, error)
	Card(ctx context.Context, key string) (int64, error)
}

type SortedSetStore interface {
	Add(ctx context.Context, key string, members ...ZMember) (int64, error)
	Range(ctx context.Context, key string, start, stop int64) ([]ZMember, error)
	RevRange(ctx context.Context, key string, start, stop int64) ([]ZMember, error)
	Remove(ctx context.Context, key string, members ...string) (int64, error)
	Score(ctx context.Context, key, member string) (float64, error)
}

type PubSub interface {
	Publish(ctx context.Context, channel string, payload any) (int64, error)
	Subscribe(ctx context.Context, channels ...string) (Subscription, error)
}

type Stream interface {
	Add(ctx context.Context, stream string, values map[string]any, maxLen int64, approximate bool) (string, error)
	ReadGroup(ctx context.Context, request StreamReadRequest) ([]StreamReadResponse, error)
	Ack(ctx context.Context, stream, group string, ids ...string) (int64, error)
	Delete(ctx context.Context, stream string, ids ...string) (int64, error)
	CreateGroup(ctx context.Context, stream, group, start string) error
}

type Script interface {
	Eval(ctx context.Context, script string, keys []string, args ...any) (any, error)
	EvalSHA(ctx context.Context, sha string, keys []string, args ...any) (any, error)
	Load(ctx context.Context, script string) (string, error)
}

type Bulk interface {
	Get(ctx context.Context, keys []string, batchSize int) (map[string]any, error)
	Delete(ctx context.Context, keys []string, batchSize int) (int64, error)
	Expire(ctx context.Context, keys []string, ttl time.Duration, batchSize int) (int64, error)
}

type kvService struct{ client *RedisClient }
type hashService struct{ client *RedisClient }
type listService struct{ client *RedisClient }
type setService struct{ client *RedisClient }
type sortedSetService struct{ client *RedisClient }
type pubsubService struct{ client *RedisClient }
type streamService struct{ client *RedisClient }
type scriptService struct{ client *RedisClient }
type bulkService struct{ client *RedisClient }

func (s kvService) Get(ctx context.Context, key string) (string, error) {
	if err := validateKey(key); err != nil {
		return "", err
	}
	result, err := s.client.execute(ctx, Operation{Name: "GET", Kind: "kv", Keys: []string{key}}, func(inner context.Context) (any, error) {
		return s.client.backend.Get(inner, key)
	})
	if err != nil {
		return "", err
	}
	return result.(string), nil
}

func (s kvService) Set(ctx context.Context, key string, value any, ttl time.Duration) error {
	if err := validateKey(key); err != nil {
		return err
	}
	_, err := s.client.execute(ctx, Operation{Name: "SET", Kind: "kv", Keys: []string{key}}, func(inner context.Context) (any, error) {
		return nil, s.client.backend.Set(inner, key, value, toSeconds(ttl))
	})
	return err
}

func (s kvService) MGet(ctx context.Context, keys ...string) ([]any, error) {
	if err := validateKeys(keys); err != nil {
		return nil, err
	}
	result, err := s.client.execute(ctx, Operation{Name: "MGET", Kind: "kv", Keys: keys}, func(inner context.Context) (any, error) {
		return s.client.backend.MGet(inner, keys...)
	})
	if err != nil {
		return nil, err
	}
	return result.([]any), nil
}

func (s kvService) MSet(ctx context.Context, values map[string]any, ttl time.Duration) error {
	if err := validateMap("values", values); err != nil {
		return err
	}

	if ttl <= 0 {
		_, err := s.client.execute(ctx, Operation{Name: "MSET", Kind: "kv"}, func(inner context.Context) (any, error) {
			return nil, s.client.backend.MSet(inner, values)
		})
		return err
	}

	_, err := s.client.Pipeline(ctx, func(builder PipelineBuilder) {
		for key, value := range values {
			builder.Set(key, value, ttl)
		}
	})
	return err
}

func (s kvService) Del(ctx context.Context, keys ...string) (int64, error) {
	if err := validateKeys(keys); err != nil {
		return 0, err
	}
	result, err := s.client.execute(ctx, Operation{Name: "DEL", Kind: "kv", Keys: keys}, func(inner context.Context) (any, error) {
		return s.client.backend.Del(inner, keys...)
	})
	if err != nil {
		return 0, err
	}
	return result.(int64), nil
}

func (s kvService) Exists(ctx context.Context, keys ...string) (int64, error) {
	if err := validateKeys(keys); err != nil {
		return 0, err
	}
	result, err := s.client.execute(ctx, Operation{Name: "EXISTS", Kind: "kv", Keys: keys}, func(inner context.Context) (any, error) {
		return s.client.backend.Exists(inner, keys...)
	})
	if err != nil {
		return 0, err
	}
	return result.(int64), nil
}

func (s kvService) Expire(ctx context.Context, key string, ttl time.Duration) (bool, error) {
	if err := validateKey(key); err != nil {
		return false, err
	}
	result, err := s.client.execute(ctx, Operation{Name: "EXPIRE", Kind: "kv", Keys: []string{key}}, func(inner context.Context) (any, error) {
		return s.client.backend.Expire(inner, key, toSeconds(ttl))
	})
	if err != nil {
		return false, err
	}
	return result.(bool), nil
}

func (s kvService) TTL(ctx context.Context, key string) (time.Duration, error) {
	if err := validateKey(key); err != nil {
		return 0, err
	}
	result, err := s.client.execute(ctx, Operation{Name: "TTL", Kind: "kv", Keys: []string{key}}, func(inner context.Context) (any, error) {
		return s.client.backend.TTL(inner, key)
	})
	if err != nil {
		return 0, err
	}
	return time.Duration(result.(float64) * float64(time.Second)), nil
}

func (s kvService) Incr(ctx context.Context, key string) (int64, error) {
	if err := validateKey(key); err != nil {
		return 0, err
	}
	result, err := s.client.execute(ctx, Operation{Name: "INCR", Kind: "kv", Keys: []string{key}}, func(inner context.Context) (any, error) {
		return s.client.backend.Incr(inner, key)
	})
	if err != nil {
		return 0, err
	}
	return result.(int64), nil
}

func (s kvService) Decr(ctx context.Context, key string) (int64, error) {
	if err := validateKey(key); err != nil {
		return 0, err
	}
	result, err := s.client.execute(ctx, Operation{Name: "DECR", Kind: "kv", Keys: []string{key}}, func(inner context.Context) (any, error) {
		return s.client.backend.Decr(inner, key)
	})
	if err != nil {
		return 0, err
	}
	return result.(int64), nil
}

func (s hashService) Set(ctx context.Context, key string, values map[string]any) (int64, error) {
	if err := validateKey(key); err != nil {
		return 0, err
	}
	if err := validateMap("values", values); err != nil {
		return 0, err
	}
	result, err := s.client.execute(ctx, Operation{Name: "HSET", Kind: "hash", Keys: []string{key}}, func(inner context.Context) (any, error) {
		return s.client.backend.HSet(inner, key, values)
	})
	if err != nil {
		return 0, err
	}
	return result.(int64), nil
}

func (s hashService) Get(ctx context.Context, key, field string) (string, error) {
	if err := validateKey(key); err != nil {
		return "", err
	}
	if err := validateKey(field); err != nil {
		return "", ValidationError{Field: "field", Message: err.Error()}
	}
	result, err := s.client.execute(ctx, Operation{Name: "HGET", Kind: "hash", Keys: []string{key}}, func(inner context.Context) (any, error) {
		return s.client.backend.HGet(inner, key, field)
	})
	if err != nil {
		return "", err
	}
	return result.(string), nil
}

func (s hashService) GetAll(ctx context.Context, key string) (map[string]string, error) {
	if err := validateKey(key); err != nil {
		return nil, err
	}
	result, err := s.client.execute(ctx, Operation{Name: "HGETALL", Kind: "hash", Keys: []string{key}}, func(inner context.Context) (any, error) {
		return s.client.backend.HGetAll(inner, key)
	})
	if err != nil {
		return nil, err
	}
	return result.(map[string]string), nil
}

func (s hashService) MGet(ctx context.Context, key string, fields ...string) ([]any, error) {
	if err := validateKey(key); err != nil {
		return nil, err
	}
	if err := validateStringSlice("fields", fields); err != nil {
		return nil, err
	}
	result, err := s.client.execute(ctx, Operation{Name: "HMGET", Kind: "hash", Keys: []string{key}}, func(inner context.Context) (any, error) {
		return s.client.backend.HMGet(inner, key, fields...)
	})
	if err != nil {
		return nil, err
	}
	return result.([]any), nil
}

func (s hashService) Del(ctx context.Context, key string, fields ...string) (int64, error) {
	if err := validateKey(key); err != nil {
		return 0, err
	}
	if err := validateStringSlice("fields", fields); err != nil {
		return 0, err
	}
	result, err := s.client.execute(ctx, Operation{Name: "HDEL", Kind: "hash", Keys: []string{key}}, func(inner context.Context) (any, error) {
		return s.client.backend.HDel(inner, key, fields...)
	})
	if err != nil {
		return 0, err
	}
	return result.(int64), nil
}

func (s listService) LPush(ctx context.Context, key string, values ...any) (int64, error) {
	if err := validateKey(key); err != nil {
		return 0, err
	}
	result, err := s.client.execute(ctx, Operation{Name: "LPUSH", Kind: "list", Keys: []string{key}}, func(inner context.Context) (any, error) {
		return s.client.backend.LPush(inner, key, values...)
	})
	if err != nil {
		return 0, err
	}
	return result.(int64), nil
}

func (s listService) RPush(ctx context.Context, key string, values ...any) (int64, error) {
	if err := validateKey(key); err != nil {
		return 0, err
	}
	result, err := s.client.execute(ctx, Operation{Name: "RPUSH", Kind: "list", Keys: []string{key}}, func(inner context.Context) (any, error) {
		return s.client.backend.RPush(inner, key, values...)
	})
	if err != nil {
		return 0, err
	}
	return result.(int64), nil
}

func (s listService) LPop(ctx context.Context, key string) (string, error) {
	if err := validateKey(key); err != nil {
		return "", err
	}
	result, err := s.client.execute(ctx, Operation{Name: "LPOP", Kind: "list", Keys: []string{key}}, func(inner context.Context) (any, error) {
		return s.client.backend.LPop(inner, key)
	})
	if err != nil {
		return "", err
	}
	return result.(string), nil
}

func (s listService) RPop(ctx context.Context, key string) (string, error) {
	if err := validateKey(key); err != nil {
		return "", err
	}
	result, err := s.client.execute(ctx, Operation{Name: "RPOP", Kind: "list", Keys: []string{key}}, func(inner context.Context) (any, error) {
		return s.client.backend.RPop(inner, key)
	})
	if err != nil {
		return "", err
	}
	return result.(string), nil
}

func (s listService) Range(ctx context.Context, key string, start, stop int64) ([]string, error) {
	if err := validateKey(key); err != nil {
		return nil, err
	}
	result, err := s.client.execute(ctx, Operation{Name: "LRANGE", Kind: "list", Keys: []string{key}}, func(inner context.Context) (any, error) {
		return s.client.backend.LRange(inner, key, start, stop)
	})
	if err != nil {
		return nil, err
	}
	return result.([]string), nil
}

func (s listService) Len(ctx context.Context, key string) (int64, error) {
	if err := validateKey(key); err != nil {
		return 0, err
	}
	result, err := s.client.execute(ctx, Operation{Name: "LLEN", Kind: "list", Keys: []string{key}}, func(inner context.Context) (any, error) {
		return s.client.backend.LLen(inner, key)
	})
	if err != nil {
		return 0, err
	}
	return result.(int64), nil
}

func (s setService) Add(ctx context.Context, key string, members ...string) (int64, error) {
	if err := validateKey(key); err != nil {
		return 0, err
	}
	if err := validateStringSlice("members", members); err != nil {
		return 0, err
	}
	result, err := s.client.execute(ctx, Operation{Name: "SADD", Kind: "set", Keys: []string{key}}, func(inner context.Context) (any, error) {
		return s.client.backend.SAdd(inner, key, members...)
	})
	if err != nil {
		return 0, err
	}
	return result.(int64), nil
}

func (s setService) Remove(ctx context.Context, key string, members ...string) (int64, error) {
	if err := validateKey(key); err != nil {
		return 0, err
	}
	if err := validateStringSlice("members", members); err != nil {
		return 0, err
	}
	result, err := s.client.execute(ctx, Operation{Name: "SREM", Kind: "set", Keys: []string{key}}, func(inner context.Context) (any, error) {
		return s.client.backend.SRem(inner, key, members...)
	})
	if err != nil {
		return 0, err
	}
	return result.(int64), nil
}

func (s setService) Members(ctx context.Context, key string) ([]string, error) {
	if err := validateKey(key); err != nil {
		return nil, err
	}
	result, err := s.client.execute(ctx, Operation{Name: "SMEMBERS", Kind: "set", Keys: []string{key}}, func(inner context.Context) (any, error) {
		return s.client.backend.SMembers(inner, key)
	})
	if err != nil {
		return nil, err
	}
	return result.([]string), nil
}

func (s setService) IsMember(ctx context.Context, key, member string) (bool, error) {
	if err := validateKey(key); err != nil {
		return false, err
	}
	if err := validateKey(member); err != nil {
		return false, ValidationError{Field: "member", Message: err.Error()}
	}
	result, err := s.client.execute(ctx, Operation{Name: "SISMEMBER", Kind: "set", Keys: []string{key}}, func(inner context.Context) (any, error) {
		return s.client.backend.SIsMember(inner, key, member)
	})
	if err != nil {
		return false, err
	}
	return result.(bool), nil
}

func (s setService) Card(ctx context.Context, key string) (int64, error) {
	if err := validateKey(key); err != nil {
		return 0, err
	}
	result, err := s.client.execute(ctx, Operation{Name: "SCARD", Kind: "set", Keys: []string{key}}, func(inner context.Context) (any, error) {
		return s.client.backend.SCard(inner, key)
	})
	if err != nil {
		return 0, err
	}
	return result.(int64), nil
}

func (s sortedSetService) Add(ctx context.Context, key string, members ...ZMember) (int64, error) {
	if err := validateKey(key); err != nil {
		return 0, err
	}
	result, err := s.client.execute(ctx, Operation{Name: "ZADD", Kind: "zset", Keys: []string{key}}, func(inner context.Context) (any, error) {
		return s.client.backend.ZAdd(inner, key, members...)
	})
	if err != nil {
		return 0, err
	}
	return result.(int64), nil
}

func (s sortedSetService) Range(ctx context.Context, key string, start, stop int64) ([]ZMember, error) {
	if err := validateKey(key); err != nil {
		return nil, err
	}
	result, err := s.client.execute(ctx, Operation{Name: "ZRANGE", Kind: "zset", Keys: []string{key}}, func(inner context.Context) (any, error) {
		return s.client.backend.ZRange(inner, key, start, stop)
	})
	if err != nil {
		return nil, err
	}
	return result.([]contract.ZMember), nil
}

func (s sortedSetService) RevRange(ctx context.Context, key string, start, stop int64) ([]ZMember, error) {
	if err := validateKey(key); err != nil {
		return nil, err
	}
	result, err := s.client.execute(ctx, Operation{Name: "ZREVRANGE", Kind: "zset", Keys: []string{key}}, func(inner context.Context) (any, error) {
		return s.client.backend.ZRevRange(inner, key, start, stop)
	})
	if err != nil {
		return nil, err
	}
	return result.([]contract.ZMember), nil
}

func (s sortedSetService) Remove(ctx context.Context, key string, members ...string) (int64, error) {
	if err := validateKey(key); err != nil {
		return 0, err
	}
	if err := validateStringSlice("members", members); err != nil {
		return 0, err
	}
	result, err := s.client.execute(ctx, Operation{Name: "ZREM", Kind: "zset", Keys: []string{key}}, func(inner context.Context) (any, error) {
		return s.client.backend.ZRem(inner, key, members...)
	})
	if err != nil {
		return 0, err
	}
	return result.(int64), nil
}

func (s sortedSetService) Score(ctx context.Context, key, member string) (float64, error) {
	if err := validateKey(key); err != nil {
		return 0, err
	}
	if err := validateKey(member); err != nil {
		return 0, ValidationError{Field: "member", Message: err.Error()}
	}
	result, err := s.client.execute(ctx, Operation{Name: "ZSCORE", Kind: "zset", Keys: []string{key}}, func(inner context.Context) (any, error) {
		return s.client.backend.ZScore(inner, key, member)
	})
	if err != nil {
		return 0, err
	}
	return result.(float64), nil
}

func (s pubsubService) Publish(ctx context.Context, channel string, payload any) (int64, error) {
	if err := validateKey(channel); err != nil {
		return 0, ValidationError{Field: "channel", Message: err.Error()}
	}
	result, err := s.client.execute(ctx, Operation{Name: "PUBLISH", Kind: "pubsub", Keys: []string{channel}}, func(inner context.Context) (any, error) {
		return s.client.backend.Publish(inner, channel, payload)
	})
	if err != nil {
		return 0, err
	}
	return result.(int64), nil
}

func (s pubsubService) Subscribe(ctx context.Context, channels ...string) (Subscription, error) {
	if err := validateStringSlice("channels", channels); err != nil {
		return nil, err
	}
	result, err := s.client.execute(ctx, Operation{Name: "SUBSCRIBE", Kind: "pubsub", Keys: channels}, func(inner context.Context) (any, error) {
		return s.client.backend.Subscribe(inner, channels...)
	})
	if err != nil {
		return nil, err
	}
	return result.(Subscription), nil
}

func (s streamService) Add(ctx context.Context, stream string, values map[string]any, maxLen int64, approximate bool) (string, error) {
	if err := validateKey(stream); err != nil {
		return "", ValidationError{Field: "stream", Message: err.Error()}
	}
	if err := validateMap("values", values); err != nil {
		return "", err
	}
	result, err := s.client.execute(ctx, Operation{Name: "XADD", Kind: "stream", Keys: []string{stream}}, func(inner context.Context) (any, error) {
		return s.client.backend.XAdd(inner, stream, values, maxLen, approximate)
	})
	if err != nil {
		return "", err
	}
	return result.(string), nil
}

func (s streamService) ReadGroup(ctx context.Context, request StreamReadRequest) ([]StreamReadResponse, error) {
	if err := validateStringSlice("streams", request.Streams); err != nil {
		return nil, err
	}
	result, err := s.client.execute(ctx, Operation{Name: "XREADGROUP", Kind: "stream", Keys: request.Streams}, func(inner context.Context) (any, error) {
		return s.client.backend.XReadGroup(inner, request)
	})
	if err != nil {
		return nil, err
	}
	return result.([]contract.StreamReadResponse), nil
}

func (s streamService) Ack(ctx context.Context, stream, group string, ids ...string) (int64, error) {
	if err := validateKey(stream); err != nil {
		return 0, err
	}
	if err := validateKey(group); err != nil {
		return 0, ValidationError{Field: "group", Message: err.Error()}
	}
	if err := validateStringSlice("ids", ids); err != nil {
		return 0, err
	}
	result, err := s.client.execute(ctx, Operation{Name: "XACK", Kind: "stream", Keys: []string{stream}}, func(inner context.Context) (any, error) {
		return s.client.backend.XAck(inner, stream, group, ids...)
	})
	if err != nil {
		return 0, err
	}
	return result.(int64), nil
}

func (s streamService) Delete(ctx context.Context, stream string, ids ...string) (int64, error) {
	if err := validateKey(stream); err != nil {
		return 0, err
	}
	if err := validateStringSlice("ids", ids); err != nil {
		return 0, err
	}
	result, err := s.client.execute(ctx, Operation{Name: "XDEL", Kind: "stream", Keys: []string{stream}}, func(inner context.Context) (any, error) {
		return s.client.backend.XDel(inner, stream, ids...)
	})
	if err != nil {
		return 0, err
	}
	return result.(int64), nil
}

func (s streamService) CreateGroup(ctx context.Context, stream, group, start string) error {
	if err := validateKey(stream); err != nil {
		return err
	}
	if err := validateKey(group); err != nil {
		return ValidationError{Field: "group", Message: err.Error()}
	}
	_, err := s.client.execute(ctx, Operation{Name: "XGROUP", Kind: "stream", Keys: []string{stream}}, func(inner context.Context) (any, error) {
		return nil, s.client.backend.XGroupCreateMkStream(inner, stream, group, start)
	})
	return err
}

func (s scriptService) Eval(ctx context.Context, script string, keys []string, args ...any) (any, error) {
	result, err := s.client.execute(ctx, Operation{Name: "EVAL", Kind: "script", Keys: keys}, func(inner context.Context) (any, error) {
		return s.client.backend.Eval(inner, script, keys, args...)
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (s scriptService) EvalSHA(ctx context.Context, sha string, keys []string, args ...any) (any, error) {
	result, err := s.client.execute(ctx, Operation{Name: "EVALSHA", Kind: "script", Keys: keys}, func(inner context.Context) (any, error) {
		return s.client.backend.EvalSHA(inner, sha, keys, args...)
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (s scriptService) Load(ctx context.Context, script string) (string, error) {
	result, err := s.client.execute(ctx, Operation{Name: "SCRIPTLOAD", Kind: "script"}, func(inner context.Context) (any, error) {
		return s.client.backend.ScriptLoad(inner, script)
	})
	if err != nil {
		return "", err
	}
	return result.(string), nil
}

func (s bulkService) Get(ctx context.Context, keys []string, batchSize int) (map[string]any, error) {
	if err := validateKeys(keys); err != nil {
		return nil, err
	}
	if batchSize <= 0 {
		batchSize = 128
	}

	result := make(map[string]any, len(keys))
	for _, batch := range chunk(keys, batchSize) {
		values, err := s.client.KV().MGet(ctx, batch...)
		if err != nil {
			return nil, err
		}
		for index, key := range batch {
			result[key] = values[index]
		}
	}
	return result, nil
}

func (s bulkService) Delete(ctx context.Context, keys []string, batchSize int) (int64, error) {
	if err := validateKeys(keys); err != nil {
		return 0, err
	}
	if batchSize <= 0 {
		batchSize = 128
	}

	var total int64
	for _, batch := range chunk(keys, batchSize) {
		deleted, err := s.client.KV().Del(ctx, batch...)
		if err != nil {
			return total, err
		}
		total += deleted
	}
	return total, nil
}

func (s bulkService) Expire(ctx context.Context, keys []string, ttl time.Duration, batchSize int) (int64, error) {
	if err := validateKeys(keys); err != nil {
		return 0, err
	}
	if batchSize <= 0 {
		batchSize = 128
	}

	var total int64
	for _, batch := range chunk(keys, batchSize) {
		results, err := s.client.Pipeline(ctx, func(builder PipelineBuilder) {
			for _, key := range batch {
				builder.Expire(key, ttl)
			}
		})
		if err != nil {
			return total, err
		}
		for _, item := range results {
			if item.Err == nil {
				total++
			}
		}
	}
	return total, nil
}

func chunk(values []string, batchSize int) [][]string {
	output := make([][]string, 0, (len(values)/batchSize)+1)
	for start := 0; start < len(values); start += batchSize {
		end := start + batchSize
		if end > len(values) {
			end = len(values)
		}
		output = append(output, values[start:end])
	}
	return output
}
