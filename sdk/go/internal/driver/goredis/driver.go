package goredis

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/Wembie/Resilix/sdk/go/internal/contract"
)

var (
	errUnsupportedPipelineCommand = errors.New("resilix: unsupported pipeline command")
	errMissingPipelineResult      = errors.New("resilix: pipeline command did not produce a result")
	errBitOpNOTArgs               = errors.New("resilix: BitOp NOT requires exactly one source key")
	errUnknownBitOp               = errors.New("resilix: unknown BitOp operation")
)

type Driver struct {
	client redis.UniversalClient
}

func New(options *redis.UniversalOptions) *Driver {
	return &Driver{
		client: redis.NewUniversalClient(options),
	}
}

func (d *Driver) Close() error {
	return d.client.Close()
}

func (d *Driver) Ping(ctx context.Context) error {
	return normalizeError(d.client.Ping(ctx).Err())
}

func (d *Driver) PoolStats() contract.PoolStats {
	stats := d.client.PoolStats()
	return contract.PoolStats{
		Hits:       stats.Hits,
		Misses:     stats.Misses,
		Timeouts:   stats.Timeouts,
		TotalConns: stats.TotalConns,
		IdleConns:  stats.IdleConns,
		StaleConns: stats.StaleConns,
	}
}

func (d *Driver) Get(ctx context.Context, key string) (string, error) {
	value, err := d.client.Get(ctx, key).Result()
	return value, normalizeError(err)
}

func (d *Driver) Set(ctx context.Context, key string, value any, ttlSeconds float64) error {
	return normalizeError(d.client.Set(ctx, key, value, time.Duration(ttlSeconds*float64(time.Second))).Err())
}

func (d *Driver) MGet(ctx context.Context, keys ...string) ([]any, error) {
	value, err := d.client.MGet(ctx, keys...).Result()
	return value, normalizeError(err)
}

func (d *Driver) MSet(ctx context.Context, values map[string]any) error {
	return normalizeError(d.client.MSet(ctx, values).Err())
}

func (d *Driver) Del(ctx context.Context, keys ...string) (int64, error) {
	value, err := d.client.Del(ctx, keys...).Result()
	return value, normalizeError(err)
}

func (d *Driver) Exists(ctx context.Context, keys ...string) (int64, error) {
	value, err := d.client.Exists(ctx, keys...).Result()
	return value, normalizeError(err)
}

func (d *Driver) Expire(ctx context.Context, key string, ttlSeconds float64) (bool, error) {
	value, err := d.client.Expire(ctx, key, time.Duration(ttlSeconds*float64(time.Second))).Result()
	return value, normalizeError(err)
}

func (d *Driver) TTL(ctx context.Context, key string) (float64, error) {
	value, err := d.client.TTL(ctx, key).Result()
	return value.Seconds(), normalizeError(err)
}

func (d *Driver) Incr(ctx context.Context, key string) (int64, error) {
	value, err := d.client.Incr(ctx, key).Result()
	return value, normalizeError(err)
}

func (d *Driver) Decr(ctx context.Context, key string) (int64, error) {
	value, err := d.client.Decr(ctx, key).Result()
	return value, normalizeError(err)
}

func (d *Driver) HSet(ctx context.Context, key string, values map[string]any) (int64, error) {
	value, err := d.client.HSet(ctx, key, values).Result()
	return value, normalizeError(err)
}

func (d *Driver) HGet(ctx context.Context, key, field string) (string, error) {
	value, err := d.client.HGet(ctx, key, field).Result()
	return value, normalizeError(err)
}

func (d *Driver) HGetAll(ctx context.Context, key string) (map[string]string, error) {
	value, err := d.client.HGetAll(ctx, key).Result()
	return value, normalizeError(err)
}

func (d *Driver) HMGet(ctx context.Context, key string, fields ...string) ([]any, error) {
	value, err := d.client.HMGet(ctx, key, fields...).Result()
	return value, normalizeError(err)
}

func (d *Driver) HDel(ctx context.Context, key string, fields ...string) (int64, error) {
	value, err := d.client.HDel(ctx, key, fields...).Result()
	return value, normalizeError(err)
}

func (d *Driver) LPush(ctx context.Context, key string, values ...any) (int64, error) {
	value, err := d.client.LPush(ctx, key, values...).Result()
	return value, normalizeError(err)
}

func (d *Driver) RPush(ctx context.Context, key string, values ...any) (int64, error) {
	value, err := d.client.RPush(ctx, key, values...).Result()
	return value, normalizeError(err)
}

func (d *Driver) LPop(ctx context.Context, key string) (string, error) {
	value, err := d.client.LPop(ctx, key).Result()
	return value, normalizeError(err)
}

func (d *Driver) RPop(ctx context.Context, key string) (string, error) {
	value, err := d.client.RPop(ctx, key).Result()
	return value, normalizeError(err)
}

func (d *Driver) LRange(ctx context.Context, key string, start, stop int64) ([]string, error) {
	value, err := d.client.LRange(ctx, key, start, stop).Result()
	return value, normalizeError(err)
}

func (d *Driver) LLen(ctx context.Context, key string) (int64, error) {
	value, err := d.client.LLen(ctx, key).Result()
	return value, normalizeError(err)
}

func (d *Driver) SAdd(ctx context.Context, key string, members ...string) (int64, error) {
	args := make([]any, 0, len(members))
	for _, member := range members {
		args = append(args, member)
	}
	value, err := d.client.SAdd(ctx, key, args...).Result()
	return value, normalizeError(err)
}

func (d *Driver) SRem(ctx context.Context, key string, members ...string) (int64, error) {
	args := make([]any, 0, len(members))
	for _, member := range members {
		args = append(args, member)
	}
	value, err := d.client.SRem(ctx, key, args...).Result()
	return value, normalizeError(err)
}

func (d *Driver) SMembers(ctx context.Context, key string) ([]string, error) {
	value, err := d.client.SMembers(ctx, key).Result()
	return value, normalizeError(err)
}

func (d *Driver) SIsMember(ctx context.Context, key, member string) (bool, error) {
	value, err := d.client.SIsMember(ctx, key, member).Result()
	return value, normalizeError(err)
}

func (d *Driver) SCard(ctx context.Context, key string) (int64, error) {
	value, err := d.client.SCard(ctx, key).Result()
	return value, normalizeError(err)
}

func (d *Driver) ZAdd(ctx context.Context, key string, members ...contract.ZMember) (int64, error) {
	zMembers := make([]redis.Z, 0, len(members))
	for _, member := range members {
		zMembers = append(zMembers, redis.Z{Score: member.Score, Member: member.Member})
	}
	value, err := d.client.ZAdd(ctx, key, zMembers...).Result()
	return value, normalizeError(err)
}

func (d *Driver) ZRange(ctx context.Context, key string, start, stop int64) ([]contract.ZMember, error) {
	value, err := d.client.ZRangeWithScores(ctx, key, start, stop).Result()
	return toContractZMembers(value), normalizeError(err)
}

func (d *Driver) ZRevRange(ctx context.Context, key string, start, stop int64) ([]contract.ZMember, error) {
	value, err := d.client.ZRevRangeWithScores(ctx, key, start, stop).Result()
	return toContractZMembers(value), normalizeError(err)
}

func (d *Driver) ZRem(ctx context.Context, key string, members ...string) (int64, error) {
	args := make([]any, 0, len(members))
	for _, member := range members {
		args = append(args, member)
	}
	value, err := d.client.ZRem(ctx, key, args...).Result()
	return value, normalizeError(err)
}

func (d *Driver) ZScore(ctx context.Context, key, member string) (float64, error) {
	value, err := d.client.ZScore(ctx, key, member).Result()
	return value, normalizeError(err)
}

func (d *Driver) Publish(ctx context.Context, channel string, payload any) (int64, error) {
	value, err := d.client.Publish(ctx, channel, payload).Result()
	return value, normalizeError(err)
}

func (d *Driver) Subscribe(ctx context.Context, channels ...string) (contract.Subscription, error) {
	pubsub := d.client.Subscribe(ctx, channels...)
	if err := normalizeError(pubsub.Ping(ctx)); err != nil {
		return nil, err
	}

	output := make(chan contract.Message, 128)
	source := pubsub.Channel()

	go func() {
		defer close(output)
		for message := range source {
			output <- contract.Message{
				Channel: message.Channel,
				Pattern: message.Pattern,
				Payload: message.Payload,
			}
		}
	}()

	return subscription{
		pubsub:   pubsub,
		messages: output,
	}, nil
}

func (d *Driver) XAdd(ctx context.Context, stream string, values map[string]any, maxLen int64, approximate bool) (string, error) {
	value, err := d.client.XAdd(ctx, &redis.XAddArgs{
		Stream: stream,
		MaxLen: maxLen,
		Approx: approximate,
		Values: values,
	}).Result()
	return value, normalizeError(err)
}

func (d *Driver) XReadGroup(ctx context.Context, request contract.StreamReadRequest) ([]contract.StreamReadResponse, error) {
	streams := make([]string, 0, len(request.Streams)+len(request.IDs))
	streams = append(streams, request.Streams...)
	streams = append(streams, request.IDs...)

	value, err := d.client.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    request.Group,
		Consumer: request.Consumer,
		Streams:  streams,
		Count:    request.Count,
		Block:    request.Block,
		NoAck:    request.NoAck,
	}).Result()
	return toContractStreams(value), normalizeError(err)
}

func (d *Driver) XAck(ctx context.Context, stream, group string, ids ...string) (int64, error) {
	value, err := d.client.XAck(ctx, stream, group, ids...).Result()
	return value, normalizeError(err)
}

func (d *Driver) XDel(ctx context.Context, stream string, ids ...string) (int64, error) {
	value, err := d.client.XDel(ctx, stream, ids...).Result()
	return value, normalizeError(err)
}

func (d *Driver) XGroupCreateMkStream(ctx context.Context, stream, group, start string) error {
	err := d.client.XGroupCreateMkStream(ctx, stream, group, start).Err()
	if err != nil && strings.Contains(err.Error(), "BUSYGROUP") {
		return nil
	}
	return normalizeError(err)
}

func (d *Driver) Eval(ctx context.Context, script string, keys []string, args ...any) (any, error) {
	value, err := d.client.Eval(ctx, script, keys, args...).Result()
	return value, normalizeError(err)
}

func (d *Driver) EvalSHA(ctx context.Context, sha string, keys []string, args ...any) (any, error) {
	value, err := d.client.EvalSha(ctx, sha, keys, args...).Result()
	return value, normalizeError(err)
}

func (d *Driver) ScriptLoad(ctx context.Context, script string) (string, error) {
	value, err := d.client.ScriptLoad(ctx, script).Result()
	return value, normalizeError(err)
}

func (d *Driver) Pipeline(ctx context.Context, commands []contract.PipelineCommand) ([]contract.CommandResult, error) {
	executed, err := d.client.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		for _, command := range commands {
			if applyErr := applyCommand(ctx, pipe, command); applyErr != nil {
				return applyErr
			}
		}
		return nil
	})
	return toCommandResults(commands, executed), normalizeError(err)
}

func (d *Driver) Transaction(ctx context.Context, watchKeys []string, commands []contract.PipelineCommand) ([]contract.CommandResult, error) {
	var executed []redis.Cmder

	if len(watchKeys) == 0 {
		result, err := d.client.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
			for _, command := range commands {
				if applyErr := applyCommand(ctx, pipe, command); applyErr != nil {
					return applyErr
				}
			}
			return nil
		})
		return toCommandResults(commands, result), normalizeError(err)
	}

	err := d.client.Watch(ctx, func(tx *redis.Tx) error {
		var txErr error
		executed, txErr = tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
			for _, command := range commands {
				if applyErr := applyCommand(ctx, pipe, command); applyErr != nil {
					return applyErr
				}
			}
			return nil
		})
		return txErr
	}, watchKeys...)

	return toCommandResults(commands, executed), normalizeError(err)
}

func (d *Driver) Scan(ctx context.Context, cursor uint64, pattern string, count int64) ([]string, uint64, error) {
	keys, next, err := d.client.Scan(ctx, cursor, pattern, count).Result()
	return keys, next, normalizeError(err)
}

func (d *Driver) GeoAdd(ctx context.Context, key string, members ...contract.GeoMember) (int64, error) {
	args := make([]*redis.GeoLocation, 0, len(members))
	for _, m := range members {
		args = append(args, &redis.GeoLocation{
			Longitude: m.Longitude,
			Latitude:  m.Latitude,
			Name:      m.Name,
		})
	}
	value, err := d.client.GeoAdd(ctx, key, args...).Result()
	return value, normalizeError(err)
}

func (d *Driver) GeoDist(ctx context.Context, key, member1, member2, unit string) (float64, error) {
	value, err := d.client.GeoDist(ctx, key, member1, member2, unit).Result()
	return value, normalizeError(err)
}

func (d *Driver) GeoPos(ctx context.Context, key string, members ...string) ([]*contract.GeoPosition, error) {
	positions, err := d.client.GeoPos(ctx, key, members...).Result()
	if err != nil {
		return nil, normalizeError(err)
	}
	result := make([]*contract.GeoPosition, len(positions))
	for i, pos := range positions {
		if pos != nil {
			result[i] = &contract.GeoPosition{Longitude: pos.Longitude, Latitude: pos.Latitude}
		}
	}
	return result, nil
}

func (d *Driver) GeoSearch(ctx context.Context, key string, query contract.GeoSearchQuery) ([]contract.GeoSearchResult, error) {
	args := &redis.GeoSearchQuery{
		Sort:     query.Sort,
		Count:    int(query.Count),
		CountAny: query.Any,
	}
	if query.FromMember != "" {
		args.Member = query.FromMember
	} else if query.FromCoord != nil {
		args.Longitude = query.FromCoord.Longitude
		args.Latitude = query.FromCoord.Latitude
	}
	if query.ByBox != nil {
		args.BoxWidth = query.ByBox.Width
		args.BoxHeight = query.ByBox.Height
		args.BoxUnit = query.Unit
	} else {
		args.Radius = query.ByRadius
		args.RadiusUnit = query.Unit
	}

	locations, err := d.client.GeoSearchLocation(ctx, key, &redis.GeoSearchLocationQuery{
		GeoSearchQuery: *args,
		WithCoord:      query.WithCoord,
		WithDist:       query.WithDist,
	}).Result()
	if err != nil {
		return nil, normalizeError(err)
	}

	results := make([]contract.GeoSearchResult, 0, len(locations))
	for _, loc := range locations {
		r := contract.GeoSearchResult{
			Name:     loc.Name,
			Distance: loc.Dist,
			GeoHash:  loc.GeoHash,
		}
		if query.WithCoord {
			r.Coord = &contract.GeoPosition{Longitude: loc.Longitude, Latitude: loc.Latitude}
		}
		results = append(results, r)
	}
	return results, nil
}

func (d *Driver) PFAdd(ctx context.Context, key string, elements ...any) (bool, error) {
	value, err := d.client.PFAdd(ctx, key, elements...).Result()
	return value == 1, normalizeError(err)
}

func (d *Driver) PFCount(ctx context.Context, keys ...string) (int64, error) {
	value, err := d.client.PFCount(ctx, keys...).Result()
	return value, normalizeError(err)
}

func (d *Driver) PFMerge(ctx context.Context, dest string, keys ...string) error {
	return normalizeError(d.client.PFMerge(ctx, dest, keys...).Err())
}

func (d *Driver) SetBit(ctx context.Context, key string, offset int64, value int) (int64, error) {
	v, err := d.client.SetBit(ctx, key, offset, value).Result()
	return v, normalizeError(err)
}

func (d *Driver) GetBit(ctx context.Context, key string, offset int64) (int64, error) {
	value, err := d.client.GetBit(ctx, key, offset).Result()
	return value, normalizeError(err)
}

func (d *Driver) BitCount(ctx context.Context, key string, start, end int64) (int64, error) {
	value, err := d.client.BitCount(ctx, key, &redis.BitCount{Start: start, End: end}).Result()
	return value, normalizeError(err)
}

func (d *Driver) BitOp(ctx context.Context, op, destKey string, keys ...string) (int64, error) {
	var (
		value int64
		err   error
	)
	switch strings.ToUpper(op) {
	case "AND":
		value, err = d.client.BitOpAnd(ctx, destKey, keys...).Result()
	case "OR":
		value, err = d.client.BitOpOr(ctx, destKey, keys...).Result()
	case "XOR":
		value, err = d.client.BitOpXor(ctx, destKey, keys...).Result()
	case "NOT":
		if len(keys) == 0 {
			return 0, errBitOpNOTArgs
		}
		value, err = d.client.BitOpNot(ctx, destKey, keys[0]).Result()
	default:
		return 0, fmt.Errorf("%w: %s", errUnknownBitOp, op)
	}
	return value, normalizeError(err)
}

func (d *Driver) BitPos(ctx context.Context, key string, bit int, pos ...int64) (int64, error) {
	switch len(pos) {
	case 0:
		value, err := d.client.BitPos(ctx, key, int64(bit)).Result()
		return value, normalizeError(err)
	case 1:
		value, err := d.client.BitPosSpan(ctx, key, int8(bit), pos[0], -1, "BYTE").Result()
		return value, normalizeError(err)
	default:
		value, err := d.client.BitPosSpan(ctx, key, int8(bit), pos[0], pos[1], "BYTE").Result()
		return value, normalizeError(err)
	}
}

func (d *Driver) LMPop(ctx context.Context, count int64, direction string, keys ...string) (string, []string, error) {
	key, values, err := d.client.LMPop(ctx, direction, count, keys...).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return "", nil, nil
		}
		return "", nil, normalizeError(err)
	}
	return key, values, nil
}

func (d *Driver) ZMPop(ctx context.Context, count int64, order string, keys ...string) (string, []contract.ZMember, error) {
	key, zs, err := d.client.ZMPop(ctx, order, count, keys...).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return "", nil, nil
		}
		return "", nil, normalizeError(err)
	}
	return key, toContractZMembers(zs), nil
}

type subscription struct {
	pubsub   *redis.PubSub
	messages <-chan contract.Message
}

func (s subscription) Messages() <-chan contract.Message {
	return s.messages
}

func (s subscription) Close() error {
	return s.pubsub.Close()
}

func normalizeError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, redis.Nil) {
		return contract.ErrKeyNotFound
	}
	return err
}

func applyCommand(ctx context.Context, pipe redis.Pipeliner, command contract.PipelineCommand) error {
	switch strings.ToUpper(command.Name) {
	case "SET":
		pipe.Set(ctx, command.Key, command.Value, command.TTL)
	case "GET":
		pipe.Get(ctx, command.Key)
	case "DEL":
		pipe.Del(ctx, command.Keys...)
	case "INCR":
		pipe.Incr(ctx, command.Key)
	case "DECR":
		pipe.Decr(ctx, command.Key)
	case "EXPIRE":
		pipe.Expire(ctx, command.Key, command.TTL)
	case "HSET":
		pipe.HSet(ctx, command.Key, command.Values)
	case "SADD":
		members := make([]any, 0, len(command.Members))
		for _, member := range command.Members {
			members = append(members, member)
		}
		pipe.SAdd(ctx, command.Key, members...)
	case "ZADD":
		members := make([]redis.Z, 0, len(command.ZMembers))
		for _, member := range command.ZMembers {
			members = append(members, redis.Z{Score: member.Score, Member: member.Member})
		}
		pipe.ZAdd(ctx, command.Key, members...)
	case "XADD":
		pipe.XAdd(ctx, &redis.XAddArgs{
			Stream: command.Key,
			Values: command.StreamValues,
			MaxLen: command.MaxLen,
			Approx: command.Approximate,
		})
	case "MSET":
		pipe.MSet(ctx, command.Values)
	default:
		return fmt.Errorf("%w: %s", errUnsupportedPipelineCommand, command.Name)
	}
	return nil
}

func toContractZMembers(values []redis.Z) []contract.ZMember {
	output := make([]contract.ZMember, 0, len(values))
	for _, value := range values {
		output = append(output, contract.ZMember{
			Score:  value.Score,
			Member: fmt.Sprint(value.Member),
		})
	}
	return output
}

func toContractStreams(values []redis.XStream) []contract.StreamReadResponse {
	output := make([]contract.StreamReadResponse, 0, len(values))
	for _, stream := range values {
		entries := make([]contract.StreamEntry, 0, len(stream.Messages))
		for _, message := range stream.Messages {
			entries = append(entries, contract.StreamEntry{
				ID:     message.ID,
				Values: message.Values,
			})
		}
		output = append(output, contract.StreamReadResponse{
			Stream:  stream.Stream,
			Entries: entries,
		})
	}
	return output
}

func toCommandResults(commands []contract.PipelineCommand, executed []redis.Cmder) []contract.CommandResult {
	results := make([]contract.CommandResult, 0, len(executed))
	for index, command := range commands {
		if index >= len(executed) {
			results = append(results, contract.CommandResult{
				Name: command.Name,
				Key:  pipelineKey(command),
				Err:  fmt.Errorf("%w: %s", errMissingPipelineResult, command.Name),
			})
			continue
		}

		cmder := executed[index]
		results = append(results, contract.CommandResult{
			Name:  command.Name,
			Key:   pipelineKey(command),
			Value: extractValue(cmder),
			Err:   normalizeError(cmder.Err()),
		})
	}
	return results
}

func pipelineKey(command contract.PipelineCommand) string {
	if command.Key != "" {
		return command.Key
	}
	if len(command.Keys) > 0 {
		return command.Keys[0]
	}
	return ""
}

func extractValue(command redis.Cmder) any {
	switch value := command.(type) {
	case *redis.StatusCmd:
		return value.Val()
	case *redis.StringCmd:
		return value.Val()
	case *redis.IntCmd:
		return value.Val()
	case *redis.BoolCmd:
		return value.Val()
	case *redis.DurationCmd:
		return value.Val()
	case *redis.FloatCmd:
		return value.Val()
	case *redis.StringSliceCmd:
		return value.Val()
	case *redis.MapStringStringCmd:
		return value.Val()
	case *redis.ZSliceCmd:
		return toContractZMembers(value.Val())
	case *redis.SliceCmd:
		return value.Val()
	default:
		return command.String()
	}
}
