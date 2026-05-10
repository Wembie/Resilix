package resilix

import (
	"context"
	"time"

	"github.com/resilix/resilix/sdk/go/internal/contract"
)

type PipelineBuilder interface {
	Set(key string, value any, ttl time.Duration)
	Get(key string)
	Del(keys ...string)
	Incr(key string)
	Decr(key string)
	Expire(key string, ttl time.Duration)
	HSet(key string, values map[string]any)
	SAdd(key string, members ...string)
	ZAdd(key string, members ...ZMember)
	XAdd(stream string, values map[string]any, maxLen int64, approximate bool)
	MSet(values map[string]any)
}

type pipelineBuilder struct {
	commands []contract.PipelineCommand
}

func (b *pipelineBuilder) Set(key string, value any, ttl time.Duration) {
	b.commands = append(b.commands, contract.PipelineCommand{Name: "SET", Key: key, Value: value, TTL: ttl})
}

func (b *pipelineBuilder) Get(key string) {
	b.commands = append(b.commands, contract.PipelineCommand{Name: "GET", Key: key})
}

func (b *pipelineBuilder) Del(keys ...string) {
	b.commands = append(b.commands, contract.PipelineCommand{Name: "DEL", Keys: keys})
}

func (b *pipelineBuilder) Incr(key string) {
	b.commands = append(b.commands, contract.PipelineCommand{Name: "INCR", Key: key})
}

func (b *pipelineBuilder) Decr(key string) {
	b.commands = append(b.commands, contract.PipelineCommand{Name: "DECR", Key: key})
}

func (b *pipelineBuilder) Expire(key string, ttl time.Duration) {
	b.commands = append(b.commands, contract.PipelineCommand{Name: "EXPIRE", Key: key, TTL: ttl})
}

func (b *pipelineBuilder) HSet(key string, values map[string]any) {
	b.commands = append(b.commands, contract.PipelineCommand{Name: "HSET", Key: key, Values: values})
}

func (b *pipelineBuilder) SAdd(key string, members ...string) {
	b.commands = append(b.commands, contract.PipelineCommand{Name: "SADD", Key: key, Members: members})
}

func (b *pipelineBuilder) ZAdd(key string, members ...ZMember) {
	b.commands = append(b.commands, contract.PipelineCommand{Name: "ZADD", Key: key, ZMembers: members})
}

func (b *pipelineBuilder) XAdd(stream string, values map[string]any, maxLen int64, approximate bool) {
	b.commands = append(b.commands, contract.PipelineCommand{
		Name:         "XADD",
		Key:          stream,
		StreamValues: values,
		MaxLen:       maxLen,
		Approximate:  approximate,
	})
}

func (b *pipelineBuilder) MSet(values map[string]any) {
	b.commands = append(b.commands, contract.PipelineCommand{Name: "MSET", Values: values})
}

func (b *pipelineBuilder) snapshot() []contract.PipelineCommand {
	copied := make([]contract.PipelineCommand, len(b.commands))
	copy(copied, b.commands)
	return copied
}

func (c *RedisClient) Pipeline(ctx context.Context, fn func(PipelineBuilder)) ([]CommandResult, error) {
	builder := &pipelineBuilder{}
	if fn != nil {
		fn(builder)
	}

	result, err := c.execute(ctx, Operation{Name: "PIPELINE", Kind: "pipeline"}, func(inner context.Context) (any, error) {
		return c.backend.Pipeline(inner, builder.snapshot())
	})
	if err != nil {
		return nil, err
	}
	return result.([]contract.CommandResult), nil
}

func (c *RedisClient) Transaction(ctx context.Context, watchKeys []string, fn func(PipelineBuilder)) ([]CommandResult, error) {
	builder := &pipelineBuilder{}
	if fn != nil {
		fn(builder)
	}

	result, err := c.execute(ctx, Operation{Name: "TRANSACTION", Kind: "transaction", Keys: watchKeys}, func(inner context.Context) (any, error) {
		return c.backend.Transaction(inner, watchKeys, builder.snapshot())
	})
	if err != nil {
		return nil, err
	}
	return result.([]contract.CommandResult), nil
}
