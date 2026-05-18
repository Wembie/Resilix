package resilix

import "context"

type BitOps interface {
	Set(ctx context.Context, key string, offset int64, value int) (int64, error)
	Get(ctx context.Context, key string, offset int64) (int64, error)
	Count(ctx context.Context, key string, start, end int64) (int64, error)
	Op(ctx context.Context, op, destKey string, keys ...string) (int64, error)
	Pos(ctx context.Context, key string, bit int, pos ...int64) (int64, error)
}

type bitService struct{ client *RedisClient }

func (s bitService) Set(ctx context.Context, key string, offset int64, value int) (int64, error) {
	if err := validateKey(key); err != nil {
		return 0, err
	}
	result, err := s.client.execute(ctx, Operation{Name: "SETBIT", Kind: "bit", Keys: []string{key}}, func(inner context.Context) (any, error) {
		return s.client.backend.SetBit(inner, key, offset, value)
	})
	if err != nil {
		return 0, err
	}
	return result.(int64), nil
}

func (s bitService) Get(ctx context.Context, key string, offset int64) (int64, error) {
	if err := validateKey(key); err != nil {
		return 0, err
	}
	result, err := s.client.execute(ctx, Operation{Name: "GETBIT", Kind: "bit", Keys: []string{key}}, func(inner context.Context) (any, error) {
		return s.client.backend.GetBit(inner, key, offset)
	})
	if err != nil {
		return 0, err
	}
	return result.(int64), nil
}

func (s bitService) Count(ctx context.Context, key string, start, end int64) (int64, error) {
	if err := validateKey(key); err != nil {
		return 0, err
	}
	result, err := s.client.execute(ctx, Operation{Name: "BITCOUNT", Kind: "bit", Keys: []string{key}}, func(inner context.Context) (any, error) {
		return s.client.backend.BitCount(inner, key, start, end)
	})
	if err != nil {
		return 0, err
	}
	return result.(int64), nil
}

func (s bitService) Op(ctx context.Context, op, destKey string, keys ...string) (int64, error) {
	if err := validateKey(destKey); err != nil {
		return 0, err
	}
	result, err := s.client.execute(ctx, Operation{Name: "BITOP", Kind: "bit", Keys: append([]string{destKey}, keys...)}, func(inner context.Context) (any, error) {
		return s.client.backend.BitOp(inner, op, destKey, keys...)
	})
	if err != nil {
		return 0, err
	}
	return result.(int64), nil
}

func (s bitService) Pos(ctx context.Context, key string, bit int, pos ...int64) (int64, error) {
	if err := validateKey(key); err != nil {
		return 0, err
	}
	result, err := s.client.execute(ctx, Operation{Name: "BITPOS", Kind: "bit", Keys: []string{key}}, func(inner context.Context) (any, error) {
		return s.client.backend.BitPos(inner, key, bit, pos...)
	})
	if err != nil {
		return 0, err
	}
	return result.(int64), nil
}
