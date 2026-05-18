package resilix

import "context"

type HyperLogLog interface {
	Add(ctx context.Context, key string, elements ...any) (bool, error)
	Count(ctx context.Context, keys ...string) (int64, error)
	Merge(ctx context.Context, dest string, keys ...string) error
}

type hllService struct{ client *RedisClient }

func (s hllService) Add(ctx context.Context, key string, elements ...any) (bool, error) {
	if err := validateKey(key); err != nil {
		return false, err
	}
	result, err := s.client.execute(ctx, Operation{Name: "PFADD", Kind: "hll", Keys: []string{key}}, func(inner context.Context) (any, error) {
		return s.client.backend.PFAdd(inner, key, elements...)
	})
	if err != nil {
		return false, err
	}
	return result.(bool), nil
}

func (s hllService) Count(ctx context.Context, keys ...string) (int64, error) {
	if err := validateKeys(keys); err != nil {
		return 0, err
	}
	result, err := s.client.execute(ctx, Operation{Name: "PFCOUNT", Kind: "hll", Keys: keys}, func(inner context.Context) (any, error) {
		return s.client.backend.PFCount(inner, keys...)
	})
	if err != nil {
		return 0, err
	}
	return result.(int64), nil
}

func (s hllService) Merge(ctx context.Context, dest string, keys ...string) error {
	if err := validateKey(dest); err != nil {
		return err
	}
	_, err := s.client.execute(ctx, Operation{Name: "PFMERGE", Kind: "hll", Keys: append([]string{dest}, keys...)}, func(inner context.Context) (any, error) {
		return nil, s.client.backend.PFMerge(inner, dest, keys...)
	})
	return err
}
