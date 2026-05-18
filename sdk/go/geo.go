package resilix

import (
	"context"

	"github.com/Wembie/Resilix/sdk/go/internal/contract"
)

type (
	GeoMember       = contract.GeoMember
	GeoPosition     = contract.GeoPosition
	GeoSearchQuery  = contract.GeoSearchQuery
	GeoBox          = contract.GeoBox
	GeoSearchResult = contract.GeoSearchResult
)

type Geo interface {
	Add(ctx context.Context, key string, members ...GeoMember) (int64, error)
	Dist(ctx context.Context, key, member1, member2, unit string) (float64, error)
	Pos(ctx context.Context, key string, members ...string) ([]*GeoPosition, error)
	Search(ctx context.Context, key string, query GeoSearchQuery) ([]GeoSearchResult, error)
}

type geoService struct{ client *RedisClient }

func (s geoService) Add(ctx context.Context, key string, members ...GeoMember) (int64, error) {
	if err := validateKey(key); err != nil {
		return 0, err
	}
	result, err := s.client.execute(ctx, Operation{Name: "GEOADD", Kind: "geo", Keys: []string{key}}, func(inner context.Context) (any, error) {
		return s.client.backend.GeoAdd(inner, key, members...)
	})
	if err != nil {
		return 0, err
	}
	return result.(int64), nil
}

func (s geoService) Dist(ctx context.Context, key, member1, member2, unit string) (float64, error) {
	if err := validateKey(key); err != nil {
		return 0, err
	}
	result, err := s.client.execute(ctx, Operation{Name: "GEODIST", Kind: "geo", Keys: []string{key}}, func(inner context.Context) (any, error) {
		return s.client.backend.GeoDist(inner, key, member1, member2, unit)
	})
	if err != nil {
		return 0, err
	}
	return result.(float64), nil
}

func (s geoService) Pos(ctx context.Context, key string, members ...string) ([]*GeoPosition, error) {
	if err := validateKey(key); err != nil {
		return nil, err
	}
	result, err := s.client.execute(ctx, Operation{Name: "GEOPOS", Kind: "geo", Keys: []string{key}}, func(inner context.Context) (any, error) {
		return s.client.backend.GeoPos(inner, key, members...)
	})
	if err != nil {
		return nil, err
	}
	return result.([]*contract.GeoPosition), nil
}

func (s geoService) Search(ctx context.Context, key string, query GeoSearchQuery) ([]GeoSearchResult, error) {
	if err := validateKey(key); err != nil {
		return nil, err
	}
	result, err := s.client.execute(ctx, Operation{Name: "GEOSEARCH", Kind: "geo", Keys: []string{key}}, func(inner context.Context) (any, error) {
		return s.client.backend.GeoSearch(inner, key, query)
	})
	if err != nil {
		return nil, err
	}
	return result.([]contract.GeoSearchResult), nil
}
