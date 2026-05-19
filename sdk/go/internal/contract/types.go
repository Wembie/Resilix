package contract

import (
	"errors"
	"time"
)

var ErrKeyNotFound = errors.New("resilix: key not found")

type PoolStats struct {
	Hits       uint32
	Misses     uint32
	Timeouts   uint32
	TotalConns uint32
	IdleConns  uint32
	StaleConns uint32
}

type ZMember struct {
	Score  float64
	Member string
}

type Message struct {
	Channel string
	Pattern string
	Payload string
}

type StreamEntry struct {
	ID     string
	Values map[string]any
}

type StreamReadRequest struct {
	Group    string
	Consumer string
	Streams  []string
	IDs      []string
	Count    int64
	Block    time.Duration
	NoAck    bool
}

type StreamReadResponse struct {
	Stream  string
	Entries []StreamEntry
}

type Subscription interface {
	Messages() <-chan Message
	Close() error
}

type CommandResult struct {
	Name  string
	Key   string
	Value any
	Err   error
}

type PipelineCommand struct {
	Name         string
	Key          string
	Keys         []string
	Value        any
	Values       map[string]any
	TTL          time.Duration
	Members      []string
	ZMembers     []ZMember
	StreamValues map[string]any
	MaxLen       int64
	Approximate  bool
}

type GeoMember struct {
	Longitude float64
	Latitude  float64
	Name      string
}

type GeoPosition struct {
	Longitude float64
	Latitude  float64
}

type GeoSearchQuery struct {
	FromMember string
	FromCoord  *GeoPosition
	ByRadius   float64
	ByBox      *GeoBox
	Unit       string
	Sort       string
	Count      int64
	Any        bool
	WithCoord  bool
	WithDist   bool
}

type GeoBox struct {
	Width  float64
	Height float64
}

type GeoSearchResult struct {
	Name     string
	Distance float64
	GeoHash  int64
	Coord    *GeoPosition
}
