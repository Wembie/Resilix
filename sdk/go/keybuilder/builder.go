// Package keybuilder provides structured Redis key construction with namespace prefixes.
//
// Usage:
//
//	kb := keybuilder.New("myapp", "v1")
//	key := kb.Build("user", userID, "profile")   // → "myapp:v1:user:42:profile"
//	pat := kb.Pattern("session", "*")             // → "myapp:v1:session:*"
package keybuilder

import (
	"fmt"
	"strings"
)

const defaultSep = ":"

// Builder constructs namespaced Redis keys from segments separated by a delimiter.
type Builder struct {
	prefix []string
	sep    string
}

// New creates a Builder with the given namespace segments.
// Example: New("app", "v2") produces keys prefixed with "app:v2:".
func New(namespace ...string) *Builder {
	return &Builder{prefix: namespace, sep: defaultSep}
}

// WithSeparator returns a copy of the Builder using sep as the segment delimiter.
func (b *Builder) WithSeparator(sep string) *Builder {
	return &Builder{prefix: b.prefix, sep: sep}
}

// Build joins the builder's prefix with the provided segments.
// Any value type is accepted; non-string types are formatted with fmt.Sprintf("%v").
func (b *Builder) Build(segments ...any) string {
	parts := make([]string, 0, len(b.prefix)+len(segments))
	parts = append(parts, b.prefix...)
	for _, seg := range segments {
		parts = append(parts, segmentString(seg))
	}
	return strings.Join(parts, b.sep)
}

// Pattern builds a key glob pattern. Semantically equivalent to Build but
// marks intent for use with SCAN / KEYS.
func (b *Builder) Pattern(segments ...any) string {
	return b.Build(segments...)
}

// Prefix returns the builder's namespace prefix joined by the separator.
func (b *Builder) Prefix() string {
	return strings.Join(b.prefix, b.sep)
}

// Sub returns a child Builder that extends this builder's prefix with extra segments.
//
//	base := keybuilder.New("app")
//	users := base.Sub("users")
//	key := users.Build(42, "profile")  // → "app:users:42:profile"
func (b *Builder) Sub(segments ...any) *Builder {
	extra := make([]string, len(segments))
	for i, seg := range segments {
		extra[i] = segmentString(seg)
	}
	prefix := make([]string, 0, len(b.prefix)+len(extra))
	prefix = append(prefix, b.prefix...)
	prefix = append(prefix, extra...)
	return &Builder{prefix: prefix, sep: b.sep}
}

func segmentString(v any) string {
	switch s := v.(type) {
	case string:
		return s
	case []byte:
		return string(s)
	default:
		return fmt.Sprintf("%v", s)
	}
}
