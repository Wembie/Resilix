package resilix

import (
	_ "embed"
	"strings"
)

//go:embed VERSION
var embeddedVersion string

// Version exposes the SDK version taken from sdk/go/VERSION.
var Version = strings.TrimSpace(embeddedVersion)
