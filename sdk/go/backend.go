package resilix

import "github.com/Wembie/Resilix/sdk/go/internal/contract"

// Backend is the port that all Redis drivers implement.
// Implement this interface to use a custom driver or a test double.
type Backend = contract.Backend
