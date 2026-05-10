package resilix

import "testing"

func TestVersionIsEmbedded(t *testing.T) {
	t.Parallel()

	if Version == "" {
		t.Fatal("expected embedded version to be set")
	}
}
