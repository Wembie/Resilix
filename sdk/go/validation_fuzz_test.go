package resilix

import "testing"

func FuzzValidateKey(f *testing.F) {
	f.Add("hello")
	f.Add("")
	f.Add("   ")

	f.Fuzz(func(t *testing.T, key string) {
		_ = validateKey(key)
	})
}
