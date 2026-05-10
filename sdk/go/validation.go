package resilix

import (
	"strings"
	"time"
)

func validateKey(key string) error {
	if strings.TrimSpace(key) == "" {
		return ValidationError{Field: "key", Message: "key cannot be empty"}
	}
	return nil
}

func validateKeys(keys []string) error {
	if len(keys) == 0 {
		return ValidationError{Field: "keys", Message: "at least one key is required"}
	}
	for _, key := range keys {
		if err := validateKey(key); err != nil {
			return err
		}
	}
	return nil
}

func validateStringSlice(name string, values []string) error {
	if len(values) == 0 {
		return ValidationError{Field: name, Message: "at least one value is required"}
	}
	for _, value := range values {
		if strings.TrimSpace(value) == "" {
			return ValidationError{Field: name, Message: "values cannot contain blanks"}
		}
	}
	return nil
}

func validateMap(name string, values map[string]any) error {
	if len(values) == 0 {
		return ValidationError{Field: name, Message: "map cannot be empty"}
	}
	return nil
}

func toSeconds(ttl time.Duration) float64 {
	return ttl.Seconds()
}
