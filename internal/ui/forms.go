package ui

import (
	"strconv"
	"strings"
)

func formString(values map[string][]string, key string) string {
	if values == nil {
		return ""
	}
	return strings.TrimSpace(first(values[key]))
}

func formOptionalString(values map[string][]string, key string) *string {
	v := formString(values, key)
	if v == "" {
		return nil
	}
	return &v
}

func formBool(values map[string][]string, key string) bool {
	v := strings.ToLower(formString(values, key))
	return v == "true" || v == "1" || v == "on" || v == "yes"
}

func formOptionalInt(values map[string][]string, key string) (*int, error) {
	v := formString(values, key)
	if v == "" {
		return nil, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return nil, err
	}
	return &n, nil
}

func formOptionalInt64(values map[string][]string, key string) (*int64, error) {
	v := formString(values, key)
	if v == "" {
		return nil, nil
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return nil, err
	}
	return &n, nil
}

func formCSV(values map[string][]string, key string) []string {
	raw := formString(values, key)
	if raw == "" {
		return nil
	}
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		out = append(out, p)
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func first(values []string) string {
	if len(values) == 0 {
		return ""
	}
	return values[0]
}
