package cli

import "strings"

const quackAPIBasePath = "/v1"

func generatedAPIPath(path string) string {
	trimmed := strings.TrimSpace(path)
	if trimmed == "" {
		return quackAPIBasePath
	}
	if !strings.HasPrefix(trimmed, "/") {
		trimmed = "/" + trimmed
	}
	if trimmed == quackAPIBasePath || strings.HasPrefix(trimmed, quackAPIBasePath+"/") {
		return trimmed
	}
	return quackAPIBasePath + trimmed
}
