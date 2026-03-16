package legacy

import "strconv"

func boolToString(v bool) string {
	if v {
		return "true"
	}
	return "false"
}

func intToString(v int) string {
	return strconv.Itoa(v)
}
