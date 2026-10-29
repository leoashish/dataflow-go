package ingest

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
)

func Coerce(raw string, t FieldType) (any, error) {
	trimmed := strings.TrimSpace(raw)
	switch t {
	case String:
		return raw, nil
	case Int:
		n, err := strconv.ParseInt(trimmed, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("not an integer: %q", raw)
		}
		return n, nil
	case Float:
		f, err := strconv.ParseFloat(trimmed, 64)
		if err != nil || math.IsNaN(f) || math.IsInf(f, 0) {
			return nil, fmt.Errorf("not a finite number: %q", raw)
		}
		return f, nil
	case Bool:
		switch strings.ToLower(trimmed) {
		case "true", "t", "yes", "y", "1":
			return true, nil
		case "false", "f", "no", "n", "0":
			return false, nil
		}
		return nil, fmt.Errorf("not a boolean: %q", raw)
	case Time:
		ts, err := time.Parse(timeLayout, trimmed)
		if err != nil {
			return nil, fmt.Errorf("not an RFC 3339 time: %q", raw)
		}
		return ts.UTC(), nil
	}
	return nil, fmt.Errorf("unknown field type %d", t)
}
