package types

import "time"

func Hash(value any) int {
	if value == nil {
		return 0
	}

	switch v := value.(type) {
	case int:
		return v
	case int64:
		return int(v)
	case int16:
		return int(v)
	case string:
		hash := 0
		for _, c := range v {
			hash += int(c)
		}
		return hash
	case bool:
		if v {
			return 1
		} else {
			return 0
		}
	case time.Time:
		return int(v.Unix())
	default:
		return 0
	}
}
