package query

import "time"

// Constant represents a typed value in the database
type Constant struct {
	value any
}

// NewConstant creates a new Constant with type checking
func NewConstant[T int | int64 | int16 | string | bool | time.Time](val T) Constant {
	return Constant{value: val}
}

func (c Constant) AsInt() (int, bool) {
	v, ok := c.value.(int)
	return v, ok
}

func (c Constant) AsLong() (int64, bool) {
	v, ok := c.value.(int64)
	return v, ok
}

func (c Constant) AsShort() (int16, bool) {
	v, ok := c.value.(int16)
	return v, ok
}

func (c Constant) AsString() (string, bool) {
	v, ok := c.value.(string)
	return v, ok
}

func (c Constant) AsBool() (bool, bool) {
	v, ok := c.value.(bool)
	return v, ok
}

func (c Constant) AsDate() (time.Time, bool) {
	v, ok := c.value.(time.Time)
	return v, ok
}
