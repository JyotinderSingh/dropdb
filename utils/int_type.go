package utils

import "runtime"

// IntSize provides the size of int on this architecture.
var IntSize = 8

func init() {
	if runtime.GOARCH == "386" || runtime.GOARCH == "arm" {
		IntSize = 4
	}
}
