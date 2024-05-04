package utils

import (
	"strings"
)

var JoinDash = JoinFactory("-")

func JoinFactory(sep string) func(args ...string) string {
	return func(args ...string) string {
		return strings.Join(args, sep)
	}
}
