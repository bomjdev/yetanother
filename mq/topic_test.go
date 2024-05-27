package mq

import (
	"strings"
	"testing"
)

func TestMatchTopic(t *testing.T) {
	type testCase struct {
		topic, value string
	}

	for _, tc := range []testCase{
		{"test", "test"},
		{"*", "test"},
		{"#", "test"},
		{"#", "test.asdasdsad"},
		{"#", "test.asdasdsad.asdasdasdsa"},
		{"test.2", "test.2"},
		{"test.*", "test.2"},
		{"test.#", "test.2"},
		{"test.#", "test.2.123"},
		{"test.#", "test.2.123.asdasds"},
		{"test.#", "test"},
		{"test.#.xxx", "test.12312.123123.asdsdasd.xxx"},
		{"test.#.xxx", "test.xxx"},
	} {
		if match := matchTopic(strings.Split(tc.topic, "."), strings.Split(tc.value, ".")); !match {
			t.Errorf("topic=%q, value=%q", tc.topic, tc.value)
		}
	}
}
