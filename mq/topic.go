package mq

import (
	"github.com/bomjdev/yetanother/utils"
	"slices"
	"strings"
)

type Topic string

func NewTopic(topics ...string) Topic {
	return Topic(joinDot(topics...))
}

func (t Topic) String() string {
	return string(t)
}

func (t Topic) With(topics ...string) Topic {
	return Topic(joinDot(append([]string{t.String()}, topics...)...))
}

func (t Topic) Match(s string) bool {
	return matchTopic(strings.Split(string(t), "."), strings.Split(s, "."))
}

var joinDot = utils.JoinFactory(".")

func matchTopic(topics, values []string) bool {
	if len(topics) == 0 {
		if len(values) == 0 {
			return true
		}
		return false
	}
	switch topics[0] {
	case "*":
		return matchTopic(topics[1:], values[1:])
	case "#":
		if len(topics) == 1 {
			return true
		}
		idx := slices.Index(values, topics[1])
		if idx < 0 {
			return false
		}
		return matchTopic(topics[1:], values[idx:])
	default:
		if topics[0] != values[0] {
			return false
		}
		return matchTopic(topics[1:], values[1:])
	}
}
