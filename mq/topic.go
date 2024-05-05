package mq

import "github.com/bomjdev/yetanother/utils"

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

var joinDot = utils.JoinFactory(".")
