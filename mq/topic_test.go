package mq

import "testing"

func TestTopic(t *testing.T) {
	topic := Topic("asd")
	t.Log(topic)
	t.Log(topic.With("ad", "asdsa", "2312"))
}
