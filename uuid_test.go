package events

import (
	"bytes"
	"testing"
	"time"
)

func Test_UUIDs_are_unique(t *testing.T) {

	id1 := NewSequentialUUID()
	id2 := NewSequentialUUID()

	//panic(fmt.Sprintf("%v", id1))

	if 0 == bytes.Compare(id1, id2) {
		t.Error("Both IDS should be unique")
	}
	if id1.Node() != id2.Node() {
		t.Error("Node ids of both UUIDs should be the same")
	}
	if id1.Sequence() == id2.Sequence() {
		t.Error("Sequences should be different")
	}
}

func Test_UUID_TimeCanBeExtracted(t *testing.T) {
	now := time.Now()
	id1 := NewSequentialUUID()

	if now.After(id1.Time()) {
		t.Error("Time in UUID should be greater or equal to local time")
	}

	if now.Add(time.Second).Before(id1.Time()) {
		t.Error("Time in UUID should be close to local time")
	}
}
