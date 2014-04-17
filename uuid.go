package eventstore

import (
	"encoding/binary"
	"sync/atomic"
	"time"

	"code.google.com/p/go-uuid/uuid"
)

var (
	seq  uint32
	node = uuid.NodeID()
)

func newSequentialUUID() []byte {
	uuid := make([]byte, 16)

	nano := time.Now().UnixNano()
	binary.BigEndian.PutUint64(uuid[0:], uint64(nano))

	copy(uuid[10:], node)
	atomic.AddUint32(&seq, 1)

	binary.BigEndian.PutUint32(uuid[8:], seq)

	return uuid
}
