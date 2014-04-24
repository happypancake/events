package events

import (
	"encoding/binary"
	"sync/atomic"
	"time"

	"code.google.com/p/go-uuid/uuid"
)

var (
	seq  uint32
	node = getNodeUint32()
)

func getNodeUint32() uint32 {
	n := uuid.NodeID()
	return binary.BigEndian.Uint32(n)
}

type UUID []byte

// 8 bytes of UNIXNANO
// 4 bytes of counter
// 4 bytes of hardware address
//type UUID []byte

func NewSequentialUUID() UUID {
	uuid := make([]byte, 16)

	nano := time.Now().UnixNano()
	incr := atomic.AddUint32(&seq, 1)

	binary.BigEndian.PutUint64(uuid[0:], uint64(nano))
	binary.BigEndian.PutUint32(uuid[8:], incr)
	binary.BigEndian.PutUint32(uuid[12:], node)

	return uuid
}

func (u UUID) Bytes() []byte {
	return []byte(u)
}

func (u UUID) Time() time.Time {
	nsec := binary.BigEndian.Uint64([]byte(u))
	return time.Unix(0, int64(nsec))
}
func (u UUID) Node() uint32 {
	return binary.BigEndian.Uint32([]byte(u)[12:])
}
func (u UUID) Sequence() uint32 {
	return binary.BigEndian.Uint32([]byte(u)[8:])
}

func (u UUID) After(another UUID) bool {
	if u.Node() != another.Node() {
		panic("Can't match UUIDs from different nodes")
	}
	t1 := u.Time()
	t2 := another.Time()
	if t1 == t2 {
		const halfway uint32 = 0xFFFFFFFF / 2
		// clocks match, let us compare sequences with wrap
		s1 := u.Sequence()
		s2 := u.Sequence()

		return s1-s2 < halfway

	} else {
		return t1.After(t2)
	}
}
