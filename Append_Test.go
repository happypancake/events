package eventstore

import (
	"github.com/FoundationDB/fdb-go/fdb"
	. "gopkg.in/check.v1"
)

// declare our unit test suite
type given_empty_event_store struct {
	store Store
}

func (s *given_empty_event_store) SetUpTest(c *C) {
	fdb.MustAPIVersion(200)
	db := fdb.MustOpenDefault()
	s.store = NewFdbStore(db, "es")
}
func (s *given_empty_event_store) TearDownTest(c *C) {
	s.store.Clear()
}

// add it to gocheck
var _ = Suite(&given_empty_event_store{})

func (s *given_empty_event_store) Test_when_we_append_one_record(c *C) {
	// when
	evt := []Envelope{New("Test", []byte("Hi"))}
	err := s.store.Append("test1", ExpectedVersionAny, evt)
	c.Assert(err, IsNil)

	recs := s.store.ReadAll(nil, 0).Items
	expectedGlobal := []GlobalRecord{
		GlobalRecord{
			Contract: "Test",
			Data:     []byte("Hi"),
		},
	}
	c.Check(recs, DeepEquals, expectedGlobal) // global stream should have this record

	expectedAggregate := []AggregateEvent{
		AggregateEvent{
			Contract: "Test",
			Data:     []byte("Hi"),
			Index:    0,
		},
	}

	c.Check(s.store.ReadAllFromAggregate("test1"), DeepEquals, expectedAggregate)
}

func (s *given_empty_event_store) Test_when_we_append_two_records_at_once(c *C) {
	r1 := New("Test", []byte("One"))
	r2 := New("Test", []byte("Two"))
	err := s.store.Append("test1", ExpectedVersionAny, []Envelope{r1, r2})
	c.Assert(err, IsNil)

	recs := s.store.ReadAll(nil, 0).Items
	expectedGlobal := []GlobalRecord{
		GlobalRecord{
			Contract: "Test",
			Data:     []byte("One"),
		},
		GlobalRecord{
			Contract: "Test",
			Data:     []byte("Two"),
		},
	}
	c.Check(recs, DeepEquals, expectedGlobal)

	expectedAggregate := []AggregateEvent{
		AggregateEvent{
			Contract: "Test",
			Data:     []byte("One"),
			Index:    0,
		},
		AggregateEvent{
			Contract: "Test",
			Data:     []byte("Two"),
			Index:    1,
		},
	}

	c.Check(s.store.ReadAllFromAggregate("test1"), DeepEquals, expectedAggregate)
}

func (s *given_empty_event_store) Test_when_we_append_expecting_some_version(c *C) {

	evt := New("Test", []byte("Hi"))
	err := s.store.Append("test1", 1, []Envelope{evt})

	c.Assert(err, DeepEquals, &ErrConcurrencyViolation{
		AggregateId:     "test1",
		ExpectedVersion: 1,
		ActualVersion:   -1,
	})
}

func (s *given_empty_event_store) Test_when_we_append_expecting_0_version(c *C) {
	evt := New("Test", []byte("Hi"))
	err := s.store.Append("test1", 0, []Envelope{evt})

	c.Assert(err, DeepEquals, &ErrConcurrencyViolation{
		AggregateId:     "test1",
		ExpectedVersion: 0,
		ActualVersion:   -1,
	})
}

func (s *given_empty_event_store) Test_when_we_append_expecting_no_aggregate(c *C) {
	evt := New("Test", []byte("Hi"))

	err := s.store.Append("test1", ExpectedVersionNone, []Envelope{evt})

	c.Assert(err, IsNil)
}
