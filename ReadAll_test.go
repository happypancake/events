package eventstore

import (
	"github.com/FoundationDB/fdb-go/fdb"
	. "gopkg.in/check.v1"
)

// declare our unit test suite
type given_event_store_with_2_records struct {
	store Store
}

func (s *given_event_store_with_2_records) SetUpTest(c *C) {
	fdb.MustAPIVersion(200)
	db := fdb.MustOpenDefault()
	s.store = NewFdbStore(db, "es")

	r1 := New("Test", []byte("One"))
	r2 := New("Test", []byte("Two"))
	s.store.Append("test", ExpectedVersionAny, []Envelope{r1, r2})
}
func (s *given_event_store_with_2_records) TearDownTest(c *C) {
	s.store.Clear()
}

func (s *given_event_store_with_2_records) Test_when_we_read_records_by_one(c *C) {
	slice1 := s.store.ReadAll(nil, 1)
	c.Check(len(slice1.Items), Equals, 1)
	c.Check(slice1.Last, NotNil)

	slice2 := s.store.ReadAll(slice1.Last, 1)

	c.Check(len(slice2.Items), Equals, 1)
	c.Check(slice2.Last, NotNil)

	slice3 := s.store.ReadAll(slice2.Last, 1)

	c.Check(len(slice3.Items), Equals, 0)
	c.Check(slice3.Last, DeepEquals, slice2.Last)
}

func (s *given_empty_event_store) Test_when_we_read_records_from_start(c *C) {
	slice := s.store.ReadAll(nil, 10)

	c.Check(len(slice.Items), Equals, 0)
	c.Check(slice.Last, IsNil)
}

// add it to gocheck
var _ = Suite(&given_event_store_with_2_records{})
