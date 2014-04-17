package eventstore

import (
	"crypto/rand"
	"time"

	"github.com/FoundationDB/fdb-go/fdb"
	"github.com/FoundationDB/fdb-go/fdb/subspace"
	"github.com/FoundationDB/fdb-go/fdb/tuple"
	"github.com/happypancake/hpc/fsd"
)

type fdbStore struct {
	space         subspace.Subspace
	db            fdb.Database
	reportMetrics bool
}

func NewFdbStore(db fdb.Database, el ...tuple.TupleElement) Store {
	space := subspace.Sub(el...)
	return &fdbStore{
		space,
		db,
		false,
	}
}

const (
	globalPrefix = 0
	aggregPrefix = 1
)

var (
	Start = make([]tuple.TupleElement, 0)
)

func nextRandom() []byte {
	b := make([]byte, 20)
	if _, err := rand.Read(b); err == nil {
		return b
	} else {

		panic(err)
	}
}

// ReportMetrics enables FSD metrics reporting. It is disabled by default
// to avoid polluting unit tests
func (es *fdbStore) ReportMetrics() {
	es.reportMetrics = true
}

func (es *fdbStore) Clear() {
	es.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		tr.ClearRange(es.space)
		return nil, nil
	})
}

func (es *fdbStore) Append(aggregId string, expectedVersion int, records []Envelope) (err error) {

	if es.reportMetrics {
		defer fsd.TimeSince("es.append", time.Now())
	}

	globalSpace := es.space.Sub(globalPrefix)
	aggregSpace := es.space.Sub(aggregPrefix, aggregId)

	// TODO add random key to reduce contention

	_, err = es.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		// we are getting them in parallel
		aggregRecord := GetLastKeyFuture(tr, aggregSpace)

		//globalRecord := GetLastKeyFuture(tr.Snapshot(), globalSpace)

		nextAggregIndex := aggregRecord.MustGetNextIndex(0)

		switch expectedVersion {
		case ExpectedVersionAny:
			break
		case ExpectedVersionNone:
			if nextAggregIndex != 0 {
				return nil, &ErrConcurrencyViolation{
					aggregId,
					expectedVersion,
					nextAggregIndex - 1,
				}
			}
		default:
			if (nextAggregIndex - 1) != expectedVersion {
				return nil, &ErrConcurrencyViolation{
					aggregId,
					expectedVersion,
					nextAggregIndex - 1,
				}
			}
		}

		for i, evt := range records {
			aggregIndex := nextAggregIndex + i

			uuid := newSequentialUUID()

			contract, data := evt.Payload()
			tr.Set(globalSpace.Sub(uuid, contract, aggregId, aggregIndex), data)
			tr.Set(aggregSpace.Sub(aggregIndex, contract), data)
		}

		return nil, nil
	})

	if es.reportMetrics {
		if nil == err {
			fsd.Count("es.append.ok", 1)
		} else {
			fsd.Count("es.append.fail", 1)
		}
	}

	return
}

func (es *fdbStore) ReadAll(last []byte, limit int) *GlobalSlice {
	globalSpace := es.space.Sub(globalPrefix)
	start, end := globalSpace.FDBRangeKeys()

	r, err := es.db.ReadTransact(func(tr fdb.ReadTransaction) (interface{}, error) {

		var scan fdb.KeyRange
		if nil == last {
			scan = fdb.KeyRange{start, end}
		} else {
			next := tr.Snapshot().GetKey(fdb.FirstGreaterThan(fdb.Key(last))).MustGet()
			scan = fdb.KeyRange{next, end}
		}

		rr := tr.Snapshot().GetRange(scan, fdb.RangeOptions{Limit: limit})

		return rr.GetSliceOrPanic(), nil

	})

	if err != nil {
		panic("Failed to read all events")
	}

	kvs := r.([]fdb.KeyValue)

	result := make([]GlobalRecord, len(kvs))

	for i, kv := range kvs {

		if t, err := globalSpace.Unpack(kv.Key); err != nil {
			panic("Failed to unpack key")
		} else {
			result[i].Contract = t[1].(string)
			result[i].Data = kv.Value
			last = []byte(kv.Key)
		}
	}
	return &GlobalSlice{result, last}
}

func (es *fdbStore) ReadAllFromAggregate(aggregId string) []AggregateEvent {
	streamSpace := es.space.Sub(aggregPrefix, aggregId)
	r, err := es.db.ReadTransact(func(tr fdb.ReadTransaction) (interface{}, error) {

		rr := tr.Snapshot().GetRange(streamSpace, fdb.RangeOptions{0, fdb.StreamingModeWantAll, false})
		return rr.GetSliceOrPanic(), nil

	})

	if err != nil {
		panic("Failed to read all from aggregate")
	}

	kvs := r.([]fdb.KeyValue)

	result := make([]AggregateEvent, len(kvs))

	for i, kv := range kvs {

		if t, err := streamSpace.Unpack(kv.Key); err != nil {
			panic("Failed to unpack key")
		} else {
			result[i].Index = int(t[0].(int64))
			result[i].Contract = t[1].(string)
			result[i].Data = kv.Value
		}
	}
	return result
}
