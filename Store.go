package events

import _ "sync"

type Store interface {
	ReadAll(last []byte, limit int) *GlobalSlice
	ReadAllFromAggregate(aggregId string) []AggregateEvent
	Clear()
	ReportMetrics()
	Append(aggregId string, expectedVersion int, records []Envelope) (err error)
}

const (
	ExpectedVersionAny  int = -2
	ExpectedVersionNone int = -1
)

type AggregateEvent struct {
	Contract string
	Data     []byte
	Index    int
}

func (ae AggregateEvent) Payload() (string, []byte) {
	return ae.Contract, ae.Data
}

// TODO : track global index here
type GlobalRecord struct {
	Contract string
	Data     []byte
}

func (gr GlobalRecord) Payload() (string, []byte) {
	return gr.Contract, gr.Data
}

type GlobalSlice struct {
	Items []GlobalRecord
	Last  []byte
}
