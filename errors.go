package eventstore

import "fmt"

type ErrConcurrencyViolation struct {
	AggregateId     string
	ExpectedVersion int
	ActualVersion   int
}

func (err *ErrConcurrencyViolation) Error() string {
	return fmt.Sprintf("Expected '%v' to be ver. '%v' but got '%v'", err.AggregateId, err.ExpectedVersion, err.ActualVersion)
}
