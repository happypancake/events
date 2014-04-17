package main

import (
	"bytes"
	"fmt"
	"sync"
	"time"

	"github.com/happypancake/hpc/events"
)

func benchmarkAppends(records, goroutines, size int) {

	log.Info("Benchmark appends with %v goroutines and %v records (each)", goroutines, records)

	es := events.NewFdbStore(db, "bench")
	es.ReportMetrics()
	defer es.Clear()

	var wg sync.WaitGroup
	wg.Add(goroutines)

	data := bytes.Repeat([]byte("Z"), size)
	pack := []events.Envelope{events.New("test", data)}

	started := time.Now()

	for t := 0; t < goroutines; t++ {
		aggName := fmt.Sprintf("agg-%v", t)

		go func() {
			defer wg.Done()
			for i := 0; i < records; i++ {
				es.Append(
					aggName,
					events.ExpectedVersionAny,
					pack,
				)

			}
		}()
	}
	wg.Wait()

	speed := (float64(size*records*goroutines) / time.Now().Sub(started).Seconds()) / (1024 * 1024)

	log.Info("Writing %v records in %v threads in %v at speed of %.1f MB/s",
		records,
		goroutines,
		time.Now().Sub(started),
		speed)
}

func benchmarkReadWrite(records, byteSize, pageSize int) {
	es := events.NewFdbStore(db, "bench")
	es.ReportMetrics()
	defer es.Clear()

	data := bytes.Repeat([]byte("Z"), byteSize)

	page := make([]events.Envelope, pageSize)
	for i := 0; i < pageSize; i++ {
		page[i] = events.New("Test", data)
	}

	for i := 0; i < (records / pageSize); i++ {
		es.Append("test", events.ExpectedVersionAny, page)
	}

	var (
		start   time.Time
		elapsed time.Duration
	)
	start = time.Now()
	agg := es.ReadAllFromAggregate("test")
	elapsed = time.Now().Sub(start)

	speed := (float64(byteSize*records) / elapsed.Seconds()) / (1024 * 1024)
	log.Info("Aggregate : Read %v records in %v at %.1f MB/s", len(agg), elapsed, speed)

	start = time.Now()

	var token []byte

	var readFromGlobalRecords = 0
	var readFromGlobalBytes = 0

	for {
		read := es.ReadAll(token, pageSize)

		if len(read.Items) == 0 {
			break
		}

		readFromGlobalRecords += len(read.Items)
		for _, b := range read.Items {
			readFromGlobalBytes += len(b.Data)
		}

		token = read.Last
	}

	elapsed = time.Now().Sub(start)

	speed = (float64(readFromGlobalBytes) / elapsed.Seconds()) / (1024 * 1024)
	log.Info("Aggregate : Read %v records in %v at %.1f MB/s", readFromGlobalRecords, elapsed, speed)
}
