package main

import (
	"flag"
	"os"
	"runtime"

	stdlog "log"
	"github.com/FoundationDB/fdb-go/fdb"
	"github.com/op/go-logging"
)

var (
	db  fdb.Database
	log *logging.Logger

	goroutines = flag.Int("g", 10, "Number of goroutines")
	records    = flag.Int("n", 100, "Number of records")
	byteSize   = flag.Int("b", 100, "Record byteSize in bytes")
	pageSize   = flag.Int("p", 5, "Page size")
)

func init() {
	consoleBackend := logging.NewLogBackend(os.Stderr, "", stdlog.LstdFlags|stdlog.Lshortfile)
	consoleBackend.Color = true
	logging.SetBackend(consoleBackend)
	log = logging.MustGetLogger("terminator")

	fdb.MustAPIVersion(200)
	db = fdb.MustOpenDefault()
}

func main() {

	flag.Parse()

	procs := runtime.GOMAXPROCS(runtime.NumCPU())

	log.Info("Set GOMAXPROCS to %v", procs)

	commands := flag.Args()
	if len(commands) == 0 {
		commands = []string{"append"}
	}

	switch commands[0] {
	case "append":
		benchmarkAppends(*records, *goroutines, *byteSize)
	case "read":
		benchmarkReadWrite(*records, *byteSize, *pageSize)
	}

}
