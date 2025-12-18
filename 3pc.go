package commit

import (
	"fmt"
	"log"
	"os"
	"sync"
	"time"
)

// Logging utilities

var (
	raftStart   = time.Now()
	raftLogger  *log.Logger
	raftLogOnce sync.Once
)

func initRaftLogger() {
	raftLogOnce.Do(func() {
		f, err := os.OpenFile("3pc.log",
			os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
		if err != nil {
			log.Fatalf("failed to open 3pc.log: %v", err)
		}

		raftLogger = log.New(f, "", 0)
	})
}

func logf(isCoordinator bool, format string, args ...interface{}) {
	// Safe to call repeatedly
	initRaftLogger()

	elapsed := time.Since(raftStart).Milliseconds()
	var identity string
	if isCoordinator {
		identity = "master"
	} else {
		identity = "server"
	}
	prefix := fmt.Sprintf("[%4d ms] %s | ", elapsed, identity)

	raftLogger.Printf(prefix+format, args...)
}

// ------------------------------------------
//                  COMMON
// ------------------------------------------

type TransactionState int

const (
	stateOperations TransactionState = iota
	stateVotedNo
	stateVotedYes
	statePreCommitted
	stateAborted
	stateCommitted
)

type RPCArgs struct {
	Tid int
}

type PrepareReply struct {
	IsRelevant bool
	Vote       bool
}

type QueryReply struct {
	TxnSummary map[int]TransactionState
}

type CommitReply struct {
	ReadValues map[string]interface{}
}
