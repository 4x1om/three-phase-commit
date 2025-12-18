package commit

import (
	"labrpc"
	"sync"
	"sync/atomic"
	"time"
)

// Responses to the client
type ResponseMsg struct {
	tid        int
	committed  bool
	readValues map[string]interface{}
}

type Coordinator struct {
	servers   []*labrpc.ClientEnd
	respChan  chan ResponseMsg
	dead      int32
	timeout   time.Duration
	recovered chan struct{}
}

// Start the 3PC protocol for a particular transaction
// This may be called concurrently
func (co *Coordinator) FinishTransaction(tid int) {
	// Block until recovery finishes
	<-co.recovered

	logf(true, "beginning prepare for tid=%d", tid)
	numServers := len(co.servers)
	isServerIrrelevant := make([]bool, numServers)

	// abortChan is closed once if anybody aborts;
	// voteChan gets fed one empty object per "yes" vote
	abortChan := make(chan struct{})
	voteChan := make(chan struct{})
	var abortOnce sync.Once

	args := RPCArgs{Tid: tid}
	for i := 0; i < numServers; i++ {
		go func(index int) {
			reply := PrepareReply{}
			co.sendPrepare(index, &args, &reply)
			if !reply.IsRelevant {
				isServerIrrelevant[index] = true
			}
			if reply.Vote {
				voteChan <- struct{}{}
			} else {
				abortOnce.Do(func() {
					close(abortChan)
				})
			}
		}(i)
	}
	timeout := time.After(co.timeout)

	yesVotes := 0
	for {
		select {
		case <-timeout:
			abortOnce.Do(func() {
				close(abortChan)
			})
			continue
		case <-abortChan:
			co.doAbort(tid, nil)
			return
		case <-voteChan:
			yesVotes++
			if yesVotes == numServers {
				// proceed to precommit
				co.doPrecommit(tid, isServerIrrelevant)
				return
			}
		}
	}
}

func (co *Coordinator) doPrecommit(tid int, isServerIrrelevant []bool) {
	// Block until recovery finishes
	<-co.recovered

	logf(true, "beginning precommit for tid=%d", tid)
	numServers := len(co.servers)
	if isServerIrrelevant == nil {
		isServerIrrelevant = make([]bool, numServers)
	}

	abortChan := make(chan struct{})
	voteChan := make(chan struct{})
	var abortOnce sync.Once

	votes := 0
	args := RPCArgs{Tid: tid}
	for index := range co.servers {
		if isServerIrrelevant[index] {
			votes++
		} else {
			go func(index int) {
				precommitOk := co.sendPreCommit(index, &args)
				if precommitOk {
					voteChan <- struct{}{}
				} else {
					abortOnce.Do(func() {
						close(abortChan)
					})
				}
			}(index)
		}
	}
	timeout := time.After(co.timeout)

	for {
		select {
		case <-timeout:
			abortOnce.Do(func() {
				close(abortChan)
			})
			continue
		case <-abortChan:
			co.doAbort(tid, isServerIrrelevant)
			return
		case <-voteChan:
			votes++
			if votes == numServers {
				co.doCommit(tid, isServerIrrelevant)
				return
			}
		}
	}
}

func (co *Coordinator) doAbort(tid int, isServerIrrelevant []bool) {
	// Block until recovery finishes
	<-co.recovered

	logf(true, "abort tid=%d", tid)
	numServers := len(co.servers)
	if isServerIrrelevant == nil {
		isServerIrrelevant = make([]bool, numServers)
	}

	response := ResponseMsg{
		tid:        tid,
		committed:  false,
		readValues: map[string]interface{}{},
	}
	co.respChan <- response

	args := RPCArgs{Tid: tid}
	for i := 0; i < numServers; i++ {
		if !isServerIrrelevant[i] {
			go func(index int) {
				for {
					abortOk := co.sendAbort(index, &args)
					if abortOk {
						break
					}
					time.Sleep(co.timeout)
				}
			}(i)
		}
	}
}

func (co *Coordinator) doCommit(tid int, isServerIrrelevant []bool) {
	// Block until recovery finishes
	<-co.recovered

	logf(true, "beginning commit for tid=%d", tid)
	numServers := len(co.servers)
	if isServerIrrelevant == nil {
		isServerIrrelevant = make([]bool, numServers)
	}

	valuesChan := make(chan map[string]interface{})
	args := RPCArgs{Tid: tid}
	for i := 0; i < numServers; i++ {
		if isServerIrrelevant[i] {
			go func() {
				valuesChan <- make(map[string]interface{})
			}()
		} else {
			go func(index int) {
				var reply CommitReply
				for {
					commitOk := co.sendCommit(index, &args, &reply)
					if commitOk {
						break
					}
					time.Sleep(co.timeout)
				}
				valuesChan <- reply.ReadValues
			}(i)
		}
	}

	collatedValues := make(map[string]interface{})
	for i := 0; i < numServers; i++ {
		readValues := <-valuesChan
		for key, value := range readValues {
			collatedValues[key] = value
		}
	}

	logf(true, "commit succeeds for tid=%d", tid)
	response := ResponseMsg{
		tid:        tid,
		committed:  true,
		readValues: collatedValues,
	}
	co.respChan <- response
}

type TxnInfo struct {
	server     int
	TxnSummary map[int]TransactionState
}

func (co *Coordinator) doRecover() {
	logf(true, "begin recovery")
	numServers := len(co.servers)
	globalTxnInfo := make(map[int][]TransactionState)

	txnSummaryChan := make(chan TxnInfo)
	for i := 0; i < numServers; i++ {
		go func(index int) {
			reply := QueryReply{}
			for {
				queryOk := co.sendQuery(index, &reply)
				if queryOk {
					break
				}
				logf(true, "sendQuery fails for server=%d", index)
				time.Sleep(co.timeout)
			}
			txnInfo := TxnInfo{
				server:     index,
				TxnSummary: reply.TxnSummary,
			}
			txnSummaryChan <- txnInfo
		}(i)
	}

	for i := 0; i < numServers; i++ {
		txnInfo := <-txnSummaryChan
		index := txnInfo.server
		// logf(true, "server=%d has provided its txn record as follows", index)
		for tid, txnState := range txnInfo.TxnSummary {
			logf(true, "server=%d reports tid=%d, txnState=%d", index, tid, txnState)
			if _, ok := globalTxnInfo[tid]; !ok {
				// tid doesn't exist on globalTxnInfo
				globalTxnInfo[tid] = make([]TransactionState, numServers)
			}
			globalTxnInfo[tid][index] = txnState
		}
		// logf(true, "this concludes the txn record of server=%d", index)
	}

	for tid, txnStates := range globalTxnInfo {
		numberVotedNoOrAborted := 0
		numberCommitted := 0
		numberPrecommitted := 0
		numberVotedYes := 0

		for _, txnState := range txnStates {
			switch txnState {
			case stateVotedNo, stateAborted:
				numberVotedNoOrAborted++
			case stateCommitted:
				numberCommitted++
			case statePreCommitted:
				numberPrecommitted++
			case stateVotedYes:
				numberVotedYes++
			}
		}

		if numberVotedNoOrAborted > 0 {
			logf(true, "recovery: abort tid=%d", tid)
			go co.doAbort(tid, nil)
		} else if numberCommitted > 0 && numberCommitted < numServers {
			logf(true, "recovery: commit tid=%d", tid)
			go co.doCommit(tid, nil)
		} else if numberPrecommitted > 0 {
			logf(true, "recovery: precommit tid=%d", tid)
			go co.doPrecommit(tid, nil)
		} else if numberVotedYes > 0 {
			logf(true, "recovery: prepare tid=%d", tid)
			go co.FinishTransaction(tid)
		}
	}

	logf(true, "recovery concluded")
	close(co.recovered)
}

// This will be called at the beginning of a test to create a new Coordinator.
// It will also be called when the Coordinator restarts, so recovery is
// triggered here
func MakeCoordinator(servers []*labrpc.ClientEnd, respChan chan ResponseMsg) *Coordinator {
	co := &Coordinator{
		servers:  servers,
		respChan: respChan,
		// Initialize other fields here
		timeout:   1 * time.Second,
		recovered: make(chan struct{}),
	}

	go co.doRecover()

	return co
}

func (co *Coordinator) sendPrepare(server int, args *RPCArgs, reply *PrepareReply) bool {
	return co.servers[server].Call("Server.Prepare", args, reply)
}

func (co *Coordinator) sendAbort(server int, args *RPCArgs) bool {
	reply := struct{}{}
	return co.servers[server].Call("Server.Abort", args, &reply)
}

func (co *Coordinator) sendQuery(server int, reply *QueryReply) bool {
	return co.servers[server].Call("Server.Query", struct{}{}, reply)
}

func (co *Coordinator) sendPreCommit(server int, args *RPCArgs) bool {
	reply := struct{}{}
	return co.servers[server].Call("Server.PreCommit", args, &reply)
}

func (co *Coordinator) sendCommit(server int, args *RPCArgs, reply *CommitReply) bool {
	return co.servers[server].Call("Server.Commit", args, reply)
}

func (co *Coordinator) Kill() {
	atomic.StoreInt32(&co.dead, 1)
}

func (co *Coordinator) killed() bool {
	z := atomic.LoadInt32(&co.dead)
	return z == 1
}
