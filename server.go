package commit

import (
	"sync"
)

type StoreItem struct {
	value interface{}
	lock  sync.RWMutex
}

type TxnItem struct {
	txnState   TransactionState
	gets       []string
	sets       map[string]interface{}
	readValues map[string]interface{} // only set at commit time
}

type Server struct {
	mu    sync.Mutex
	store map[string]*StoreItem
	txns  map[int]TxnItem
}

// This function:
// 1. Attempts to obtain locks for the given transaction
// 2. If this succeeds, vote Yes
// 3. If this fails, release any obtained locks and vote No
func (sv *Server) Prepare(args *RPCArgs, reply *PrepareReply) {
	tid := args.Tid
	sv.mu.Lock()
	defer sv.mu.Unlock()

	if _, ok := sv.txns[tid]; !ok {
		// We don't have this transaction ID at all
		reply.IsRelevant = false
		reply.Vote = true
		return
	}
	reply.IsRelevant = true
	txn := sv.txns[tid]

	switch txn.txnState {
	case stateVotedYes, statePreCommitted, stateCommitted:
		reply.Vote = true
		return
	case stateVotedNo, stateAborted:
		reply.Vote = false
		return
	}

	// Locks we have accumulated so far; will release in one go if any lock fails
	writeLocked := make([]*sync.RWMutex, 0, len(txn.sets))
	readLocked := make([]*sync.RWMutex, 0, len(txn.gets))
	lockAcquisitionFailed := false

	// Iterate over all of the keys we want to write to
	for key := range txn.sets {
		// first check if that item even exists
		if _, ok := sv.store[key]; !ok {
			// the item doesn't exist! create it
			sv.store[key] = &StoreItem{
				value: nil,
				lock:  sync.RWMutex{},
			}
		}
		if sv.store[key].lock.TryLock() {
			// Write lock acquired
			writeLocked = append(writeLocked, &sv.store[key].lock)
		} else {
			lockAcquisitionFailed = true
			break
		}
	}

	if !lockAcquisitionFailed {
		for _, key := range txn.gets {
			if _, ok := sv.store[key]; !ok {
				sv.store[key] = &StoreItem{
					value: nil,
					lock:  sync.RWMutex{},
				}
			}
			if sv.store[key].lock.TryRLock() {
				// Read lock acquired
				readLocked = append(readLocked, &sv.store[key].lock)
			} else {
				lockAcquisitionFailed = true
				break
			}
		}
	}

	if lockAcquisitionFailed {
		// Release all locks, vote no
		for _, lock := range writeLocked {
			lock.Unlock()
		}
		for _, lock := range readLocked {
			lock.RUnlock()
		}
		txn.txnState = stateVotedNo
		// Write back
		sv.txns[tid] = txn
		reply.Vote = false
		return
	}

	txn.txnState = stateVotedYes
	sv.txns[tid] = txn
	reply.Vote = true
}

// This function aborts the given transaction and releases locks
func (sv *Server) Abort(args *RPCArgs, reply *struct{}) {
	// Your code here
	tid := args.Tid
	sv.mu.Lock()
	defer sv.mu.Unlock()

	if _, ok := sv.txns[tid]; !ok {
		// I am irrelevant, nothing needs to be done
		return
	}
	txn := sv.txns[tid]

	switch txn.txnState {
	case stateAborted:
		// Also nothing needs to be done
		return
	case stateVotedYes, statePreCommitted:
		// Only in these two cases have we acquired
		// any locks
		for key := range txn.sets {
			sv.store[key].lock.Unlock()
		}
		for _, key := range txn.gets {
			sv.store[key].lock.RUnlock()
		}
	}

	// Mark as aborted so as not to abort again
	txn.txnState = stateAborted
	sv.txns[tid] = txn
}

// This function replies with information about all known transactions
func (sv *Server) Query(args struct{}, reply *QueryReply) {
	txnSummary := make(map[int]TransactionState)
	sv.mu.Lock()
	defer sv.mu.Unlock()

	for tid, txn := range sv.txns {
		txnSummary[tid] = txn.txnState
	}
	reply.TxnSummary = txnSummary
}

// This function confirms that the server is ready to commit
func (sv *Server) PreCommit(args *RPCArgs, reply *struct{}) {
	tid := args.Tid
	sv.mu.Lock()
	defer sv.mu.Unlock()

	if _, ok := sv.txns[tid]; !ok {
		// I am irrelevant
		return
	}

	txn := sv.txns[tid]
	txn.txnState = statePreCommitted
	sv.txns[tid] = txn
}

// This function applies the logged operations and then releases locks
func (sv *Server) Commit(args *RPCArgs, reply *CommitReply) {
	// Your code here
	tid := args.Tid
	reply.ReadValues = make(map[string]interface{})
	sv.mu.Lock()
	defer sv.mu.Unlock()

	if _, ok := sv.txns[tid]; !ok {
		// I am irrelevant
		return
	}

	txn := sv.txns[tid]
	switch txn.txnState {
	case stateVotedNo, stateAborted:
		// (This shouldn't ever happen. Why is Commit even
		// called here?)
		return
	case stateCommitted:
		// We have previously committed, just reply
		// with cached result
		reply.ReadValues = txn.readValues
		return
	}

	// perform the read/write
	for key, value := range txn.sets {
		sv.store[key].value = value
	}
	for _, key := range txn.gets {
		reply.ReadValues[key] = sv.store[key].value
	}
	// cache readValues
	txn.readValues = reply.ReadValues
	// Mark committed
	txn.txnState = stateCommitted
	// write back
	sv.txns[tid] = txn

	// recycle the locks
	for key := range txn.sets {
		sv.store[key].lock.Unlock()
	}
	for _, key := range txn.gets {
		sv.store[key].lock.RUnlock()
	}
}

// This function logs a Get operation
func (sv *Server) Get(tid int, key string) {
	// Your code here
	sv.mu.Lock()
	defer sv.mu.Unlock()

	// Create a new transaction item if doesn't exist
	if _, ok := sv.txns[tid]; !ok {
		sv.txns[tid] = TxnItem{
			txnState: stateOperations,
			gets:     []string{},
			sets:     map[string]interface{}{},
		}
	}

	txn := sv.txns[tid]
	logf(false, "txn=%d: GET on key=%s", tid, key)
	txn.gets = append(txn.gets, key)

	// Write back entry
	sv.txns[tid] = txn
}

// This function logs a Set operation
func (sv *Server) Set(tid int, key string, value interface{}) {
	// Your code here
	sv.mu.Lock()
	defer sv.mu.Unlock()

	if _, ok := sv.txns[tid]; !ok {
		sv.txns[tid] = TxnItem{
			txnState: stateOperations,
			gets:     []string{},
			sets:     map[string]interface{}{},
		}
	}

	txn := sv.txns[tid]
	logf(false, "txn=%d: SET on key=%s with value=%+v", tid, key, value)
	txn.sets[key] = value

	sv.txns[tid] = txn
}

// Initialize new Server
// keys is a slice of the keys that this server is responsible for storing
func MakeServer(keys []string) *Server {
	sv := &Server{
		mu:    sync.Mutex{},
		store: map[string]*StoreItem{},
		txns:  map[int]TxnItem{},
	}
	return sv
}
