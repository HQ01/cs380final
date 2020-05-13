package kvraft

import (
	"../labgob"
	"../labrpc"
	"../raft"
	"sync"
	"sync/atomic"
	"time"
)
//  "go/build" imported and not used

// For op
const (
	OpGet = iota
	OpAppend
	OpPut
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Operation int //  Get, Put, Append
	Key string
	Value string
	ClientRequestID int64
}

type Log struct {
	cmd Op
	term int
}


type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	data map[string]string // variable for database storage
	//committedOp map[int]Op
	committedLogs map[int]Log
	//lastCommitIdx int
	reqCommitIdx map[int64]int
	callbackCh map[int64]chan Log


}

// background routine that pulling commited OP from raft
func (kv *KVServer) applyChHandler() {
	var msg raft.ApplyMsg
	for !kv.killed() {
		msg = <- kv.applyCh // must be FIFO
		if msg.CommandValid {
			// append to
			kv.mu.Lock()
			cmd, ok := msg.Command.(Op)
			DPrintf3("%d: applyChHandler processing %d", kv.me, cmd.ClientRequestID)
			if ok {
				//DPrintf3("%d: Appending & executing %d, (%s, %s), commit #: %d", kv.me, cmd.Operation, cmd.Key, cmd.Value, msg.CommandIndex)
				// Check whether the request is exectued
				commitIdx, ok := kv.reqCommitIdx[cmd.ClientRequestID]
				if ok {
					//DPrintf3("%d: Skip duplicate request %l. ", kv.me, cmd.ClientRequestID)
					// Save the log but don't execute it
					//DPrintf3("Duplicate request %d", cmd.ClientRequestID)
					kv.committedLogs[msg.CommandIndex] = kv.committedLogs[commitIdx]
				} else {
					// Execute op
					switch cmd.Operation {
					case OpGet:
						cmd.Value = kv.data[cmd.Key]
					case OpAppend:
						_, ok := kv.data[cmd.Key]
						if !ok {
							kv.data[cmd.Key] = cmd.Value
						} else {
							kv.data[cmd.Key] += cmd.Value
						}
						//DPrintf3("%d: Appending (%s, %s) into kvdata %s", kv.me, cmd.Key, cmd.Value, kv.data[cmd.Key])
					case OpPut:
						kv.data[cmd.Key] = cmd.Value
						//DPrintf3("%d: Putting (%s, %s) into kvdata %s", kv.me, cmd.Key, cmd.Value, kv.data[cmd.Key])
					}
					kv.committedLogs[msg.CommandIndex] = Log{cmd: cmd, term: msg.RaftTerm}
					kv.reqCommitIdx[cmd.ClientRequestID] = msg.CommandIndex
				}

			} else {
				DPrintf3("%d: Type assertion in applyChHandler failed!", kv.me)
			}
			kv.mu.Unlock()
			if ch, ok := kv.callbackCh[cmd.ClientRequestID]; ok {
				ch <- kv.committedLogs[msg.CommandIndex]
				//close(ch)
				kv.mu.Lock()
				delete(kv.callbackCh, cmd.ClientRequestID)
				kv.mu.Unlock()
			}
		} else {
			DPrintf3("%d: msg invalid!", kv.me)
		}
	}
}



func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	DPrintf3("%d get %s, #: %d", kv.me, args.Key, args.RequestID)
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = "NotLeader"
		return
	}
	newOp := Op{Operation: OpGet, Key: args.Key, Value: "", ClientRequestID: args.RequestID}
	kv.mu.Lock()
	if _, ok := kv.callbackCh[args.RequestID]; !ok {
		kv.callbackCh[args.RequestID] = make(chan Log)
	}
	commitIdx, ok := kv.reqCommitIdx[args.RequestID]
	kv.mu.Unlock()
	if ok {
		reply.Value = kv.committedLogs[commitIdx].cmd.Value
		return
	}
	_, curTerm, _ := kv.rf.Start(newOp)
	select {
	case <- time.After(time.Second * 1):
		 reply.Err = "Timeout"
		 kv.mu.Lock()
		 //close(kv.callbackCh[args.RequestID])
		 delete(kv.callbackCh, args.RequestID)
		 kv.mu.Unlock()
		 DPrintf3("%d get timeout", kv.me)
		 return
	case log := <- kv.callbackCh[args.RequestID]:
		 if log.term != curTerm {
			 reply.Err = "NotLeader"
			 return
		 }
		 reply.Err = ""
		 reply.Value = log.cmd.Value
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	DPrintf3("%d append (%s, %s) #: %d", kv.me, args.Key, args.Value, args.RequestID)
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = "NotLeader"
		return
	}
	if _, ok := kv.reqCommitIdx[args.RequestID]; ok {
		reply.Err = "ExpiredRequest"
		return
	}

	newOp := Op{
		Key: args.Key,
		Value: args.Value,
		ClientRequestID: args.RequestID,
	}
	if args.Op == "Put" {
		newOp.Operation = OpPut
	} else {
		newOp.Operation = OpAppend
	}

	kv.mu.Lock()
	if _, ok := kv.callbackCh[args.RequestID]; !ok {
		kv.callbackCh[args.RequestID] = make(chan Log)
	}
	kv.mu.Unlock()
	_, curTerm, _ := kv.rf.Start(newOp)
	select {
	case <- time.After(time.Second * 1):
		 reply.Err = "Timeout"
		 DPrintf3("%d append timeout", kv.me)
		 kv.mu.Lock()
		 //close(kv.callbackCh[args.RequestID])
		 delete(kv.callbackCh, args.RequestID)
		 kv.mu.Unlock()
		 return
	case log := <- kv.callbackCh[args.RequestID]:
		 if log.term != curTerm {
			 reply.Err = "NotLeader"
			 return
		 }
		 reply.Err = ""
	}
	DPrintf3("%d: Finish %s (%s, %s), %s", kv.me, args.Op, newOp.Key, newOp.Value, reply.Err)
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate
	//kv.data = make(map[string]string)

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.data = make(map[string]string)
	kv.committedLogs = make(map[int]Log)
	kv.reqCommitIdx = make(map[int64]int)
	kv.callbackCh = make(map[int64]chan Log)
	//kv.lastCommitIdx = -1
	go kv.applyChHandler()

	return kv
}

