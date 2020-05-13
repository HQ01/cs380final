package kvraft

import (
	"../labgob"
	"../labrpc"
	"log"
	"../raft"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type MethodType int

const (
	GET MethodType = iota
	PUT
	APPEND
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Method	MethodType
	Key		string
	Value	string
	ClerkId	int64
	RequestId int
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	kvmap map[string]string // store kv pairs
	handler map[int]chan raft.ApplyMsg // AppyMsg channels for each server
	lastRequestId map[int64]int // record last called request id from each client
}

func (kv *KVServer) wait(index int, op Op) bool {
	kv.mu.Lock()
	channel := make(chan raft.ApplyMsg, 1)
	kv.handler[index] = channel
	kv.mu.Unlock()
	for {
		select {
		case msg := <- channel:
			kv.mu.Lock()
			delete(kv.handler, index)
			kv.mu.Unlock()
			if msg.CommandIndex == index && msg.Command == op {
				return true
			}
			return false
		case <- time.After(raft.VoteTimeOutUpper * time.Millisecond):
			kv.mu.Lock()
			DPrintf("Server %d time out", kv.me)
			delete(kv.handler, index)
			kv.mu.Unlock()
			return false
		}
	}
}


func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	op := Op{Method: GET, Key: args.Key, ClerkId: args.ClerkId, RequestId: args.RequestId}

	kv.mu.Lock()
	index, _, isLeader := kv.rf.Start(op)
	kv.mu.Unlock()

	if !isLeader {
		reply.Err = ErrWrongLeader
	} else {
		ok := kv.wait(index, op)
		if ok {
			kv.mu.Lock()
			val, prs := kv.kvmap[args.Key]
			if prs {
				reply.Err = OK
				reply.Value = val
			} else {
				reply.Err = ErrNoKey
			}
			kv.mu.Unlock()
		} else {
			reply.Err = ErrWrongLeader
		}
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	op := Op{Method: PUT, Key: args.Key, Value: args.Value, ClerkId: args.ClerkId, RequestId: args.RequestId}
	if args.Op == "Append" {
		op.Method = APPEND
	}

	kv.mu.Lock()
	index, _, isLeader := kv.rf.Start(op)
	kv.mu.Unlock()

	if !isLeader {
		reply.Err = ErrWrongLeader
	} else {
		ok := kv.wait(index, op)
		if ok {
			reply.Err = OK
		} else {
			reply.Err = ErrWrongLeader
		}
	}
}

func (kv *KVServer) listening() {
	for {
		select {
		case msg := <- kv.applyCh:
			kv.mu.Lock()
			DPrintf("Server %d get msg %v", kv.me, msg)
			op := msg.Command.(Op)
			if op.Method != GET { // need to check duplicate request first
				requestId, prs := kv.lastRequestId[op.ClerkId]
				if prs && requestId >= op.RequestId {
					DPrintf("Server %d found duplicated request id %d from Client %d", kv.me, requestId, op.ClerkId)
				} else {
					kv.lastRequestId[op.ClerkId] = op.RequestId
					if op.Method == PUT {
						kv.kvmap[op.Key] = op.Value
					} else {
						kv.kvmap[op.Key] += op.Value
					}
				}
			}
			channel, prs := kv.handler[msg.CommandIndex]
			if prs {
				channel <- msg
			}
			DPrintf("Server %d kvmap: %v", kv.me, kv.kvmap)
			kv.mu.Unlock()
		}
	}
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

	// You may need initialization code here.
	kv.kvmap = make(map[string]string)
	kv.handler = make(map[int]chan raft.ApplyMsg)
	kv.lastRequestId = make(map[int64]int)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	go kv.listening()
	DPrintf("Server %d started", me)
	return kv
}
