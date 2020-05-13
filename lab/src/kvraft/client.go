package kvraft

import "../labrpc"
import "crypto/rand"
import "math/big"
import "time"


type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	id int64
	knownLeader int //record last used leader
	requestId int // unique request id
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.id = nrand()
	DPrintf("Make clerk %d", ck.id)
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	
	reply := new(GetReply)
	DPrintf("Clerk %d starts GET key: %s", ck.id, key)
	idx := ck.knownLeader
	for reply.Err != OK {
		args := GetArgs{Key: key, ClerkId: ck.id, RequestId: ck.requestId}
		ok := ck.servers[idx].Call("KVServer.Get", &args, &reply)
		ck.requestId++
		if !ok || reply.Err == ErrWrongLeader {
			newLeader := (idx + 1) % len(ck.servers)
			DPrintf("Clerk %d finds bad leader %d; change to %d", ck.id, idx, newLeader)
			idx = newLeader
		} else if reply.Err == ErrNoKey {
			DPrintf("Clerk %d gets key error", ck.id)
			return ""
		}
	}
	DPrintf("Clerk %d gets reply: %v", ck.id, reply)
	ck.knownLeader = idx
	return reply.Value
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	reply := new(PutAppendReply)
	DPrintf("Clerk %d starts %s key: %s value: %s", ck.id, op, key, value)
	idx := ck.knownLeader
	for reply.Err != OK {
		args := PutAppendArgs{Key: key, Value: value, Op: op, ClerkId: ck.id, RequestId: ck.requestId}
		ok := ck.servers[idx].Call("KVServer.PutAppend", &args, &reply)
		ck.requestId++
		if !ok || reply.Err == ErrWrongLeader {
			newLeader := (idx + 1) % len(ck.servers)
			DPrintf("Clerk %d bad leader %d; change to %d", ck.id, idx, newLeader)
			idx = newLeader
			time.Sleep(10 * time.Millisecond)
		}
	}
	ck.knownLeader = idx
	DPrintf("Clerk %d get reply", ck.id)
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
