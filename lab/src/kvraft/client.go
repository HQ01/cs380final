package kvraft

import (
	"../labrpc"
	"log"
	"sync"
	"time"
)
import "crypto/rand"
import "math/big"

const Debug3 = 0

func DPrintf3(format string, a ...interface{}) (n int, err error) {
	if Debug3 > 0 {
		log.Printf(format, a...)
	}
	return
}

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	leaderID int
	//requestID int
	mu sync.Mutex
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
	// TODO: Assume that raft are initialized. Get Leader
	ck.leaderID = 0
	return ck
}

func (ck *Clerk) updateLeader(curLeaderID int) {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	if curLeaderID == ck.leaderID {
		ck.leaderID++
		if ck.leaderID >= len(ck.servers) {
			ck.leaderID = 0
		}
	}
}


//
//// fetch the current value for a key.
//// returns "" if the key does not exist.
//// keeps trying forever in the face of all other errors.
////
//// you can send an RPC with code like this:
//// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
////
//// the types of args and reply (including whether they are pointers)
//// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	var reply GetReply
	retStr := ""
	requestID := nrand()
	args := GetArgs{Key: key, RequestID: requestID}
	DPrintf3("Clerk get(%s), reqID %d", key, requestID)
	ok := false
	for !(ok && reply.Err == "") {
		ck.mu.Lock()
		curLeaderID := ck.leaderID
		ck.mu.Unlock()
		reply.Err = ""
		//DPrintf3("Getting.......................................................")
		ok = ck.servers[curLeaderID].Call("KVServer.Get", &args, &reply)
		//DPrintf3("Getting.......................................................Done")
		if !ok {
			// Network failure?
			DPrintf3("Clerk Get() network error. %d", curLeaderID)
			ck.updateLeader(curLeaderID)
		} else {
			if reply.Err == "" {
				retStr = reply.Value
			} else {
				if reply.Err == "NotLeader" || reply.Err == "Killed" || reply.Err == "Timeout" {
					ck.updateLeader(curLeaderID)
					time.Sleep(time.Millisecond * 100)
				}
				if reply.Err == "ExpiredRequest" {
					requestID = nrand()
				}
				if reply.Err != "NotLeader" {
					DPrintf3("Clerk Get() failed: %s", reply.Err)
				}
			}
		}
	}
	return retStr
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	// TODO: random request ID might be the same as previous reqeusts
	requestID := nrand()
	args := PutAppendArgs{
		Key:   key,
		Value: value,
		Op:    op,
		RequestID: requestID,
	}
	DPrintf3("Clerk PutAppend(%s, %s), reqID %d", key, value, requestID)
	var reply PutAppendReply
	ok := false
	for !(ok && (reply.Err == "" || reply.Err == "ExpiredRequest")) {
		ck.mu.Lock()
		curLeaderID := ck.leaderID
		ck.mu.Unlock()
		reply.Err = ""
		//DPrintf3("Puttingdd.......................................................")
		ok = ck.servers[curLeaderID].Call("KVServer.PutAppend", &args, &reply)
		//DPrintf3("Putting.......................................................Done")
		if !ok {
			ck.updateLeader(curLeaderID)
			DPrintf3("Clerk PutAppend() network error. %d", curLeaderID)
		} else {
			//if reply.Err == "NotLeader" || reply.Err == "Killed" || reply.Err == "Timeout"{
			//}
			//if reply.Err != "" && reply.Err != "NotLeader" {
			 if reply.Err != ""{
				 ck.updateLeader(curLeaderID)
				DPrintf3("Clerk PutAppend error at server %d:" + string(reply.Err), curLeaderID)
				 time.Sleep(time.Millisecond * 100)
			}
		}
	}
}


func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
