package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//
import (
	"sync"
	"sync/atomic"
	"../labrpc"
	"time"
	"math/rand"
	"sort"
	//"log"
)


import "bytes"
import "../labgob"
const(
	Leader = iota
	Follower
	Candidate
	StartLog
	DefaultOp
)

const(
	VoteTimeOutLower = 350 //350
	VoteTimeOutUpper = 500 //500
	VoteSleep		 = 50
	DefaultSleep 	 = 20
	HeartBeatSleep	 = 100
)
//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	RaftTerm int
	//LeaderIndex	 int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	chmu	  sync.Mutex		  // Lock to serialize channel
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// persistent state for all servers
	currentTerm		int
	votedFor		int	//init to -1 (nil)
	logs			[]Entry

	//volatile state on all servers
	commitIndex		int
	lastApplied		int

	//volatile state on leaders (empty if this server is not leader; reset every term?\TODO)
	nextIndex		[]int
	matchIndex		[]int


	//timer
	voteTimeOutBase		time.Time
	voteTimeOutDuration	time.Duration


	//status
	status		int	//leader: 0 follower: 1 candidate: 2
	quorum		int

	//applyCh communcation w/ tester
	applyChCond *sync.Cond


}


type Entry struct {
	Index	int
	Op		interface{}
	Key		int
	Value	int
	Term	int
}


// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = (rf.status == Leader)

	// Your code here (2A).
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}


//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []Entry
	if d.Decode(&currentTerm) != nil ||
        d.Decode(&votedFor) != nil ||
        d.Decode(&log) != nil {
        // error...
        panic("fail to decode state")
    } else {
        rf.currentTerm = currentTerm
        rf.votedFor = votedFor
        rf.logs = log
	}
	DPrintf("ATTENTION: %d revived, log length %d, current term %d, status %d, lastentry term %d", rf.me, len(rf.logs), rf.currentTerm, rf.status, rf.logs[len(rf.logs)-1].Term)
}




//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term			int
	CandidateId		int
	LastLogIndex	int
	LastLogTerm	int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term			int
	VoteGranted		bool
}


//AppendEntriesArgs
type AppendEntriesArgs struct {
	Term			int
	LeaderId		int
	PrevLogIndex	int
	PrevLogTerm		int
	Entries			[]Entry
	LeaderCommit 	int
}

//AppendEntriesReply
type AppendEntriesReply struct {
	Term			int
	Success			bool

	
	//for faster log backtracking
	ConflictIndex	int
	ConflictTerm	int
}

//common functionality wrapper
func (rf *Raft) revertToFollower(term int) {
	//NEED OUTSIDE LOCK PROTECTION
	rf.currentTerm = term
	prev_status := rf.status
	if prev_status == Leader {
		DPrintf("%d step down as leader", rf.me)
	}
	rf.status = Follower
	rf.votedFor = -1
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	
	if rf.currentTerm > args.Term {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}
	
	if rf.currentTerm < args.Term {
		rf.revertToFollower(args.Term)
	}

	reply.Term = rf.currentTerm

	DPrintf("%d at term %d receive vote request from %d at term %d", rf.me, rf.currentTerm, args.CandidateId, args.Term)
	
	// "at least up-to-date section 5.4"
	if args.LastLogTerm < rf.logs[len(rf.logs)-1].Term {
		reply.VoteGranted = false
		DPrintf("%d does not grant vote to %d -- staled candidate", rf.me, args.CandidateId)
		return
	}

	if args.LastLogTerm == rf.logs[len(rf.logs)-1].Term && args.LastLogIndex < rf.logs[len(rf.logs)-1].Index {
		reply.VoteGranted = false
		DPrintf("%d does not grant vote to %d -- log not at least as up to date", rf.me, args.CandidateId)
		return
	}

	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.voteTimeOutBase = time.Now()
		rf.voteTimeOutDuration = time.Duration(rand.Intn(VoteTimeOutUpper - VoteTimeOutLower + 1) + VoteTimeOutLower) * time.Millisecond
		DPrintf("%d grant vote to %d", rf.me, args.CandidateId)
	}
	return
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	if rf.currentTerm > args.Term {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}

	if rf.currentTerm < args.Term {
		rf.revertToFollower(args.Term)
	}
	DPrintf("%d: receieved appendentries req from %d, args prev log index is %d", rf.me, args.LeaderId, args.PrevLogIndex)
	if rf.currentTerm == args.Term {
		//AppendEntries from the current leader
		rf.voteTimeOutBase = time.Now()
		rf.voteTimeOutDuration = time.Duration(rand.Intn(VoteTimeOutUpper - VoteTimeOutLower + 1) + VoteTimeOutLower) * time.Millisecond

		reply.Term = rf.currentTerm
		if args.PrevLogIndex > rf.logs[len(rf.logs)-1].Index {
			reply.Success = false
			reply.ConflictIndex = rf.logs[len(rf.logs)-1].Index + 1
			reply.ConflictTerm = -1
			return
		}
		if rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm {
			reply.Success = false
			reply.ConflictTerm = rf.logs[args.PrevLogIndex].Term
			//log.Println("the term try to find is", reply.ConflictTerm)
			logtermtmp := []int{}
			for _, e := range rf.logs {
				logtermtmp = append(logtermtmp, e.Term)
			}
			//log.Println("the term array is ", logtermtmp)
			reply.ConflictIndex = rf.lowerBsearchIdx(reply.ConflictTerm)
			//log.Println("the found idx is ", reply.ConflictIndex)

			if reply.ConflictIndex > rf.logs[len(rf.logs)-1].Index {
				DPrintf("[%d] WARNING: leader want us to find a term that does not exists and is too high, with search term %d", rf.me, reply.ConflictTerm)				
			}
 			if rf.logs[reply.ConflictIndex].Term != reply.ConflictTerm {
				DPrintf("%d: follower does not find matching search term with found lower bound index %d, search term %d", rf.me, reply.ConflictIndex, reply.ConflictTerm)
			}
			return
		}
		if len(args.Entries) > 0 {
			pre_len := len(rf.logs)
			rf.appendEntriesResolver(args.Entries, args.PrevLogIndex)
			new_len := len(rf.logs)
			DPrintf("replica %d accept AppendEntries request from %d at term %d, args entries len is %d, args prev log index is %d, old log length is %d, new log length is %d", 
			rf.me, args.LeaderId, args.Term, len(args.Entries), args.PrevLogIndex, pre_len, new_len)
		
		
		
		}
		if args.LeaderCommit > rf.commitIndex {
			prev_commitIndex := rf.commitIndex
			
			if args.LeaderCommit < rf.logs[len(rf.logs)-1].Index {
				rf.commitIndex = args.LeaderCommit
			} else {
				rf.commitIndex = rf.logs[len(rf.logs)-1].Index
			}
			if prev_commitIndex != rf.commitIndex {
				DPrintf("replica %d, local commit Index %d, received LeaderCommit %d, index of last new entry %d, change to minimum of the latter", rf.me, prev_commitIndex, args.LeaderCommit, rf.logs[len(rf.logs)-1].Index)
				rf.applyChCond.Broadcast()
			}
			
		}

		
		reply.Success = true
	}

	return

	
}

func (rf *Raft) lowerBsearchIdx(searchTerm int) int{
	//assume outside lock
	tmp := sort.Search(len(rf.logs), func(i int) bool { return rf.logs[i].Term >= searchTerm})

	return tmp
}

func (rf *Raft) appendEntriesResolver(entries []Entry, prevLogIndex int) {
	//assume outside lock

	//logic: start from the first overlapping entry (if any), once we find a conflicting entry, overwriting the remaining.
	// This check helps prevent stale AppendEntries to overwrite new AppendEntries (both from the same leader).
	if prevLogIndex != rf.logs[len(rf.logs) -1].Index {
		//we overwrite some entries
		if prevLogIndex > rf.logs[len(rf.logs) -1].Index {
			DPrintf("[%d] WARNING prev log index larger than last index ", rf.me)
		}

		curIdx := prevLogIndex + 1
		for i, e := range entries {
			if curIdx > len(rf.logs) - 1 {
				rf.logs = append(rf.logs, entries[i:]...)
				return
			}
			if rf.logs[curIdx].Term != e.Term {
				rf.logs = rf.logs[:curIdx]
				rf.logs = append(rf.logs, entries[i:]...)
				return
			} else {
				curIdx++
				continue
			}
		}
	} else {
		rf.logs = append(rf.logs, entries...)
	}

	return
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}


//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.status != Leader {
		return -1, -1, false
	}
	index := rf.logs[len(rf.logs)-1].Index + 1
	term := rf.currentTerm
	isLeader := true
	//parsed := rf.parseCommand(command)
	
	newLogEntry := Entry{Index: rf.logs[len(rf.logs)-1].Index + 1, Op: command, Term: rf.currentTerm}
	rf.logs = append(rf.logs, newLogEntry)
	rf.persist()
	rf.matchIndex[rf.me] = index
	rf.nextIndex[rf.me] = index + 1
	
	
	DPrintf("%d as leader at term %d start agreement on index %d, command %v", rf.me, term, index, command)
	go rf.startAgreement(command)


	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	DPrintf("ATTENTION: %d get killed, log length %d, current term %d, status %d, lastentry term %d", rf.me, len(rf.logs), rf.currentTerm, rf.status, rf.logs[len(rf.logs)-1].Term)
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

//function that parse the command into string for future use
func (rf *Raft) parseCommand(command interface{}) interface{} {
	//TODO implement command parsing logic

	return command

}

//function for starting the agreement
func (rf *Raft) startAgreement(parsed interface{}) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.status == Leader {
		term := rf.currentTerm
		// newLogEntry := Entry{Index: rf.logs[len(rf.logs)-1].Index + 1, Op: parsed, Term: rf.currentTerm}
		// rf.logs = append(rf.logs, newLogEntry)
		// rf.matchIndex[rf.me]++
		for i := 0; i < len(rf.peers); i++ {
			if i != rf.me {
				go rf.heartBeatPerPeer(i, term)

			}
		}
	}

	return
}

func (rf *Raft) heartBeatPerPeer(x int, term int) {
	rf.mu.Lock()
	appendArgs := AppendEntriesArgs{Term: term, LeaderId: rf.me, LeaderCommit: rf.commitIndex, PrevLogIndex: rf.logs[rf.nextIndex[x]-1].Index, PrevLogTerm: rf.logs[rf.nextIndex[x]-1].Term}
	appendReply := AppendEntriesReply{}
	entries := make([]Entry, len(rf.logs[(appendArgs.PrevLogIndex+1):]))
	originalNextIdx := rf.nextIndex[x]
	copy(entries, rf.logs[(appendArgs.PrevLogIndex+1):])
	appendArgs.Entries = entries


	rf.mu.Unlock()
	ok := rf.sendAppendEntries(x, &appendArgs, &appendReply)
	if !ok {
		return
	} else {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if rf.status != Leader {
			return
		}
		replyTerm := appendReply.Term
		if rf.currentTerm < replyTerm {
			rf.revertToFollower(replyTerm)
			rf.persist()
			return
		}
		if replyTerm < rf.currentTerm {
			DPrintf("[%d] Received staled RPC response from %d to %d, continue ", rf.me, rf.me, x)
			return
		}
		if appendReply.Success {
			DPrintf("leader %d at term %d append entry request to replica %d succeed", rf.me, rf.currentTerm, x)
			prev_nextidx := originalNextIdx
			rf.matchIndex[x] = appendArgs.PrevLogIndex + len(appendArgs.Entries)
			rf.nextIndex[x] = rf.matchIndex[x] + 1
			DPrintf("leader %d at term %d change next idx record for replica %d: %d -> %d", rf.me, rf.currentTerm, x, prev_nextidx, rf.nextIndex[x])
		} else {
			DPrintf("[%d] Append real entry from %d to %d rejected, decrement nextIndex ", rf.me, rf.me, x)
			rf.nextIndex[x] = rf.decrementStrategy(originalNextIdx, appendReply.ConflictTerm, appendReply.ConflictIndex, x)
		}
		return

	}	
}

func (rf *Raft) decrementStrategy(oldIndex int, conflictTerm int, conflictIndex int, replyer int) int {
	//This can be optimized. requires outside lock
	if oldIndex == 0 {
		DPrintf("[%d] HUGE MISTAKE try to decrement nextIndex to negative", rf.me)
	}

	//decrement 1 at a time [DEPRECATED]
	//return oldIndex - 1
	if conflictTerm == 0 || conflictIndex == 0 {
		DPrintf("[%d] WARNING fail append entries but conflict term and confclit index not initialied", rf.me)
	}

	if conflictTerm == -1 {
		DPrintf("%d leader cannot find the index with conflict term %d for replyer %d: term not specified", rf.me, conflictTerm, replyer)
		return conflictIndex
	}

	tmp := rf.upperBsearchIdx(conflictTerm)
	if tmp > rf.logs[len(rf.logs)-1].Index || rf.logs[tmp].Term != conflictTerm {
		DPrintf("%d leader cannot find the index with conflict term %d for replyer %d, with the found idx as %d", rf.me, conflictTerm, replyer, tmp)
		return conflictIndex
	}

	return tmp + 1


}

func (rf *Raft) upperBsearchIdx(searchTerm int) int{
	//assume outside lock
	n := len(rf.logs)
	tmp := rf.upperBsearch(rf.logs, 0, n-1, searchTerm, n)
	//tmp := rf.upperBsearch(len(rf.logs), func(i int) bool { return rf.logs[i].Index <= searchTerm})
	
	return tmp


}

func (rf *Raft) upperBsearch(arr []Entry, low int, high int, targetTerm int, n int) int {
	if high >= low {
		mid := low + (high - low) / 2
		if (mid == n-1 || targetTerm < arr[mid + 1].Term) && arr[mid].Term == targetTerm {
			return mid
		} else {
			if (targetTerm < arr[mid].Term) {
				return rf.upperBsearch(arr, low, mid - 1, targetTerm, n)
			} else {
				return rf.upperBsearch(arr, mid + 1, high, targetTerm, n)
			}
		}
	}

	return n
	
}


// func (rf *Raft) upperBsearch(n int, f func(int) bool) int {
// 	//Define f(-1) == true and f(n) == false
// 	//Invariant: f(i) == true, f(j+1) == false
// 	//for ascendig sorted array: f is l[i] <= target
// 	i, j := 0, n
// 	for i <= j {
// 		h := int(uint(i+j) >> 1)
// 		//i <= h < j
// 		if !f(h) {
// 			j = h - 1 //preserving f(j+1) = false
// 		} else {
// 			i = h //preserving f(i) = true
// 		}
// 	}

// 	//i = j, f(i) is true, f(j+1)=f(i+1) is false, anwer is i
// 	return i
// }






// background function that periodically checks the vote timeout and issue requestVote (possibly through other RPC?)
func (rf *Raft) statusChecker() {

	for !rf.killed() {
		rf.mu.Lock()
		
		if time.Now().Sub(rf.voteTimeOutBase) > rf.voteTimeOutDuration && rf.status != Leader {
			DPrintf("%d kicks off election", rf.me)
			go rf.initVoting()
		}
		rf.mu.Unlock()
		time.Sleep(VoteSleep * time.Millisecond)

		
	}
	
	



}

func (rf *Raft) newCommitIndexFinder() (int, bool) {
	//this function assume outside lock!
	tmp := []int{}
	tmp = append(tmp, rf.matchIndex...)
	sort.Ints(tmp)
	pre := -1
	//DPrintf("leader %d at term %d, finder get called", rf.me, rf.currentTerm)
	//log.Println(rf.matchIndex)
	for i := rf.quorum - 1; i >= 0; i-- {
		//DPrintf("i is %d", i)
		if i < 0 {
			return -1, false
		}
		if tmp[i] == pre {
			continue
		}
		if tmp[i] > rf.commitIndex && rf.logs[tmp[i]].Term == rf.currentTerm {
			return tmp[i], true
		} else {
			pre = tmp[i]
		}
	}
	return -1, false

}
//function for leader to check commit
func (rf *Raft) committer() {
	for !rf.killed() {
		rf.mu.Lock()

		if rf.status == Leader {
			N, ok := rf.newCommitIndexFinder()
			if ok {
				DPrintf("leader %d at term %d finding new commit index result: %d", rf.me, rf.currentTerm, N)
				rf.commitIndex = N
				rf.applyChCond.Broadcast()
			}

			if rf.lastApplied < rf.commitIndex {
				rf.applier()
			}
		}
		
		if rf.status == Follower {
			if rf.lastApplied < rf.commitIndex {
				rf.applier()
			}
		}

		rf.mu.Unlock()
		time.Sleep(DefaultSleep *  time.Millisecond)
	}
}

//function to bring apply to commit \\TODO
func (rf *Raft) applier() {
	//assume outside lock
	rf.lastApplied = rf.commitIndex
	return
}

//function for leader server to send heartbeat
func (rf *Raft) leaderHeartBeat() {
	for !rf.killed(){
		rf.mu.Lock()

		if rf.status == Leader {
			term := rf.currentTerm
			for i:= 0; i < len(rf.peers); i++ {
				if i != rf.me {
					go rf.heartBeatPerPeer(i, term)
				}
			}
		} else {
			rf.mu.Unlock()
			DPrintf("leader %d step down, end leader heartbeat process! ", rf.me)
			return
		}

		rf.mu.Unlock()
		time.Sleep(HeartBeatSleep * time.Millisecond)
	}
}

// function that initialize a term of voting
func (rf *Raft) initVoting() {
	
	//change shared status variable
	rf.mu.Lock()
	//defer rf.mu.Unlock()
	DPrintf("%d start initVoting function", rf.me)

	rf.voteTimeOutBase = time.Now()
	rf.voteTimeOutDuration = time.Duration(rand.Intn(VoteTimeOutUpper - VoteTimeOutLower + 1) + VoteTimeOutLower) * time.Millisecond

	rf.status = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	cond := sync.NewCond(&rf.mu)
	votingTerm := rf.currentTerm
	yay_count, total_count := 1, 1 //vote for the server itself
	rf.persist()

	rf.mu.Unlock()
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			go func(x int) {
				rf.mu.Lock()
				args := RequestVoteArgs{Term: rf.currentTerm, CandidateId: rf.me, LastLogIndex: rf.logs[len(rf.logs)-1].Index, LastLogTerm: rf.logs[len(rf.logs)-1].Term}
				reply := RequestVoteReply{}
				DPrintf("%d send request vote to %d", rf.me, x)
				rf.mu.Unlock()
				ok := rf.sendRequestVote(x, &args, &reply)
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if !ok {
					//DPrintf("[%d] [ERROR] RequestVoteRPC to %d not responded, ", rf.me, x)
					total_count++
					cond.Broadcast()
					return
				}
				
				replyTerm := reply.Term
				if rf.currentTerm < replyTerm {
					rf.revertToFollower(replyTerm)
					total_count++
					rf.persist()
					cond.Broadcast()
					return
			
				}
				if replyTerm < rf.currentTerm {
					total_count++
					cond.Broadcast()
					return // do nothing
				}
				if reply.VoteGranted {
					yay_count++
				}
				total_count++
					cond.Broadcast()
				return
			}(i)
		}
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	for yay_count < rf.quorum && total_count != len(rf.peers) && !rf.killed() {
		cond.Wait()
	}
	if yay_count >= rf.quorum {
		if rf.currentTerm != votingTerm || rf.status != Candidate {
			return
		}
		// set up to be leader
		DPrintf("%d becomes leader -- term %d, yay %d, total %d", rf.me, rf.currentTerm, yay_count, total_count)
		rf.status = Leader
		// rf.voteTimeOutBase = time.Now() (No need to reset time when you become the leader?)
		for i := 0; i < len(rf.nextIndex); i++ {
			rf.nextIndex[i] = rf.logs[len(rf.logs)-1].Index + 1
			rf.matchIndex[i] = 0
		}
		go rf.leaderHeartBeat()



		return 
	} else {
		DPrintf("[%d], loss election at term %d!", rf.me, votingTerm)
		return
	}



	
}

func (rf *Raft) applyChMessenger(applyCh chan ApplyMsg) {
	DPrintf("%d applyChMsnger initiated", rf.me)
	prev_commited := 0
	it := 0
	for !rf.killed() {
		rf.mu.Lock()

		for prev_commited == rf.commitIndex {
			//DPrintf("%d applyChMsnger woke up but need to wait", rf.me)
			rf.applyChCond.Wait()
		}
		if prev_commited > rf.commitIndex {
			DPrintf("[%d] SOMETHING WENT WRONG the prev_commited idx %d is larger than rf.commitIndex %d", rf.me, prev_commited, rf.commitIndex)
		}
		DPrintf("%d applyChMessenger wake up, prev_commited is %d and new commitIndex is %d", rf.me, prev_commited, rf.commitIndex)
		go func (prev_commited int, new_commited int) {
			rf.chmu.Lock()
			for i := prev_commited + 1; i <= new_commited; i++ {
				rf.mu.Lock()
				applyChMsg := ApplyMsg{CommandValid: true, Command: rf.logs[i].Op, CommandIndex: i, RaftTerm: rf.currentTerm}
				rf.mu.Unlock()
				applyCh <- applyChMsg
				DPrintf("%d send applyCh msg at index %d, at time %v", rf.me, i, time.Now())
				
			}
			rf.chmu.Unlock()
		}(prev_commited, rf.commitIndex)
		
		prev_commited = rf.commitIndex
		it++
		rf.mu.Unlock()

	}
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rand.Seed(time.Now().UnixNano())

	rf := &Raft{}


	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyChCond = sync.NewCond(&rf.mu)

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.votedFor = -1
	//how to initialize log array? special entry?
	rf.logs = []Entry{
		Entry {
			Index: 0,
			Op:	StartLog,
			Term: 0,
		},
	}
	
	//volatile for all servers
	rf.commitIndex = 0
	rf.lastApplied = 0

	//volatile for leaders
	rf.nextIndex = []int{}
	rf.matchIndex = []int{}

	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex = append(rf.nextIndex, 1)
		rf.matchIndex = append(rf.matchIndex, 0)
	}



	//timeout initialization
	rf.voteTimeOutBase = time.Now()
	rf.voteTimeOutDuration = time.Duration(rand.Intn(VoteTimeOutUpper - VoteTimeOutLower + 1) + VoteTimeOutLower) * time.Millisecond
	rf.quorum = len(rf.peers)/2 + 1

	//status
	rf.status = Follower

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.statusChecker()
	go rf.committer()
	go rf.applyChMessenger(applyCh)

	DPrintf("%d initialized, ", rf.me)


	return rf
}



//TODO item
/**
1) one goroutine for sending to each peer and retry indefinitely. Always check the leader status
2) When entry has been "safely replicated", applied immediately in the leader.
3) A log entry is commited once the leader that created the entry has replicated it on a majority of the servers. This also commits all preceding entries in the leaders' log, 
including entries created by previous leaders.
4) Leader keep track of the highest index it knows to be committed. Send it to followers in heartbeat.
5) Add a logic to handle retry the AppendEntriesRPC
6) When a leader cames into power, it initialize all nextIndex values to the index just after the last one in its log.\\
7) We need a committer()


**/
 