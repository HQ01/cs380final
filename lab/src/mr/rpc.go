package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
type WorkArgs struct {
	Workerid string
}

type WorkReply struct {
	Isfinished bool

	Taskid int
	Filename string
	MapReduce string //indicating the task type is Map or Reduce
	BucketNumber int
}

type CommitArgs struct {
	Workerid string
	Taskid int
	MapReduce string
}

type CommitReply struct {
	IsOK bool
}


// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
