package mr

import (
	"context"
	"errors"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

const (
	TaskIdle = iota
	TaskWorking
	TaskCommit
)


type Master struct {
	// Your definitions here.
	files []string
	nReduce int

	//init with 0
	mapTasks []int
	reduceTasks []int

	mapCount int
	//init with -1
	workerCommit map[string]int
	allCommited bool

	//init with 10 secods
	timeout time.Duration

	mu sync.RWMutex

}

func (m *Master) Work(args *WorkArgs, reply *WorkReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	//first to map
	for k, v := range m.files {
		if m.mapTasks[k] != TaskIdle {
			continue
		}
		reply.Taskid = k
		reply.Filename = v
		reply.MapReduce = "map"
		reply.BucketNumber = m.nReduce
		reply.Isfinished = false
		m.workerCommit[args.Workerid] = TaskWorking
		m.mapTasks[k] = TaskWorking

		ctx, _ := context.WithTimeout(context.Background(), m.timeout)
		go func() {
			select {
			case <- ctx.Done():
				{
					m.mu.Lock()
					defer m.mu.Unlock()
					if m.workerCommit[args.Workerid] != TaskCommit && m.mapTasks[k] != TaskCommit {
						m.mapTasks[k] = TaskIdle
						log.Println("[Error]:", "worker", args.Workerid, "map task:", k, "timeout")
					}
				}
			}
		}()
		return nil // so that RPC will finish
	}

	//then dispatch reduce work
	for k, v := range m.reduceTasks {
		if m.mapCount != len(m.files) {
			return nil // TODO why should we return nil here? 
		}
		if v != TaskIdle {
			continue
		}

		reply.Taskid = k
		reply.Filename = ""
		reply.MapReduce = "reduce"
		reply.BucketNumber = len(m.files)
		reply.Isfinished = false
		m.workerCommit[args.Workerid] = TaskWorking
		m.reduceTasks[k] = TaskWorking

		ctx, _ := context.WithTimeout(context.Background(), m.timeout)
		go func () {
			select {
			case <- ctx.Done():{
				m.mu.Lock()
				if m.workerCommit[args.Workerid] != TaskCommit && m.reduceTasks[k] != TaskCommit {
					m.reduceTasks[k] = TaskIdle
					log.Println("[Error] : ", " worker: ", args.Workerid, " reduce taks: ", k, " timeout ")
				}
				m.mu.Unlock()
			}
			}
		}()

		log.Println("a worker", args.Workerid, "apply a reduce task:", *reply)

		return nil

	}


	//handling Commit
	for _, v := range m.workerCommit {
		if v == TaskWorking {
			reply.Isfinished = false
			return nil
		}
	}

	reply.Isfinished = true //I think there might be a quick way to set reply.isfinished = true once all commited.
	//question: how to initialize workercommit?
	return errors.New("worker apply but no tasks to dispatch, TODO check whether all tasks are finished")
}

func (m *Master) Commit(args *CommitArgs, reply *CommitReply) error {
	log.Println("a worker", args.Workerid, "commit a " + args.MapReduce + " task: ", args.Taskid)
	m.mu.Lock()
	switch args.MapReduce {
		case "map":
			{
				m.mapTasks[args.Taskid] = TaskCommit
				m.workerCommit[args.Workerid] = TaskCommit
				m.mapCount++
			}
		case "reduce":
			{
				m.reduceTasks[args.Taskid] = TaskCommit
				m.workerCommit[args.Workerid] = TaskCommit
			}
	}
	m.mu.Unlock()

	log.Println("current", m.mapTasks, m.reduceTasks)
	for _, v := range m.mapTasks {
		if v != TaskCommit {
			return nil
		}
	}

	for _, v := range m.reduceTasks {
		if v != TaskCommit {
			return nil
		}
	}
	m.allCommited = true
	log.Println("all task completed!")
	return nil

}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 99999
	return nil
}


//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {

	// Your code here.
	return m.allCommited

}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{
		files:			files,
		nReduce:		nReduce,
		mapTasks:		make([]int, len(files)),
		reduceTasks:	make([]int, nReduce),
		workerCommit: 	make(map[string]int),
		allCommited:	false,
		timeout:		10 * time.Second,
	}

	// Your code here.


	m.server()
	return &m
}
