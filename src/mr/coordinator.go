package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

type Coordinator struct {
	// Your definitions here.
	mutex    sync.RWMutex
	files    []string
	mapTasks map[string]uint64
	nReduce  int
}

func NewCoordinator(files []string, nReduce int) *Coordinator {
	tasks := make(map[string]uint64)
	c := Coordinator{sync.RWMutex{}, files, tasks, nReduce}

	return &c
}

// Your code here -- RPC handlers for the worker to call.

// Task issues a map/reduce task to worker
func (c *Coordinator) Task(args *TaskArgs, reply *TaskReply) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	for i, file := range c.files {
		if _, ok := c.mapTasks[file]; !ok {
			c.mapTasks[file] = args.Id
			reply.Task = Task{
				Type:     MapTask,
				FileName: file,
				Number:   i,
			}
			break
		}
	}
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := NewCoordinator(files, nReduce)

	// Your code here.

	c.server()
	return c
}
