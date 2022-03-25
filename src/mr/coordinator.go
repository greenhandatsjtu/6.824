package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

type TaskStatus uint8

const (
	Unassigned TaskStatus = iota
	Executing
	Finished
)

type WorkerInfo struct {
	alive bool
}

type TaskInfo struct {
	taskStatus TaskStatus
	worker     *WorkerInfo
}

type MapTaskInfo struct {
	TaskInfo
	file string
}

type ReduceTaskInfo TaskInfo

type Coordinator struct {
	// Your definitions here.
	mutex sync.RWMutex

	workers []WorkerInfo

	mapTasks           []MapTaskInfo
	unfinishedMapTasks int

	reduceTasks           []ReduceTaskInfo
	unfinishedReduceTasks int
	nReduce               int
}

func NewCoordinator(files []string, nReduce int) *Coordinator {
	mapTasks := make([]MapTaskInfo, len(files))
	for i := range mapTasks {
		mapTasks[i].file = files[i]
	}
	reduceTasks := make([]ReduceTaskInfo, nReduce)
	c := Coordinator{sync.RWMutex{}, []WorkerInfo{}, mapTasks, len(files), reduceTasks, nReduce, nReduce}

	return &c
}

// Your code here -- RPC handlers for the worker to call.

// Shake shakes hands with worker, return worker id and nReduce
func (c *Coordinator) Shake(args *ShakeArgs, reply *ShakeReply) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	// reply with worker id and nReduce
	reply.WorkerId = len(c.workers)
	reply.NReduce = c.nReduce

	// add new worker to works list, alive at first
	c.workers = append(c.workers, WorkerInfo{true})
	return nil
}

// Task issues a map/reduce task to worker
func (c *Coordinator) Task(args *TaskArgs, reply *TaskReply) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if c.unfinishedMapTasks > 0 {
		for i, task := range c.mapTasks {
			if task.taskStatus == Unassigned {
				// update task status
				c.mapTasks[i].taskStatus = Executing
				// assign task to this worker
				c.mapTasks[i].worker = &c.workers[args.WorkerId]

				reply.Task = Task{
					Type:     MapTask,
					FileName: task.file,
					Number:   i,
				}
				break
			}
		}
	} else {
		fmt.Println("All map task finished")
		reply.Task.Type = ExitTask
	}
	return nil
}

// Finish receives message from workers indicating that a work has been finished
func (c *Coordinator) Finish(args *FinishArgs, _ *FinishReply) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	switch args.Type {
	case MapTask:
		c.unfinishedMapTasks--
		c.mapTasks[args.Number].taskStatus = Finished
		c.mapTasks[args.Number].worker = nil
		break
	case ReduceTask:
		c.unfinishedReduceTasks--
		c.reduceTasks[args.Number].taskStatus = Finished
		c.reduceTasks[args.Number].worker = nil
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
	// Your code here.

	return c.unfinishedReduceTasks == 0
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
