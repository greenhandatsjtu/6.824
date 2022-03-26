package mr

import (
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
	taskStatus TaskStatus  // task status
	worker     *WorkerInfo // task assigned worker
	file       string      // map task file name
}

type Coordinator struct {
	// Your definitions here.
	mutex sync.RWMutex

	workers []WorkerInfo

	mapTasks           []TaskInfo
	unfinishedMapTasks int // number of unfinished map tasks
	nMap               int // number of map tasks

	reduceTasks           []TaskInfo
	unfinishedReduceTasks int // number of unfinished reduce tasks
	nReduce               int // number of reduce tasks
}

func NewCoordinator(files []string, nReduce int) *Coordinator {
	mapTasks := make([]TaskInfo, len(files))
	for i := range mapTasks {
		mapTasks[i].file = files[i]
	}
	reduceTasks := make([]TaskInfo, nReduce)
	c := Coordinator{sync.RWMutex{}, []WorkerInfo{}, mapTasks, len(files), len(files), reduceTasks, nReduce, nReduce}

	return &c
}

// Your code here -- RPC handlers for the worker to call.

//TODO: add health check

// ShakeHands shakes hands with worker, return worker id and nReduce
func (c *Coordinator) ShakeHands(args *ShakeArgs, reply *ShakeReply) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	// reply with worker id and nReduce
	reply.WorkerId = len(c.workers)
	reply.NMap = c.nMap
	reply.NReduce = c.nReduce

	// add new worker to works list, alive at first
	c.workers = append(c.workers, WorkerInfo{true})
	return nil
}

// Task issues a map/reduce task to worker
func (c *Coordinator) Task(args *TaskArgs, reply *TaskReply) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if c.unfinishedMapTasks > 0 || c.unfinishedReduceTasks > 0 {
		var tasks []TaskInfo
		var taskType TaskType
		if c.unfinishedMapTasks > 0 {
			// assign map tasks before all map tasks finished
			tasks = c.mapTasks
			taskType = MapTask
		} else {
			// assign reduce tasks after all map tasks finished
			tasks = c.reduceTasks
			taskType = ReduceTask
		}
		for i, task := range tasks {
			if task.taskStatus == Unassigned {
				// update task status
				tasks[i].taskStatus = Executing
				// assign task to this worker
				tasks[i].worker = &c.workers[args.WorkerId]

				reply.Task = Task{
					Type:     taskType,
					FileName: task.file,
					Number:   i,
				}
				break
			}
		}
		// all tasks assigned but some tasks unfinished, reply Nop task
		// don't need to explicitly set task.Type to NopTask as this field is default to 0
	} else {
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
		if c.unfinishedMapTasks == 0 {
			log.Println("All map tasks finished.")
		}
	case ReduceTask:
		c.unfinishedReduceTasks--
		c.reduceTasks[args.Number].taskStatus = Finished
		c.reduceTasks[args.Number].worker = nil
		if c.unfinishedReduceTasks == 0 {
			log.Println("All reduce tasks finished.")
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
	// Your code here.
	// must read lock here
	c.mutex.RLock()
	defer c.mutex.RUnlock()
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
