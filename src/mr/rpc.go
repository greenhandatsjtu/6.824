package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

// Add your RPC definitions here.

type TaskType uint8

const (
	NopTask TaskType = iota // no task assigned
	MapTask
	ReduceTask
	ExitTask
)

type Task struct {
	Type     TaskType // task type
	FileName string   // map task file
	Number   int      // task number
}

type TaskArgs struct {
	WorkerId int // worker id
}

type TaskReply struct {
	Task Task
}

type FinishArgs struct {
	Type   TaskType // task type
	Number int      // task number
}

type FinishReply struct {
}

type ShakeArgs struct {
}

type ShakeReply struct {
	WorkerId int // worker id
	NReduce  int // number of reduce tasks
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
