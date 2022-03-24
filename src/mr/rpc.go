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
	MapTask TaskType = iota
	ReduceTask
	ExitTask
)

type Task struct {
	Type     TaskType
	FileName string
	Number   int
}

type TaskArgs struct {
	Id uint64
}

type TaskReply struct {
	Task Task
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
