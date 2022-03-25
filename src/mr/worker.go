package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// execTask execute a task
func execTask(task *Task, mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) error {
	// RPC request to fetch task

	switch task.Type {
	case MapTask:
		fmt.Println(task.FileName)
		file, err := os.Open(task.FileName)
		if err != nil {
			return err
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			return err
		}
		if err = file.Close(); err != nil {
			return err
		}

		kva := mapf(task.FileName, string(content))
		sort.Sort(ByKey(kva))

		output, err := os.OpenFile(fmt.Sprintf("mr-%d.txt", task.Number), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
		if err != nil {
			return nil
		}
		enc := json.NewEncoder(output)
		for _, kv := range kva {
			if err = enc.Encode(&kv); err != nil {
				return err
			}
		}
		if err = output.Close(); err != nil {
			return err
		}
	case ExitTask:
		fmt.Println("Exiting...")
		os.Exit(0)
	}
	return nil
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	// ask coordinator for task periodically
	for true {
		task := CallTask()
		if err := execTask(task, mapf, reducef); err != nil {
			log.Fatal(err)
		}
		CallFinish(task.Type, task.Number)
		time.Sleep(time.Second)
	}
}

func CallTask() *Task {
	// declare an argument structure.
	args := TaskArgs{}

	// fill in the argument(s).
	args.Id = 99

	// declare a reply structure.
	reply := TaskReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Task", &args, &reply)
	if !ok {
		fmt.Printf("call task failed!\n")
	}
	return &reply.Task
}

func CallFinish(taskType TaskType, number int) {
	args := FinishArgs{taskType, number}
	if ok := call("Coordinator.Finish", &args, nil); !ok {
		fmt.Println("call finish failed!")
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
