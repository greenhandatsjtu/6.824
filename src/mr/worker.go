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
		log.Printf("Worker %d\tmap task: %s\n", workerId, task.FileName)
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

		var outputs []*os.File
		var encs []*json.Encoder
		for i := 0; i < nReduce; i++ {
			output, err := os.OpenFile(fmt.Sprintf("mr-%d-%d", task.Number, i), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
			if err != nil {
				return err
			}
			outputs = append(outputs, output)
			encs = append(encs, json.NewEncoder(output))
		}
		for _, kv := range kva {
			i := ihash(kv.Key) % nReduce // compute reduce task number
			if err = encs[i].Encode(&kv); err != nil {
				return err
			}
		}
		for _, output := range outputs {
			if err = output.Close(); err != nil {
				return err
			}
		}
	case ReduceTask:
		log.Printf("Worker %d\treduce task: %d\n", workerId, task.Number)
		var kva []KeyValue

		// read intermediate K/V pairs from nMap files
		for i := 0; i < nMap; i++ {
			file, err := os.Open(fmt.Sprintf("mr-%d-%d", i, task.Number))
			if err != nil {
				return err
			}

			// parse intermediate K/V pairs
			dec := json.NewDecoder(file)
			for {
				var kv KeyValue
				if err := dec.Decode(&kv); err != nil {
					break
				}
				kva = append(kva, kv)
			}
			if err = file.Close(); err != nil {
				return err
			}
		}

		// create output file
		ofile, err := os.OpenFile(fmt.Sprintf("mr-out-%d", task.Number), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
		if err != nil {
			return err
		}

		sort.Sort(ByKey(kva))
		i := 0
		for i < len(kva) {
			j := i + 1
			// merge K/V pairs with identical key
			for j < len(kva) && kva[j].Key == kva[i].Key {
				j++
			}
			var values []string
			for k := i; k < j; k++ {
				values = append(values, kva[k].Value)
			}
			output := reducef(kva[i].Key, values)

			// this is the correct format for each line of Reduce output.
			if _, err = fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output); err != nil {
				return err
			}

			i = j
		}
		if err = ofile.Close(); err != nil {
			return err
		}
	case ExitTask:
		fmt.Println("Exiting...")
		os.Exit(0)
	}
	return nil
}

var workerId int
var nMap int
var nReduce int

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	// shake hands
	reply := CallShake()
	workerId = reply.WorkerId
	nMap = reply.NMap
	nReduce = reply.NReduce

	// ask coordinator for task periodically
	for true {
		task := CallTask()
		if err := execTask(task, mapf, reducef); err != nil {
			log.Fatal(err)
		}
		if task.Type == MapTask || task.Type == ReduceTask {
			CallFinish(task.Type, task.Number)
		}
		time.Sleep(time.Second)
	}
}

// CallShake shakes hands with coordinator, receives workerId and nReduce
func CallShake() ShakeReply {
	args := ShakeArgs{}
	reply := ShakeReply{}

	if ok := call("Coordinator.ShakeHands", &args, &reply); !ok {
		fmt.Println("call ShakeHands failed!")
	}
	return reply
}

// CallTask asks coordinator for new task
func CallTask() *Task {
	args := TaskArgs{workerId}
	reply := TaskReply{}

	// send the RPC request, wait for the reply.
	ok := call("Coordinator.Task", &args, &reply)
	if !ok {
		fmt.Printf("call task failed!\n")
	}
	return &reply.Task
}

// CallFinish indicates coordinator this worker has finished a task
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
