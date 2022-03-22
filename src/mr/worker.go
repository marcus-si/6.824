package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strings"
	"time"
)

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

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	for {
		reply, ok := FetchTask()
		if !ok {
			fmt.Println("unable to contact the coordinator and quit")
			return
		}
		if reply.WorkType == C || reply.TaskId == -1 {
			fmt.Println("Work is done and quit")
			return
		}
		fmt.Printf("start task %d of type %d at %v\n", reply.TaskId, reply.WorkType, time.Now())
		if reply.WorkType == M {
			handleMapTask(mapf, reply)
		} else if reply.WorkType == R {
			handleReduceTask(reducef, reply)
		}
	}

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

func handleMapTask(mapf func(string, string) []KeyValue, reply *FetchTaskReply) {
	content := reply.FileBody
	kva := mapf(reply.FilePath, content)
	hashMap := make(map[int][]KeyValue, reply.ReducerNum)
	for _, kv := range kva {
		reduceTaskNumber := ihash(kv.Key) % reply.ReducerNum
		hashMap[reduceTaskNumber] = append(hashMap[reduceTaskNumber], kv)
	}
	completed := true
	for key, value := range hashMap {
		fileName := fmt.Sprintf("mr-%d-%d", reply.TaskId, key)
		file, err := os.OpenFile(fileName, os.O_WRONLY|os.O_TRUNC|os.O_CREATE, 0666)
		defer func() {
			file.Close()
		}()
		if err == nil {
			enc := json.NewEncoder(file)
			for _, kv := range value {
				enc.Encode(&kv)
			}
		} else {
			completed = false
		}
	}
	if completed {
		CompleteTask(reply.TaskId, reply.WorkType, true)
	}
}

func handleReduceTask(reducef func(string, []string) string, reply *FetchTaskReply) {
	kva := reply.Kva
	sort.Slice(kva, func(i, j int) bool {
		return kva[i].Key < kva[j].Key
	})
	ok := reduceToFile(reducef, kva, "mr-out-"+reply.FilePath)
	if ok {
		CompleteTask(reply.TaskId, reply.WorkType, true)
	}
}

func reduceToFile(reducef func(string, []string) string, kva []KeyValue, filePath string) (ok bool) {
	var output []string
	startKv := kva[0]
	values := []string{startKv.Value}
	for i := 1; i < len(kva); i++ {
		if startKv.Key == kva[i].Key {
			values = append(values, kva[i].Value)
		} else {
			occurrences := reducef(startKv.Key, values)
			output = append(output, fmt.Sprintf("%v %v\n", startKv.Key, occurrences))
			startKv = kva[i]
			values = []string{startKv.Value}
		}
	}
	occurrences := reducef(startKv.Key, values)
	output = append(output, fmt.Sprintf("%v %v", startKv.Key, occurrences))
	file, err := os.OpenFile(filePath, os.O_WRONLY|os.O_APPEND|os.O_TRUNC|os.O_CREATE, 0644)
	defer func() {
		file.Close()
	}()
	if err != nil {
		return false
	}
	_, writingErr := file.WriteString(strings.Join(output, ""))
	return writingErr == nil
}

func FetchTask() (*FetchTaskReply, bool) {
	args := FetchTaskArgs{WorkerId: 1}
	reply := FetchTaskReply{}
	ok := call("Coordinator.AssignTask", &args, &reply)
	if !ok {
		fmt.Printf("call failed!\n")
	}
	return &reply, ok
}

func CompleteTask(taskId int, workType WorkType, completed bool) *CompleteTaskReply {
	args := CompleteTaskArgs{taskId, workType, completed}
	reply := CompleteTaskReply{}
	ok := call("Coordinator.CompleteTask", &args, &reply)
	if ok {
		fmt.Printf("completed task %d of type %d at %v\n", args.TaskId, args.WorkType, time.Now())
	} else {
		fmt.Printf("unable to notify Coordinator about the task[id=%d] completed\n", args.TaskId)
	}
	return &reply
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
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
