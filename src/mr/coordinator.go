package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"regexp"
	"sync/atomic"
	"time"
)

// var taskMapMutex = &sync.RWMutex{}

const (
	UNASSIGNED uint32 = iota
	ASSIGNED
	COMPLETED
)

type Coordinator struct {
	// Your definitions here.
	TaskList                []*Task
	UnassignedTaskList      []Task
	AssignedTaskMap         map[int]Task
	MapTaskNum              int
	CompletedMapTaskNum     uint32
	CompletedTaskNum        uint32
	CompletedTaskNumChannel chan uint32
	ReducerNum              int
}

type Task struct {
	Id        int
	FilePath  string
	FileBody  string
	StartTime time.Time
	WorkType  WorkType
	LockState uint32
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) AssignTask(args *FetchTaskArgs, reply *FetchTaskReply) error {
	//try to find a unassigned map task
	if c.Done() {
		reply.WorkType = C
		return nil
	}
	ok := findTask(c, c.ReducerNum, reply)
	if !ok {
		reply.TaskId = -1
	}
	return nil
}

func findTask(c *Coordinator, reducerNum int, reply *FetchTaskReply) (ok bool) {
	for _, value := range c.TaskList {
		var kva []KeyValue
		var reduceFileOk bool
		if value.WorkType == R {
			kva, reduceFileOk = isReduceFileReady(value.FilePath, c.MapTaskNum)
			if !reduceFileOk {
				continue
			}
		}
		if atomic.CompareAndSwapUint32(&value.LockState, UNASSIGNED, ASSIGNED) {
			reply.FilePath = value.FilePath
			reply.WorkType = value.WorkType
			reply.TaskId = value.Id
			reply.ReducerNum = reducerNum
			reply.MapTaskNum = c.MapTaskNum
			reply.Kva = kva
			reply.FileBody = value.FileBody
			return true
		}
	}
	return false
}

func isReduceFileReady(reduceFilePath string, mapTaskNum int) (kva []KeyValue, ok bool) {
	fileNamePattern, _ := regexp.Compile("mr-\\d+-" + reduceFilePath)
	var tempFilePaths []string
	files, err := ioutil.ReadDir("./")
	if err != nil {
		log.Fatalln("unalbe to load intermediate files")
		return
	}
	for _, f := range files {
		if fileNamePattern.MatchString(f.Name()) {
			tempFilePaths = append(tempFilePaths, f.Name())
		}
	}
	ok = len(tempFilePaths) == mapTaskNum
	if ok {
		for _, f := range tempFilePaths {
			file, err := os.OpenFile(f, os.O_RDONLY, 0444)
			defer func() {
				file.Close()
			}()
			if err == nil {
				dec := json.NewDecoder(file)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					kva = append(kva, kv)
				}
			} else {
				return kva, false
			}
		}
	}
	return
}

func (c *Coordinator) CompleteTask(args *CompleteTaskArgs, reply *CompleteTaskReply) error {
	for _, value := range c.TaskList {
		if value.Id == args.TaskId {
			if args.Completed && atomic.CompareAndSwapUint32(&value.LockState, ASSIGNED, COMPLETED) {
				atomic.AddUint32(&c.CompletedTaskNum, 1)
			} else if !args.Completed && atomic.CompareAndSwapUint32(&value.LockState, ASSIGNED, UNASSIGNED) {
				fmt.Println("unlocked an incomplete job of id", args.TaskId)
			}
			return nil
		}
	}
	return nil
}

func createReduceTasks(c *Coordinator) {
	for i := 0; i < c.ReducerNum; i++ {
		id := c.MapTaskNum + i
		task := Task{Id: id, FilePath: fmt.Sprint(i), WorkType: R, LockState: UNASSIGNED}
		c.TaskList[id] = &task
	}
}

// func checkAndMoveAssignedTasksBackToUnlocked(c *Coordinator, maxAllowedTimeInSeconds int) {
// 	duration := time.Duration(maxAllowedTimeInSeconds * int(time.Second))
// 	for !c.Done() {
// 		time.Sleep(duration)
// 		now := time.Now()
// 		taskMapMutex.RLock()
// 		for _, value := range c.TaskList {
// 			if value.StartTime.Add(duration).Before(now) && atomic.CompareAndSwapUint32(&value.LockState, LOCKED, UNLOCKED) {
// 				fmt.Println("a slow job")
// 			}
// 		}
// 		taskMapMutex.RUnlock()
// 	}
// }

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
	log.Println("Coordinator is listening at unix:/" + sockname)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	if atomic.CompareAndSwapUint32(&c.CompletedTaskNum, uint32(c.MapTaskNum+c.ReducerNum), 0) {
		ret = true
	}

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.ReducerNum = nReduce

	// Your code here.
	// Save file list
	fileNum := len(files)
	c.MapTaskNum = fileNum
	totalTasks := fileNum + nReduce

	//initialize assigned task map with a capicity of total tasks
	c.TaskList = make([]*Task, totalTasks)
	c.CompletedTaskNumChannel = make(chan uint32, 1)

	actualMapTaskNum := 0
	//create map tasks
	for i := 0; i < fileNum; i++ {
		print("File", i, files[i])
		body, err := ioutil.ReadFile(files[i])
		if err == nil {
			newTask := Task{Id: i, FilePath: files[i], WorkType: M, LockState: UNASSIGNED, FileBody: string(body)}
			c.TaskList[i] = &newTask
			actualMapTaskNum++
		}

	}
	c.MapTaskNum = actualMapTaskNum
	createReduceTasks(&c)

	//create reduce tasks

	// file, err := os.OpenFile(InputFilesPersistentPath, os.O_WRONLY|os.O_APPEND|os.O_TRUNC|os.O_CREATE, 0644)
	// if err != nil {
	// 	log.Fatal(err)
	// }
	// defer file.Close()
	// for i := 0; i < len(files); i++ {
	// 	_, err = file.WriteString(files[i] + "\n")
	// 	if err != nil {
	// 		log.Fatal(err)
	// 	}
	// }
	c.server()
	// go checkAndMoveAssignedTasksBackToUnlocked(&c, 15)
	// go insertIntoAssignedTaskMap(&c)
	// go insertIntoUnassignedTaskList(&c)
	return &c
}
