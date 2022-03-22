package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

type FetchTaskArgs struct {
	WorkerId int
}

type FetchTaskReply struct {
	WorkType   WorkType
	FilePath   string
	FileBody   string
	Kva        []KeyValue
	TaskId     int
	ReducerNum int
	MapTaskNum int
}

type WorkType int

const (
	M WorkType = iota
	R
	C //completed
)

type CompleteTaskArgs struct {
	TaskId    int
	WorkType  WorkType
	Completed bool
}

type CompleteTaskReply struct {
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

const InputFilesPersistentPath = "input-files"
