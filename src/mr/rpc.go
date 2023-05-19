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

type MapTask struct {
	Filename string
	TaskId   int
	NReduce  int
}

type ReduceTask struct {
	Intermediate []string
	TaskId       int
}

type TaskRequestArgs struct {
}

type TaskRequestReply struct {
	Map    *MapTask
	Reduce *ReduceTask
}

type MapTaskFinishedArgs struct {
	Success     bool
	Ofiles      map[int][]string
	TempFileMap map[string]string
	TaskId      int
}

type MapTaskFinishedReply struct {
}

type ReduceTaskFinishedArgs struct {
	Success      bool
	TempFilename string
	TaskId       int
}

type ReduceTaskFinishedReply struct {
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
