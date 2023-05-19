package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	mapTaskFile            map[int]string //maps map task id to filename
	mapTaskStatus          []int          //0 -> not done, 1 -> in progress, 2 -> done
	mapTaskAssignedTime    []time.Time
	reduceTaskStatus       []int //0 -> not done, 1 -> assigned
	reduceTaskAssignedTime []time.Time
	intermediateFiles      map[int][]string //inter files for reduce task
	nReduce                int
	mu                     sync.Mutex
	JobDone                bool
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) MapTaskFinishedHandler(args *MapTaskFinishedArgs, reply *MapTaskFinishedReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	id := args.TaskId

	if !args.Success {
		c.mapTaskStatus[id] = 0
	} else {
		c.mapTaskStatus[id] = 2
		for oname, tempname := range args.TempFileMap {
			os.Rename(tempname, oname)
		}

		for key, values := range args.Ofiles {
			c.intermediateFiles[key] = append(c.intermediateFiles[key], values...)
		}
	}

	return nil
}

func (c *Coordinator) ReduceTaskFinishedHandler(args *ReduceTaskFinishedArgs, reply *ReduceTaskFinishedReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	id := args.TaskId

	if !args.Success {
		c.reduceTaskStatus[id] = 0
	} else {
		c.reduceTaskStatus[id] = 2
		ofile := "mr-out-" + fmt.Sprint(args.TaskId)
		os.Rename(args.TempFilename, ofile)
	}

	return nil
}

func (c *Coordinator) TaskRequestHandler(args *TaskRequestArgs, reply *TaskRequestReply) error {
	reply.Map = nil
	reply.Reduce = nil
	c.mu.Lock()
	defer c.mu.Unlock()
	//find available map task
	ok := false
	for i := 0; i < len(c.mapTaskStatus); i++ {
		if c.mapTaskStatus[i] == 1 && c.mapTaskAssignedTime[i].Before(time.Now().Add(-time.Second*10)) {
			c.mapTaskStatus[i] = 0
		}

		if c.mapTaskStatus[i] == 0 {
			c.mapTaskAssignedTime[i] = time.Now()
			c.mapTaskStatus[i] = 1
			newTask := MapTask{Filename: c.mapTaskFile[i], TaskId: i, NReduce: c.nReduce}
			reply.Map = &newTask

			return nil
		}

		if c.mapTaskStatus[i] != 2 {
			ok = true
		}
	}

	//if at least one map task remaining, wait and return
	if ok {
		return nil
	}

	//assign available reduce task
	for i := 0; i < c.nReduce; i++ {
		if c.reduceTaskStatus[i] == 1 && c.reduceTaskAssignedTime[i].Before(time.Now().Add(-time.Second*10)) {
			c.reduceTaskStatus[i] = 0
		}
		if c.reduceTaskStatus[i] == 0 {
			c.reduceTaskAssignedTime[i] = time.Now()
			c.reduceTaskStatus[i] = 1
			newTask := ReduceTask{Intermediate: c.intermediateFiles[i], TaskId: i}
			reply.Reduce = &newTask
			return nil
		}
	}

	c.JobDone = true
	return nil
}

// start a thread that listens for RPCs from worker.go
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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := c.JobDone

	// Your code here.
	if ret {
		files, err := filepath.Glob("./map-*-*.out")
		if err != nil {
			panic(err)
		}
		for _, f := range files {
			if err := os.Remove(f); err != nil {
				panic(err)
			}
		}
	}

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	c.mu.Lock()
	c.intermediateFiles = make(map[int][]string)
	c.mapTaskStatus = make([]int, len(files))
	c.mapTaskAssignedTime = make([]time.Time, len(files))
	c.reduceTaskStatus = make([]int, nReduce)
	c.reduceTaskAssignedTime = make([]time.Time, nReduce)
	c.mapTaskFile = make(map[int]string)
	// Your code here.
	for i, file := range files {
		c.mapTaskFile[i] = file
		c.mapTaskStatus[i] = 0
	}

	for i := 0; i < nReduce; i++ {
		c.reduceTaskStatus[i] = 0
	}

	c.nReduce = nReduce
	c.mu.Unlock()

	c.server()
	return &c
}
