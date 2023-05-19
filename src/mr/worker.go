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
	"sync"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func HandleReduceTask(files []string, id int, reducef func(string, []string) string) (bool, string) {
	var kva []KeyValue = make([]KeyValue, 0)

	//read intermediate values into list
	for _, file := range files {
		ifile, err := os.Open(file)

		if err != nil {
			fmt.Printf("Error opening intermediate file %s: %s\n", file, err)
			return false, ""
		}

		dec := json.NewDecoder(ifile)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}

		ifile.Close()
	}

	sort.Sort(ByKey(kva))
	// fmt.Printf("%v\n", kva)

	//create and close temp file
	ofile, err := ioutil.TempFile("./", "mr-out-temp-")

	if err != nil {
		fmt.Printf("Error creating output file %s\n: %s", ofile.Name(), err)
		return false, ""
	}

	ofile.Close()

	//repoen temp file with write access
	ofile, err = os.OpenFile(ofile.Name(), os.O_APPEND|os.O_CREATE|os.O_RDWR, os.ModePerm)

	if err != nil {
		fmt.Printf("Error opening or creating file %s: %s\n", ofile.Name(), err)
		return false, ""
	}

	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)
		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

		i = j
	}

	ofile.Close()
	name := ofile.Name()
	return true, name
}

func HandleMapTask(filename string, id int, mapf func(string, string) []KeyValue, nReduce int) (map[int][]string, map[string]string) {
	var mu sync.Mutex
	mu.Lock()
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
		return nil, nil
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
		return nil, nil
	}
	file.Close()
	mu.Unlock()
	kva := mapf(filename, string(content))

	sort.Sort(ByKey(kva))

	i := 0
	var used map[int]bool = make(map[int]bool)
	var tempFiles map[string]string = make(map[string]string)
	var intermediate map[int][]string = make(map[int][]string)

	for i < len(kva) {
		keyhash := ihash(kva[i].Key) % nReduce
		oname := "map-" + fmt.Sprint(id) + "-" + fmt.Sprint(keyhash) + ".out"
		if !used[keyhash] {
			used[keyhash] = true
			intermediate[keyhash] = append(intermediate[keyhash], oname)
			file, err := ioutil.TempFile("./", "map-temp-")
			if err != nil {
				fmt.Printf("Error creating temp file\n")
				return nil, nil
			}
			tempFiles[oname] = file.Name()
			file.Close()
		}

		mu.Lock()

		ofile, err := os.OpenFile(tempFiles[oname], os.O_APPEND|os.O_CREATE|os.O_RDWR, os.ModePerm)

		if err != nil {
			fmt.Printf("Error opening or creating file %s: %s\n", oname, err)
			return nil, nil
		}

		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}

		enc := json.NewEncoder(ofile)

		for k := i; k < j; k++ {
			err := enc.Encode(kva[k])
			if err != nil {
				fmt.Printf("Error encoding to intermediate file\n")
				return nil, nil
			}
		}

		i = j
		ofile.Close()
		mu.Unlock()
	}

	return intermediate, tempFiles
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	for {
		reply := CallTaskRequest()
		if reply == nil {
			break
		}

		if reply.Map != nil {
			mapRes, tempFiles := HandleMapTask(reply.Map.Filename, reply.Map.TaskId, mapf, reply.Map.NReduce)
			args := MapTaskFinishedArgs{}
			args.TaskId = reply.Map.TaskId
			if mapRes == nil {
				args.Success = false
			} else {
				args.Success = true
				args.Ofiles = mapRes
				args.TempFileMap = tempFiles
			}

			ok := CallMapTaskFinished(args)
			if !ok {
				break
			}

			continue
		}

		if reply.Reduce != nil {
			args := ReduceTaskFinishedArgs{}
			ok, name := HandleReduceTask(reply.Reduce.Intermediate, reply.Reduce.TaskId, reducef)

			args.Success = ok
			args.TaskId = reply.Reduce.TaskId
			args.TempFilename = name

			resultOk := CallReduceTaskFinished(args)
			if !resultOk {
				break
			}
		}

		time.Sleep(time.Millisecond * 10)
	}

}

func CallTaskRequest() *TaskRequestReply {

	// declare an argument structure.
	args := TaskRequestArgs{}

	// declare a reply structure.
	reply := TaskRequestReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.TaskRequestHandler", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("call success\n")
		return &reply
	} else {
		fmt.Printf("call failed!\n")
		return nil
	}
}

func CallMapTaskFinished(args MapTaskFinishedArgs) bool {
	reply := MapTaskFinishedReply{}

	ok := call("Coordinator.MapTaskFinishedHandler", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("map task finished call success\n")
		return true
	} else {
		fmt.Printf("map task finished call failed!\n")
		return false
	}
}

func CallReduceTaskFinished(args ReduceTaskFinishedArgs) bool {
	reply := ReduceTaskFinishedReply{}

	ok := call("Coordinator.ReduceTaskFinishedHandler", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reduce task finished call success\n")
		return true
	} else {
		fmt.Printf("reduce task finished call failed!\n")
		return false
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
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
