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

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

	for {
		// Ask Task
		TaskReply := AskTaskReply{}
		AskTask(AskTaskArgs{}, &TaskReply)

		// Do Map or Reduce Task according to Task Type
		switch TaskReply.Type {
		case "Map":
			DoMapTask(&TaskReply, mapf)
		case "Reduce":
			DoReduceTask(&TaskReply, reducef)
		case "Wait":
			time.Sleep(1 * time.Second)
			continue
		case "Completed":
			return
		default:
			return
		}
	}
}

// Ask for a task
func AskTask(args AskTaskArgs, reply *AskTaskReply) {
	ok := call("Coordinator.AskTask", args, reply)
	if ok {
		// fmt.Println("AskTask RPC call succeeded")
	} else {
		// fmt.Println("AskTask RPC call failed")
	}
}

// Map Function
func DoMapTask(reply *AskTaskReply, mapf func(string, string) []KeyValue) {
	// read the file
	file, err := os.Open(reply.InFilename)
	if err != nil {
		log.Fatalf("cannot open %v", reply.InFilename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", reply.InFilename)
	}
	file.Close()
	// call the Map function
	kvs := mapf(reply.InFilename, string(content))
	// store kv pairs in intermediate files
	tmpFiles := make([]*os.File, 0)
	tmpEncoders := make([]*json.Encoder, 0)
	for i := 0; i < reply.Reduces; i++ {
		// create the temp file
		tmpFile, err := os.CreateTemp(".", "mr-tmp")
		if err != nil {
			// fmt.Println("Create temp file failed")
		} else {
			tmpFiles = append(tmpFiles, tmpFile)
		}
		// create the file encoder
		tmpEncoder := json.NewEncoder(tmpFile)
		tmpEncoders = append(tmpEncoders, tmpEncoder)
	}
	for _, kv := range kvs {
		err := tmpEncoders[ihash(kv.Key)%reply.Reduces].Encode(kv)
		if err != nil {
			// fmt.Printf("Encode %v %v into %vth temp file failed\n", kv.Key, kv.Value, ihash(kv.Key)%reply.Reduces)
		}
	}
	// Rename
	for i := 0; i < reply.Reduces; i++ {
		os.Rename(tmpFiles[i].Name(), fmt.Sprintf("mr-%v-%v", reply.MapId, i))
	}
	// Send RPC to notify the map task is completed
	finishTaskArgs := FinishMapTaskArgs{}
	finishTaskReply := FinishMapTaskReply{}
	finishTaskArgs.MapId = reply.MapId
	for i := 0; i < reply.Reduces; i++ {
		finishTaskArgs.TmpFiles = append(finishTaskArgs.TmpFiles, fmt.Sprintf("mr-%v-%v", reply.MapId, i))
	}
	FinishMapTask(&finishTaskArgs, &finishTaskReply)
}

func FinishMapTask(args *FinishMapTaskArgs, reply *FinishMapTaskReply) {
	ok := call("Coordinator.FinishMapTask", args, reply)
	if ok {
		// fmt.Println("FinishMapTask RPC call succeeded")
	} else {
		// fmt.Println("FinishMapTask RPC call failed")
	}
}

// Reduce function
func DoReduceTask(reply *AskTaskReply, reducef func(string, []string) string) {
	kva := make([]KeyValue, 0)
	for _, tmpFile := range reply.TmpFiles {
		file, err := os.Open(tmpFile)
		if err != nil {
			log.Fatalf("cannot open %v", tmpFile)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
	}
	// Sort
	sort.Sort(ByKey(kva))
	// Store
	outFilename := fmt.Sprintf("mr-out-%v", reply.ReduceId)
	outfile, _ := os.Create(outFilename)

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
		fmt.Fprintf(outfile, "%v %v\n", kva[i].Key, output)

		i = j
	}
	outfile.Close()
	// Send RPC to notify the reduce task is completed
	finishTaskArgs := FinishReduceTaskArgs{}
	finishTaskReply := FinishReduceTaskReply{}
	finishTaskArgs.ReduceId = reply.ReduceId
	FinishReduceTask(&finishTaskArgs, &finishTaskReply)
}

func FinishReduceTask(args *FinishReduceTaskArgs, reply *FinishReduceTaskReply) {
	ok := call("Coordinator.FinishReduceTask", args, reply)
	if ok {
		// fmt.Println("FinishReduceTask RPC call succeeded")
	} else {
		// fmt.Println("FinishReduceTask RPC call failed")
	}
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
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
