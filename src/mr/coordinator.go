package mr

import (
	// "fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	TaskStatus          string
	Reduces             int
	MapId               int
	ReduceId            int
	InFiles             []string
	TmpFiles            []string
	CompletedMapIds     []int
	CompletedReduceIds  []int
	MutexTaskStatus     sync.Mutex
	MutexAssignedMap    sync.Mutex
	MutexAssignedReduce sync.Mutex
	AssignedMapIds      map[int]time.Time
	AssignedReduceIds   map[int]time.Time
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) AskTask(args *AskTaskArgs, reply *AskTaskReply) error {
	reply.Type = "Wait"

	switch c.TaskStatus {
	case "Map":
		c.ReplyMapTask(reply)
	case "Reduce":
		c.ReplyReduceTask(reply)
	case "Completed":
		c.ReplyCompletedTask(reply)
	}

	return nil
}

func (c *Coordinator) ReplyMapTask(reply *AskTaskReply) {
	// Whether there are some tasks out of date
	for id, startTime := range c.AssignedMapIds {
		if startTime.Add(10 * time.Second).Before(time.Now()) {
			reply.MapId = id
			reply.Reduces = c.Reduces
			reply.InFilename = c.InFiles[id]
			reply.Type = "Map"
			c.MutexAssignedMap.Lock()
			c.AssignedMapIds[id] = time.Now()
			c.MutexAssignedMap.Unlock()
			return
		}
	}

	if c.MapId < len(c.InFiles) {
		reply.MapId = c.MapId
		reply.Reduces = c.Reduces
		reply.InFilename = c.InFiles[c.MapId]
		reply.Type = "Map"
		c.MutexAssignedMap.Lock()
		c.AssignedMapIds[c.MapId] = time.Now()
		c.MutexAssignedMap.Unlock()
		c.MapId++
		// fmt.Printf("Reply Type : %v\n", reply.Type)
	} else {
		reply.Type = "Wait"
		// fmt.Printf("Reply Type : %v\n", reply.Type)
	}
}

func (c *Coordinator) ReplyReduceTask(reply *AskTaskReply) {
	for id, startTime := range c.AssignedReduceIds {
		if startTime.Add(10 * time.Second).Before(time.Now()) {
			for _, file := range c.TmpFiles {
				fileSplit := strings.Split(file, "-")
				requiredReduceId, err := strconv.Atoi(fileSplit[len(fileSplit)-1])
				if err != nil {
					// fmt.Println("Convert string to int failed")
					continue
				}
				if requiredReduceId == id {
					reply.TmpFiles = append(reply.TmpFiles, file)
				}
			}
			reply.ReduceId = id
			reply.Type = "Reduce"
			c.MutexAssignedReduce.Lock()
			c.AssignedReduceIds[id] = time.Now()
			c.MutexAssignedReduce.Unlock()
			return
		}
	}

	if c.ReduceId < c.Reduces {
		for _, file := range c.TmpFiles {
			fileSplit := strings.Split(file, "-")
			requiredReduceId, err := strconv.Atoi(fileSplit[len(fileSplit)-1])
			if err != nil {
				// fmt.Println("Convert string to int failed")
				continue
			}
			if requiredReduceId == c.ReduceId {
				reply.TmpFiles = append(reply.TmpFiles, file)
			}
		}

		reply.Type = "Reduce"
		reply.ReduceId = c.ReduceId
		c.MutexAssignedReduce.Lock()
		c.AssignedReduceIds[c.ReduceId] = time.Now()
		c.MutexAssignedReduce.Unlock()
		c.ReduceId++
	} else {
		reply.Type = "Wait"
	}
}

func (c *Coordinator) ReplyCompletedTask(reply *AskTaskReply) {
	reply.Type = "Completed"
}

func (c *Coordinator) FinishMapTask(args *FinishMapTaskArgs, reply *FinishMapTaskReply) error {
	c.TmpFiles = append(c.TmpFiles, args.TmpFiles...)
	delete(c.AssignedMapIds, args.MapId)
	c.CompletedMapIds = append(c.CompletedMapIds, args.MapId)
	// Check whether all map tasks are completed
	if len(c.CompletedMapIds) >= len(c.InFiles) {
		c.MutexTaskStatus.Lock()
		c.TaskStatus = "Reduce"
		c.MutexTaskStatus.Unlock()
	}
	// fmt.Printf("Task Status : %v\n", c.TaskStatus)
	return nil
}

func (c *Coordinator) FinishReduceTask(args *FinishReduceTaskArgs, reply *FinishReduceTaskReply) error {
	delete(c.AssignedReduceIds, args.ReduceId)
	c.CompletedReduceIds = append(c.CompletedReduceIds, args.ReduceId)
	// Check whether all reduce tasks are completed
	if len(c.CompletedReduceIds) >= c.Reduces {
		c.MutexTaskStatus.Lock()
		c.TaskStatus = "Completed"
		c.MutexTaskStatus.Unlock()
	}
	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
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
	ret := false

	// Your code here.
	c.MutexTaskStatus.Lock()
	if c.TaskStatus == "Completed" {
		ret = true
	}
	c.MutexTaskStatus.Unlock()

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.TaskStatus = "Map"
	c.Reduces = nReduce
	c.MapId = 0
	c.ReduceId = 0
	c.InFiles = files
	c.AssignedMapIds = make(map[int]time.Time, 0)
	c.AssignedReduceIds = make(map[int]time.Time, 0)

	c.server()
	return &c
}
