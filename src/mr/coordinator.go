package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

const (
	IDLE int = iota
	IN_PROGRESS
	COMPLETE
)

type Coordinator struct {
	// Your definitions here.
	mu                  sync.Mutex
	TotalTaskAllocated  int
	InputFiles          []string
	ReduceTaskNum       int
	WorkerTaskApplySeqs map[string]int
	MapTaskStatus       map[string]int
}

// Your code here -- RPC handlers for the worker to call.
// 客户端通过 rpc 调用这个方法，这个方法在服务端执行
func (c *Coordinator) ApplyTask(args *ApplyTaskArgs, reply *ApplyTaskReply) error {
	// switch c.TotalTaskAllocated % 3 {
	// case 0:
	// 	reply.Task = MapTask{}
	// case 1:
	// 	reply.Task = ReduceTask{}
	// case 2:
	// 	reply.Task = NoneTask{}
	// }

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.TotalTaskAllocated%2 == 0 {
		reply.Task = MapTask{MapFileName: c.InputFiles[0], ReduceTaskNum: c.ReduceTaskNum}
	} else {
		reply.Task = MapTask{MapFileName: c.InputFiles[1], ReduceTaskNum: c.ReduceTaskNum}
	}

	reply.WorkerApplyTaskSeq = c.WorkerTaskApplySeqs[args.WorkerName]
	c.WorkerTaskApplySeqs[args.WorkerName]++

	c.TotalTaskAllocated++

	return nil
}

func (c *Coordinator) NotifyTaskComplete(args *NotifyTaskCompleteArgs, reply *NotifyTaskCompleteReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	info := fmt.Sprintf("worker: %v\n\t worker apply task sequence: %v\n\t output files: %v\n\n",
		args.WorkerName, args.WorkerApplyTaskSeq, args.OutputFiles)

	switch task := args.Task.(type) {
	case MapTask:
		fmt.Printf("map-%v complete!\n\t %v", task.MapFileName, info)
	case ReduceTask:
		fmt.Printf("reduce-%v complete!\n\t %v", task.ReduceTaskIdx, info)
	case NoneTask:
		fmt.Println("worker complete none task (then it will exit...)")
	default:
		log.Fatalln("NotifyTaskComplete(): invalid type!")
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

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.

	c.TotalTaskAllocated = 0

	c.InputFiles = make([]string, len(files))
	copy(c.InputFiles, files)

	c.ReduceTaskNum = nReduce

	c.WorkerTaskApplySeqs = make(map[string]int)

	c.MapTaskStatus = make(map[string]int)
	for _, f := range files {
		c.MapTaskStatus[f] = IDLE
	}

	c.server()
	return &c
}
