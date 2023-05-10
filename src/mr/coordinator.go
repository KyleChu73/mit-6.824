package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"path"
	"sort"
	"sync"
	"time"
)

const (
	IDLE int = iota
	IN_PROGRESS
	COMPLETE
)

type StatusType int

func (st StatusType) String() string {
	ret := ""
	switch int(st) {
	case IDLE:
		ret = "IDLE"
	case IN_PROGRESS:
		ret = "IN_PROGRESS"
	case COMPLETE:
		ret = "COMPLETE"
	}
	return ret
}

type TaskStatus struct {
	Status     StatusType
	LastestSeq int // 最新发出的任务的序号，-1 标识还没派发过这个任务
	waiterChan chan *NotifyTaskCompleteArgs
}

func NewTaskStatus() *TaskStatus {
	return &TaskStatus{Status: StatusType(IDLE), LastestSeq: -1, waiterChan: make(chan *NotifyTaskCompleteArgs)}
}

// 不用 channel 是因为我需要一个无界的东西
type TaskQueue struct {
	mu   *sync.Mutex
	cond *sync.Cond

	queue []interface{}
}

func (q *TaskQueue) Add(task interface{}) {
	q.mu.Lock()
	defer q.mu.Unlock()

	q.queue = append(q.queue, task)
	q.cond.Signal()
}

func (q *TaskQueue) Take() interface{} {
	q.mu.Lock()
	defer q.mu.Unlock()

	for len(q.queue) == 0 {
		q.cond.Wait()
	}

	tsk := q.queue[0]
	q.queue = q.queue[1:]

	return tsk
}

func NewTaskQueue() *TaskQueue {
	mu := &sync.Mutex{}
	return &TaskQueue{mu, sync.NewCond(mu), make([]interface{}, 0)}
}

type Coordinator struct {
	// Your definitions here.
	mu                  *sync.Mutex
	TotalTaskAllocated  int
	InputFiles          []string
	NumReduce           int
	WorkerTaskApplySeqs map[string]int
	MapTaskStatus       map[string]*TaskStatus
	ReduceTaskStatus    map[int]*TaskStatus

	MapLeft    int // 剩余 map 任务数量
	ReduceLeft int // 剩余 reduce 任务数量

	MapLatch    *sync.Cond // 有个go程在等待这个条件变量，然后放入 reduce 任务
	ReduceLatch *sync.Cond // 有个go程在等待这个条件变量，然后终止 coordinator

	taskQue *TaskQueue

	quit chan int
}

func timeUpNotifier() chan int {
	ch := make(chan int)
	// 返回一个channel，他会在10秒之后被写入
	go func() {
		time.Sleep(10 * time.Second)
		ch <- 1
	}()
	return ch
}

// Your code here -- RPC handlers for the worker to call.
// 客户端通过 rpc 调用这个方法，这个方法在服务端执行
func (c *Coordinator) ApplyTask(args *ApplyTaskArgs, reply *ApplyTaskReply) error {
	task := c.taskQue.Take()

	c.mu.Lock()
	defer c.mu.Unlock()

	switch task := task.(type) {
	case MapTask:
		taskStatus := c.MapTaskStatus[task.MapFileName]
		taskStatus.LastestSeq++
		taskStatus.Status = StatusType(IN_PROGRESS)
		reply.TaskAllocSeq = taskStatus.LastestSeq
		go c.taskWaiter(task, taskStatus.waiterChan, timeUpNotifier())
		fmt.Printf("%v applied: map-%v TaskAllocSeq:%v WorkerApplyTaskSeq:%v\n", args.WorkerName, task.MapFileName, reply.TaskAllocSeq, reply.WorkerApplyTaskSeq)

	case ReduceTask:
		taskStatus := c.ReduceTaskStatus[task.ReduceTaskID]
		taskStatus.LastestSeq++
		taskStatus.Status = StatusType(IN_PROGRESS)
		reply.TaskAllocSeq = taskStatus.LastestSeq
		go c.taskWaiter(task, taskStatus.waiterChan, timeUpNotifier())
	}
	reply.Task = task

	reply.WorkerApplyTaskSeq = c.WorkerTaskApplySeqs[args.WorkerName]
	c.WorkerTaskApplySeqs[args.WorkerName]++

	c.TotalTaskAllocated++

	return nil
}

// 分配一个任务之后应该等待回复，十秒没收到回复则重新把这个任务加入队列
// TODO 任务完成的时候，好像只要让它退出就行。。
// 其他的放到 NotifyTaskComplete 里
func (c *Coordinator) taskWaiter(task interface{}, waiterChan chan *NotifyTaskCompleteArgs, timeUp chan int) {
	var args *NotifyTaskCompleteArgs

	select {
	case args = <-waiterChan:
		switch task := args.Task.(type) {
		case MapTask: // 完成了一个 map 任务
			fmt.Printf("map-%v taskwaiter exiting...\n", task.MapFileName)

		case ReduceTask:
			// fmt.Printf("reduce-%v complete!\n\t %v", task.ReduceTaskIdx, info)

		case NoneTask:
			fmt.Println("worker complete none task (then it will exit...)")

		default:
			log.Fatalln("NotifyTaskComplete(): invalid type!")
		}

	case <-timeUp:
		c.taskQue.Add(task)
		c.mu.Lock()
		defer c.mu.Unlock()
		switch task := task.(type) {
		case MapTask:
			c.MapTaskStatus[task.MapFileName].LastestSeq++
			c.MapTaskStatus[task.MapFileName].Status = StatusType(IDLE)
			fmt.Printf("****warning: reput map-%v\n", task.MapFileName)
		}
	}

}

// 一个 worker 完成任务后通知 coordinator
func (c *Coordinator) NotifyTaskComplete(args *NotifyTaskCompleteArgs, reply *NotifyTaskCompleteReply) error {
	// 有一个任务完成了
	files := args.OutputFiles
	fileInfo := []string{}
	for _, file := range files {
		fileInfo = append(fileInfo, path.Base(file))
	}
	info := fmt.Sprintf("task alloc seq: %v\n\t worker: %v\n\t worker apply task sequence: %v\n\t output files: %v\n",
		args.TaskAllocSeq, args.WorkerName, args.WorkerApplyTaskSeq, fileInfo)

	c.mu.Lock()
	defer c.mu.Unlock()

	var ch chan *NotifyTaskCompleteArgs
	switch task := args.Task.(type) {
	case MapTask:
		mapName := task.MapFileName
		ch = c.MapTaskStatus[mapName].waiterChan
		latestSeq := c.MapTaskStatus[task.MapFileName].LastestSeq
		// 只要TaskAllocSeq等于LastestSeq，那么taskWaiter一定活着
		// 如果taskWaiter已经重放任务并退出，那么它一定会更新LastestSeq（旧的TaskAllocSeq也就无法与之一致）
		if args.TaskAllocSeq == latestSeq {
			fmt.Printf("map-%v complete! \n\t task info: \n\t %v", task.MapFileName, info)
			ch <- args // 唤醒 task 对应的 taskWaiter
			c.MapTaskStatus[task.MapFileName].Status = StatusType(COMPLETE)
			c.MapLeft--
			if c.MapLeft <= 0 {
				c.MapLatch.Broadcast()
			}

			fmt.Printf("map task left: %v\n", c.MapLeft)
			allStatus := []string{}
			for k, v := range c.MapTaskStatus {
				allStatus = append(allStatus, fmt.Sprintf("%v:%v, ", k, v.Status))
			}
			sort.StringSlice(allStatus).Sort()
			fmt.Printf("\t%v\n\n", allStatus)
		} else {
			fmt.Printf("map-%v complete, but out of date, latest seq: %v \n\t task info: \n\t %v", task.MapFileName, latestSeq, info)
		}

	case ReduceTask:
		// reduceId := task.ReduceTaskIdx
		// ch = c.ReduceTaskStatus[reduceId].waiterChan
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
	<-c.quit
	return true

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.mu = &sync.Mutex{}

	c.TotalTaskAllocated = 0

	c.InputFiles = make([]string, 0)
	for _, file := range files {
		c.InputFiles = append(c.InputFiles, path.Base(file))
	}

	c.NumReduce = nReduce

	c.WorkerTaskApplySeqs = make(map[string]int)

	c.MapTaskStatus = make(map[string]*TaskStatus)
	for _, f := range c.InputFiles {
		c.MapTaskStatus[f] = NewTaskStatus()
	}

	c.ReduceTaskStatus = make(map[int]*TaskStatus)
	for i := 0; i < nReduce; i++ {
		c.ReduceTaskStatus[i] = NewTaskStatus()
	}

	c.MapLeft = len(files)
	c.ReduceLeft = nReduce

	c.MapLatch = sync.NewCond(c.mu)
	c.ReduceLatch = sync.NewCond(c.mu)

	c.taskQue = NewTaskQueue()
	for _, file := range c.InputFiles {
		c.taskQue.Add(MapTask{file, nReduce})
	}

	c.quit = make(chan int)

	// 等待所有 map 完成，然后放入 reduce 任务
	go func(c *Coordinator) {
		c.mu.Lock()
		for c.MapLeft > 0 {
			c.MapLatch.Wait()
		}
		c.mu.Unlock()
		fmt.Println("all map task done")
		// TODO DEBUG
		fmt.Println("all task done")
		for i := 0; i < nReduce; i++ {
			c.taskQue.Add(ReduceTask{i})
		}
	}(&c)

	go func(c *Coordinator) {
		c.mu.Lock()
		defer c.mu.Unlock()
		for c.ReduceLeft > 0 {
			c.ReduceLatch.Wait()
		}
		fmt.Println("all task done")
		os.Exit(0)
	}(&c)

	c.server()
	return &c
}
