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
	"strconv"
	"strings"
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
	waiterChan chan int
}

func NewTaskStatus() *TaskStatus {
	return &TaskStatus{Status: StatusType(IDLE), LastestSeq: -1, waiterChan: make(chan int)}
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

	MapGroup sync.WaitGroup

	ReduceGroup sync.WaitGroup

	taskQue *TaskQueue

	pendingReduceFiles map[int][]string // reduce 任务的输入

	quit chan int
}

func timeUpNotifier(quit chan int) chan int {
	ch := make(chan int)
	// 返回一个channel，他会在10秒之后被写入
	go func() {
		time.Sleep(10 * time.Second)
		select {
		case <-quit:
			return
		default:
		}
		ch <- 1
	}()
	return ch
}

// helper，无锁
func resetTaskStatus(taskStatus *TaskStatus) int {
	taskStatus.LastestSeq++
	taskStatus.Status = StatusType(IN_PROGRESS)
	return taskStatus.LastestSeq
}

// Your code here -- RPC handlers for the worker to call.
// 客户端通过 rpc 调用这个方法，这个方法在服务端执行
func (c *Coordinator) ApplyTask(args *ApplyTaskArgs, reply *ApplyTaskReply) error {
	task := c.taskQue.Take()

	c.mu.Lock()
	defer c.mu.Unlock()

	reply.WorkerApplySeq = c.WorkerTaskApplySeqs[args.WorkerName]
	c.WorkerTaskApplySeqs[args.WorkerName]++

	c.TotalTaskAllocated++

	timerQuit := make(chan int)

	switch task := task.(type) {
	case MapTask:
		taskStatus := c.MapTaskStatus[task.MapFileName]
		reply.TaskAllocSeq = resetTaskStatus(taskStatus)
		go c.taskWaiter(task, taskStatus.waiterChan, timeUpNotifier(timerQuit), timerQuit)
		log.Printf("%v applied: map-%v TaskAllocSeq:%v WorkerApplySeq:%v\n", args.WorkerName, path.Base(task.MapFileName), reply.TaskAllocSeq, reply.WorkerApplySeq)

	case ReduceTask:
		taskStatus := c.ReduceTaskStatus[task.ReduceTaskID]
		reply.TaskAllocSeq = resetTaskStatus(taskStatus)
		go c.taskWaiter(task, taskStatus.waiterChan, timeUpNotifier(timerQuit), timerQuit)
		log.Printf("%v applied: reduce-%v TaskAllocSeq:%v WorkerApplySeq:%v\n", args.WorkerName, task.ReduceTaskID, reply.TaskAllocSeq, reply.WorkerApplySeq)

	case NoneTask:
		reply.TaskAllocSeq = c.TotalTaskAllocated
		log.Printf("%v applied: none TaskAllocSeq:%v WorkerApplySeq:%v\n", args.WorkerName, reply.TaskAllocSeq, reply.WorkerApplySeq)
	}

	reply.Task = task

	return nil
}

// 分配一个任务之后应该等待回复，十秒没收到回复则重新把这个任务加入队列
func (c *Coordinator) taskWaiter(task interface{}, waiterChan chan int, timeUp chan int, quitTimer chan int) {
	select {
	case <-waiterChan:
		switch task := task.(type) {
		case MapTask: // 完成了一个 map 任务
			log.Printf("map-%v taskwaiter exiting...\n", path.Base(task.MapFileName))

		case ReduceTask:
			log.Printf("reduce-%v taskwaiter exiting...\n", task.ReduceTaskID)

		default:
			log.Fatalln("taskWaiter(): invalid type!")
		}
		quitTimer <- 1

	case <-timeUp:
		c.reputTask(task)
	}

}

func (c *Coordinator) reputTask(task interface{}) {
	c.taskQue.Add(task)
	c.mu.Lock()
	defer c.mu.Unlock()
	switch task := task.(type) {
	case MapTask:
		c.MapTaskStatus[task.MapFileName].LastestSeq++
		c.MapTaskStatus[task.MapFileName].Status = StatusType(IDLE)
		log.Printf("****warning: reput map-%v\n", path.Base(task.MapFileName))
	}
}

func (c *Coordinator) fmtStatus(which int) []string {
	allStatus := []string{}
	switch which {
	case MAP:
		for k, v := range c.MapTaskStatus {
			allStatus = append(allStatus, fmt.Sprintf("%v:%v, ", path.Base(k), v.Status))
		}
	case REDUCE:
		for k, v := range c.ReduceTaskStatus {
			allStatus = append(allStatus, fmt.Sprintf("%v:%v, ", k, v.Status))
		}
	}
	sort.StringSlice(allStatus).Sort()
	return allStatus
}

func removeFiles(files []string) {
	for _, file := range files {
		err := os.Remove(file)
		if err != nil {
			log.Fatalln(err)
		}
	}
}

// 输入是一个map任务产生的临时中间文件
// 将临时文件名字末尾的随机数去除，并记录下新的文件名
func (c *Coordinator) finalizeMap(mapOutFiles []string) {
	for _, oldName := range mapOutFiles {
		idx := strings.LastIndex(oldName, "-")
		if idx != -1 {
			newName := oldName[:idx] // 去除末尾的随机数
			err := os.Rename(oldName, newName)
			if err != nil {
				log.Fatalln("Error renaming file:", err)
			}
			parts := strings.Split(newName, "-")
			if len(parts) >= 1 {
				numStr := parts[len(parts)-1]
				num, err := strconv.Atoi(numStr)
				if err != nil {
					log.Fatalln("Error Atoi:", err)
				}
				c.pendingReduceFiles[num] = append(c.pendingReduceFiles[num], newName)
			}
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
	info := fmt.Sprintf("task alloc seq: %v\n\t\t\t worker: %v\n\t\t\t worker apply task sequence: %v\n\t\t\t output files: %v",
		args.TaskAllocSeq, args.WorkerName, args.WorkerApplyTaskSeq, fileInfo)

	c.mu.Lock()
	defer c.mu.Unlock()

	var ch chan int
	switch task := args.Task.(type) {
	case MapTask:
		mapName := task.MapFileName
		ch = c.MapTaskStatus[mapName].waiterChan
		latestSeq := c.MapTaskStatus[task.MapFileName].LastestSeq
		// 只要TaskAllocSeq等于LastestSeq，那么taskWaiter一定活着
		// 如果taskWaiter已经重放任务并退出，那么它一定会更新LastestSeq（旧的TaskAllocSeq也就无法与之一致）
		if args.TaskAllocSeq == latestSeq {
			ch <- 1 // 唤醒 task 对应的 taskWaiter
			c.finalizeMap(args.OutputFiles)
			c.MapTaskStatus[task.MapFileName].Status = StatusType(COMPLETE)
			c.MapGroup.Done()
			// log.Printf("map task left: %v\n")
			allStatus := c.fmtStatus(MAP)
			log.Printf("map-%v complete! \n\t\t\t task info: \n\t\t\t %v\n\t\t\t %v\n", path.Base(task.MapFileName), info, allStatus)
		} else {
			log.Printf("map-%v complete, but out of date, latest seq: %v \n\t\t\t task info: \n\t\t\t %v", path.Base(task.MapFileName), latestSeq, info)
			removeFiles(args.OutputFiles)
		}

	case ReduceTask:
		reduceId := task.ReduceTaskID
		ch = c.ReduceTaskStatus[reduceId].waiterChan
		latestSeq := c.ReduceTaskStatus[task.ReduceTaskID].LastestSeq
		if args.TaskAllocSeq == latestSeq {
			ch <- 1 // 唤醒 task 对应的 taskWaiter
			c.ReduceTaskStatus[task.ReduceTaskID].Status = StatusType(COMPLETE)
			c.ReduceGroup.Done()
			// log.Printf("reduce task left: %v\n", c.ReduceLeft)
			allStatus := c.fmtStatus(REDUCE)
			log.Printf("reduce-%v complete! \n\t\t\t task info: \n\t\t\t %v\n\t\t\t %v\n", task.ReduceTaskID, info, allStatus)
		} else {
			log.Printf("reduce-%v complete, but out of date, latest seq: %v \n\t\t\t task info: \n\t\t\t %v", task.ReduceTaskID, latestSeq, info)
			removeFiles(args.OutputFiles)
		}

	case NoneTask:
		log.Printf("worker replied none task exit... \n\t\t\t info: \n\t\t\t %v", info)
	}

	return nil
}

func (c *Coordinator) copyReduceFiles() map[int][]string {
	c.mu.Lock()
	defer c.mu.Unlock()
	reduceFiles := map[int][]string{}
	for k, v := range c.pendingReduceFiles {
		reduceFiles[k] = make([]string, len(v))
		copy(reduceFiles[k], v)
	}
	return reduceFiles
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
	reduceFiles := c.copyReduceFiles()
	log.Println("removing intermediate files...")
	for _, files := range reduceFiles {
		removeFiles(files)
	}
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
	c.InputFiles = append(c.InputFiles, files...)

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

	c.taskQue = NewTaskQueue()
	for _, file := range files {
		c.taskQue.Add(MapTask{file, nReduce})
	}

	c.pendingReduceFiles = make(map[int][]string)
	for i := 0; i < nReduce; i++ {
		c.pendingReduceFiles[i] = make([]string, 0)
	}

	c.quit = make(chan int)

	c.MapGroup.Add(len(c.InputFiles))
	// 等待所有 map 完成，然后放入 reduce 任务
	go func(c *Coordinator) {
		c.MapGroup.Wait()

		reduceFiles := c.copyReduceFiles()

		log.Println("all map task done, putting reduce tasks")
		for i := 0; i < nReduce; i++ {
			c.taskQue.Add(ReduceTask{i, reduceFiles[i]})
		}
	}(&c)

	c.ReduceGroup.Add(nReduce)

	go func(c *Coordinator) {
		c.ReduceGroup.Wait()
		log.Println("all task done")
		for i := 0; i < nReduce; i++ {
			c.taskQue.Add(NoneTask{})
		}
		log.Printf("Coordinator will exit in 10 seconds...\n\n")
		c.quit <- 1
	}(&c)

	c.server()
	return &c
}
