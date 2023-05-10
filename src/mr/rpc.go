package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"encoding/gob"
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

type ApplyTaskArgs struct {
	WorkerName string
}

type MapTask struct {
	MapFileName string // 唯一标识了一个 map 任务
	NReduce     int    // 共有多少个 reduce task，用于哈希
}

type ReduceTask struct {
	ReduceTaskID      int      // 唯一标识了一个 reduce 任务
	IntermediateFiles []string //
}

type NoneTask struct {
}

type ApplyTaskReply struct {
	WorkerApplySeq int // 每个 Worker 有一个任务号
	Task           interface{}
	TaskAllocSeq   int // 这个任务第几次分配
}

func init() {
	gob.Register(MapTask{})
	gob.Register(ReduceTask{})
	gob.Register(NoneTask{})

	// log.SetOutput(ioutil.Discard)	// 为了更好的看测试结果
}

const (
	MAP int = iota
	REDUCE
	NONE
)

type TaskType int

func (t TaskType) String() string {
	var str string
	switch int(t) {
	case MAP:
		str = "MAP"
	case REDUCE:
		str = "REDUCE"
	}

	return str
}

type NotifyTaskCompleteArgs struct {
	Task               interface{}
	TaskAllocSeq       int
	WorkerName         string
	WorkerApplyTaskSeq int
	OutputFiles        []string
}

type NotifyTaskCompleteReply struct {
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
