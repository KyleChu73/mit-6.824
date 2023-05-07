package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// MapTask数据结构会有竞争吗
// 总之现在先实现一个单线程的worker
// TODO：多线程 worker
func doMapTask(mapf func(string, string) []KeyValue, task *MapTask) []KeyValue {
	filename := task.MapFileName
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	defer file.Close()
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}

	intermediate := mapf(filename, string(content))

	return intermediate
}

// 进行哈希、写出临时文件
func writeIntermediateFiles(kva []KeyValue, maptask string, nReduce int) []string {
	// 将所有的 kv 罗列成 nReduce 行
	kvtable := make([][]KeyValue, nReduce)
	for _, kv := range kva {
		key := kv.Key
		reduceIdx := ihash(key) % nReduce
		kvtable[reduceIdx] = append(kvtable[reduceIdx], kv)
	}

	tmpfiles := []string{}
	for reduceIdx, kva := range kvtable {
		f, err := ioutil.TempFile("/home/kyle/6.824/tmp", fmt.Sprintf("mr-%v-%v-*", maptask, reduceIdx))
		if err != nil {
			log.Fatalln("writeIntermediateFiles(): cannot create tmp file")
		}
		defer f.Close()

		// 全部写入文件
		enc := json.NewEncoder(f)
		for _, kv := range kva {
			err := enc.Encode(&kv)
			if err != nil {
				log.Fatalln("writeIntermediateFiles(): Encode(&kv) failed")
			}
		}
		tmpfiles = append(tmpfiles, f.Name())
	}

	return tmpfiles
}

func doReduceTask(reducef func(string, []string) string, task *ReduceTask) string {
	return ""
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	workerName := fmt.Sprintf("worker-%d", os.Getpid())
	latestTaskSeq := 0
loop:
	for {
		// worker 通过 rpc 申请一个任务
		args := ApplyTaskArgs{WorkerName: workerName}
		reply := ApplyTaskReply{}
		ok := call("Coordinator.ApplyTask", &args, &reply)
		if ok {
			if reply.WorkerApplyTaskSeq >= latestTaskSeq {
				// 永远做最新的工作
				latestTaskSeq = reply.WorkerApplyTaskSeq

				switch task := reply.Task.(type) {
				case MapTask:
					fmt.Println("received a map task")
					intermediate := doMapTask(mapf, &task)
					files := writeIntermediateFiles(intermediate, task.MapFileName, task.ReduceTaskNum)
					
					// 将产生的临时文件告知 coordinator
					cmpArgs := NotifyTaskCompleteArgs{
						Task: task,
						WorkerName: workerName, WorkerApplyTaskSeq: reply.WorkerApplyTaskSeq,
						OutputFiles: files}
					cmpReply := NotifyTaskCompleteReply{}
					ok := call("Coordinator.NotifyTaskComplete", &cmpArgs, &cmpReply)
					if !ok {
						fmt.Printf("map task %v completed by %v, but failed to notify coordinator\n", task.MapFileName, workerName)
					} else {
						fmt.Printf("map task %v completed by %v, notified coordinator\n", task.MapFileName, workerName)
					}

				case ReduceTask:
					fmt.Println("received a reduce task")

				case NoneTask:
					fmt.Println("received none task")
					break loop

				default:
					// 不应该有其他类型
					log.Fatalln("Worker(): invalid type!")
				}
			} else {
				// 这个任务过时了
				fmt.Println("received a old task, ignore")
			}

		} else {
			// 也许网络波动，rpc 失败了……
			// TODO：防止无限循环
			fmt.Printf("call failed!\n")
		}

		time.Sleep(time.Second) // 这个循环不要过于频繁
	}

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

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
