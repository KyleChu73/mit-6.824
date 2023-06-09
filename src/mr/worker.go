package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"path"
	"sort"
	"strconv"
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
		f, err := ioutil.TempFile("", fmt.Sprintf("mr-%v-%v-*", path.Base(maptask), reduceIdx))
		if err != nil {
			log.Fatalln("writeIntermediateFiles(): cannot create tmp file", err)
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

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

func doReduceTask(reducef func(string, []string) string, task *ReduceTask) string {
	intermediate := []KeyValue{}
	files := task.IntermediateFiles
	for _, fName := range files {
		file, err := os.Open(fName)
		if err != nil {
			log.Fatalln("doReduceTask(): cannot open file", err)
		}
		defer file.Close()

		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
	}
	sort.Sort(ByKey(intermediate))
	oname := "mr-out-" + strconv.Itoa(task.ReduceTaskID)
	ofile, err := os.Create(oname)
	if err != nil {
		log.Fatalln("Error: create file", err)
	}
	defer ofile.Close()
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	return ofile.Name()
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
			if reply.WorkerApplySeq >= latestTaskSeq {
				// 永远做最新的工作
				latestTaskSeq = reply.WorkerApplySeq

				switch task := reply.Task.(type) {
				case MapTask:
					log.Printf("received map-%v\n", path.Base(task.MapFileName))
					// log.Println("debug sleeping")
					// time.Sleep(2 * time.Second)
					intermediate := doMapTask(mapf, &task)
					files := writeIntermediateFiles(intermediate, task.MapFileName, task.NReduce)
					log.Printf("map-%v done, notifying coordinator", path.Base(task.MapFileName))
					// 将产生的临时文件告知 coordinator
					ok := CallNotifyTaskComplete(task, reply.TaskAllocSeq, workerName, reply.WorkerApplySeq, files)
					if !ok {
						log.Printf("map task %v completed by %v, but failed to notify coordinator\n", path.Base(task.MapFileName), workerName)
					} else {
						log.Printf("map task %v completed by %v, notified coordinator\n", path.Base(task.MapFileName), workerName)
					}

				case ReduceTask:
					log.Println("received a reduce task")
					// log.Println("debug sleeping")
					// time.Sleep(2 * time.Second)
					file := doReduceTask(reducef, &task)
					log.Printf("reduce-%v done, notifying coordinator", task.ReduceTaskID)
					ok := CallNotifyTaskComplete(task, reply.TaskAllocSeq, workerName, reply.WorkerApplySeq, []string{file})
					if !ok {
						log.Printf("reduce task %v completed by %v, but failed to notify coordinator\n", task.ReduceTaskID, workerName)
					} else {
						log.Printf("reduce task %v completed by %v, notified coordinator\n", task.ReduceTaskID, workerName)
					}

				case NoneTask:
					log.Println("received none task")
					ok := CallNotifyTaskComplete(task, reply.TaskAllocSeq, workerName, reply.WorkerApplySeq, []string{})
					if !ok {
						log.Printf("%v received none task, but failed to notify coordinator, exiting...\n", workerName)
					} else {
						log.Printf("%v received none task, notified coordinator, exiting...\n", workerName)
					}
					break loop

				default:
					// 不应该有其他类型
					log.Fatalln("Worker(): invalid type!")
				}
			} else {
				// 这个任务过时了
				log.Println("received a old task, ignore")
			}

		} else {
			// 也许网络波动，rpc 失败了……
			log.Printf("call failed!\n")
		}

		time.Sleep(time.Second / 10) // 这个循环不要过于频繁
	}

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

func CallNotifyTaskComplete(task interface{}, taskAllocSeq int, workerName string, workerApplyTaskSeq int, files []string) bool {
	cmpArgs := NotifyTaskCompleteArgs{
		Task:               task,
		WorkerName:         workerName,
		WorkerApplyTaskSeq: workerApplyTaskSeq,
		OutputFiles:        files,
		TaskAllocSeq:       taskAllocSeq,
	}
	cmpReply := NotifyTaskCompleteReply{}
	ok := call("Coordinator.NotifyTaskComplete", &cmpArgs, &cmpReply)
	return ok
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
