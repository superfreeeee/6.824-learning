package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"strconv"
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
	for {
		taskInfo := CallAskTask()
		if taskInfo == nil {
			// request fail => Exit
			return
		}
		switch taskInfo.State {
		case TaskMap:
			workerMap(mapf, *taskInfo)
			CallTaskDone(taskInfo)

		case TaskReduce:
			workerReduce(reducef, *taskInfo)
			CallTaskDone(taskInfo)

		case TaskWait:
			// sleep 5s
			time.Sleep(time.Duration(time.Second * 5))

		case TaskDone:
			// Worker Exit
			return

		default:
			panic("Invalid Task.State")
		}

		// time.Sleep(time.Duration(time.Millisecond * 100))
	}

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
}

func workerMap(mapf func(string, string) []KeyValue, taskInfo TaskInfo) {
	// fmt.Printf("Do TaskMap(%v_%v)\n", taskInfo.FileName, taskInfo.FileIndex)

	// map: content => KeyValue
	intermediate := []KeyValue{}
	file, err := os.Open(taskInfo.FileName)
	if err != nil {
		log.Fatalf("workerMap: cannot open %v", taskInfo.FileName)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("workerMap: cannot read %v", taskInfo.FileName)
	}
	file.Close()
	kva := mapf(taskInfo.FileName, string(content))
	intermediate = append(intermediate, kva...)

	// prepare map result files
	nReduce := taskInfo.NReduce
	outFiles := make([]*os.File, nReduce)
	fileEncs := make([]*json.Encoder, nReduce)
	for outIndex := 0; outIndex < nReduce; outIndex += 1 {
		outFiles[outIndex], err = ioutil.TempFile(".", "mr-tmp-*")
		if err != nil {
			log.Fatalf("workerMap: Create outFile err: %v", err)
		}
		fileEncs[outIndex] = json.NewEncoder(outFiles[outIndex])
	}

	// divide into reduce parts files
	for _, kv := range intermediate {
		outIndex := ihash(kv.Key) % nReduce
		enc := fileEncs[outIndex]
		err := enc.Encode(&kv)
		if err != nil {
			fmt.Printf("File %v Key %v Value %v Error: %v\n", taskInfo.FileName, kv.Key, kv.Value, err)
			panic("Json parse failed")
		}
	}

	// close files
	for outIndex, file := range outFiles {
		outName := "mr-" + strconv.Itoa(taskInfo.FileIndex) + "-" + strconv.Itoa(outIndex)
		oldPath := filepath.Join(file.Name())
		os.Rename(oldPath, outName)
		file.Close()
	}
}

func workerReduce(reducef func(string, []string) string, taskInfo TaskInfo) {
	// fmt.Printf("Do TaskReduce(%v)\n", taskInfo.PartIndex)

	intermediate := []KeyValue{}
	for fileIndex := 0; fileIndex < taskInfo.NFiles; fileIndex += 1 {
		fileName := "mr-" + strconv.Itoa(fileIndex) + "-" + strconv.Itoa(taskInfo.PartIndex)
		file, err := os.Open(fileName)
		if err != nil {
			log.Fatalf("workerReduce: cannot open %v, %v", fileName, err)
		}
		dec := json.NewDecoder(file)

		for {
			var kv KeyValue
			err := dec.Decode(&kv)
			if err != nil {
				break
			}

			intermediate = append(intermediate, kv)
		}

		file.Close()
	}

	sort.Sort(ByKey(intermediate))

	// tmp file
	oFile, err := ioutil.TempFile(".", "mr-*")
	if err != nil {
		panic("workerReduce: Create file error")
	}

	// reduce
	for i := 0; i < len(intermediate); {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j += 1
		}
		values := []string{}
		for k := i; k < j; k += 1 {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)
		fmt.Fprintf(oFile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}

	os.Rename(filepath.Join(oFile.Name()), "mr-out-"+strconv.Itoa(taskInfo.PartIndex))
	oFile.Close()
}

// ask new task
func CallAskTask() *TaskInfo {
	args := ExampleArgs{}
	reply := TaskInfo{}
	ok := call("Coordinator.AskTask", &args, &reply)
	if ok {
		return &reply
	} else {
		return nil
	}
}

// notice task done
func CallTaskDone(taskInfo *TaskInfo) {
	reply := ExampleReply{}
	call("Coordinator.TaskDone", taskInfo, &reply)
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
		// log.Fatal("dialing:", err)
		return false
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
