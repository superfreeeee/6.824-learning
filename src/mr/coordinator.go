package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

// Task in Coordinator
type TaskStatInterface interface {
	CreateTaskInfo() TaskInfo
	SetNow()
	OutOfTime() bool   // task timeout
	GetFileIndex() int // map id
	GetPartIndex() int // reduce id
}

// impl TaskStatInterface
type TaskStat struct {
	createTime time.Time
	fileName   string
	fileIndex  int
	nFiles     int
	partIndex  int
	nReduce    int
}

const TEN_SEC = time.Duration(time.Second * 10)

func (t *TaskStat) OutOfTime() bool {
	currentTime := time.Now()
	return currentTime.Sub(t.createTime) >= TEN_SEC
}

func (t *TaskStat) SetNow() {
	t.createTime = time.Now()
}

func (t *TaskStat) GetFileIndex() int {
	return t.fileIndex
}

func (t *TaskStat) GetPartIndex() int {
	return t.partIndex
}

type MapTaskStat struct {
	TaskStat
}

func (t *MapTaskStat) CreateTaskInfo() TaskInfo {
	return TaskInfo{
		State:     TaskMap,
		FileName:  t.fileName,
		FileIndex: t.fileIndex,
		NFiles:    t.nFiles,
		PartIndex: t.partIndex,
		NReduce:   t.nReduce,
	}
}

type ReduceTaskStat struct {
	TaskStat
}

func (t *ReduceTaskStat) CreateTaskInfo() TaskInfo {
	return TaskInfo{
		State:     TaskReduce,
		FileName:  t.fileName,
		FileIndex: t.fileIndex,
		NFiles:    t.nFiles,
		PartIndex: t.partIndex,
		NReduce:   t.nReduce,
	}
}

// task queue in Coordinator
type TaskStatQueue struct {
	taskArray []TaskStatInterface
	mutex     sync.Mutex
}

func (Q *TaskStatQueue) lock() {
	Q.mutex.Lock()
}

func (Q *TaskStatQueue) unlock() {
	Q.mutex.Unlock()
}

func (Q *TaskStatQueue) Size() int {
	return len(Q.taskArray)
}

func (Q *TaskStatQueue) Pop() TaskStatInterface {
	Q.lock()

	// check array
	rest := Q.Size()
	if rest == 0 {
		Q.unlock()
		return nil
	}

	// pop taskStat
	taskStat := Q.taskArray[rest-1]
	Q.taskArray = Q.taskArray[:rest-1]

	Q.unlock()
	return taskStat
}

func (Q *TaskStatQueue) Push(taskStat TaskStatInterface) {
	Q.lock()

	// check taskStat
	if taskStat == nil {
		Q.unlock()
		return
	}

	// append
	Q.taskArray = append(Q.taskArray, taskStat)

	Q.unlock()
}

func (Q *TaskStatQueue) RemoveTask(fileIndex int, partIndex int) {
	Q.lock()

	targetIndex := -1
	for index, taskStat := range Q.taskArray {
		if taskStat.GetFileIndex() == fileIndex && taskStat.GetPartIndex() == partIndex {
			targetIndex = index
			break
		}
	}

	// target not found
	if targetIndex == -1 {
		Q.unlock()
		return
	}

	// append
	Q.taskArray = append(Q.taskArray[:targetIndex], Q.taskArray[targetIndex+1:]...)

	Q.unlock()
}

type Coordinator struct {
	// Your definitions here.
	files   []string
	nReduce int
	isDone  bool

	mapTaskWaiting    TaskStatQueue
	mapTaskRunning    TaskStatQueue
	reduceTaskWaiting TaskStatQueue
	reduceTaskRunning TaskStatQueue
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) AskTask(args *ExampleArgs, reply *TaskInfo) error {
	// check reduce task
	reduceTask := c.reduceTaskWaiting.Pop()
	if reduceTask != nil {
		// record create time
		reduceTask.SetNow()
		// task running
		c.reduceTaskRunning.Push(reduceTask)
		// setup reply
		*reply = reduceTask.CreateTaskInfo()
		// fmt.Printf("Distribute TaskReduce(%v)\n", reply.PartIndex)
		return nil
	}

	// check map task
	mapTask := c.mapTaskWaiting.Pop()
	if mapTask != nil {
		// record create time
		mapTask.SetNow()
		// task running
		c.mapTaskRunning.Push(mapTask)
		// setup reply
		*reply = mapTask.CreateTaskInfo()
		// fmt.Printf("Distribute TaskMap(%v_%v)\n", reply.FileName, reply.FileIndex)
		return nil
	}

	if c.mapTaskRunning.Size() > 0 || c.reduceTaskRunning.Size() > 0 {
		// wait
		reply.State = TaskWait
		// fmt.Println("Distribute TaskWait")
		return nil
	} else {
		// done
		reply.State = TaskDone
		c.isDone = true
		// fmt.Println("Distribute TaskDone")
		return nil
	}
}

func (c *Coordinator) TaskDone(taskInfo *TaskInfo, reply *ExampleReply) error {
	switch taskInfo.State {
	case TaskMap:
		// fmt.Printf("TaskMap(%v_%v) Complete\n", taskInfo.FileName, taskInfo.FileIndex)
		c.mapTaskRunning.RemoveTask(taskInfo.FileIndex, taskInfo.PartIndex)
		if c.mapTaskWaiting.Size() == 0 && c.mapTaskRunning.Size() == 0 {
			c.distributeReduce()
		}

	case TaskReduce:
		// fmt.Printf("TaskReduce(%v) Complete\n", taskInfo.PartIndex)
		c.reduceTaskRunning.RemoveTask(taskInfo.FileIndex, taskInfo.PartIndex)

	default:
		panic("Invalid TaskDone.State")
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
	// fmt.Println("> sockname =", sockname)
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
	// Your code here.
	return (c.mapTaskWaiting.Size() == 0 &&
		c.mapTaskRunning.Size() == 0 &&
		c.reduceTaskWaiting.Size() == 0 &&
		c.reduceTaskRunning.Size() == 0) ||
		c.isDone
}

func (c *Coordinator) distributeMap() {
	// create map tasks
	nFiles := len(c.files)
	for fileIndex, fileName := range c.files {
		task := MapTaskStat{
			TaskStat: TaskStat{
				createTime: time.Now(),
				fileName:   fileName,
				fileIndex:  fileIndex,
				nFiles:     nFiles,
				nReduce:    c.nReduce,
			},
		}
		task.SetNow()
		c.mapTaskWaiting.Push(&task)
	}

	// fmt.Printf(">>> Distribute TaskMap(%v)\n", c.mapTaskWaiting.Size())
}

func (c *Coordinator) distributeReduce() {
	// create reduce tasks
	nFiles := len(c.files)
	for partIndex := c.nReduce - 1; partIndex >= 0; partIndex -= 1 {
		task := ReduceTaskStat{
			TaskStat: TaskStat{
				createTime: time.Now(),
				partIndex:  partIndex,
				nFiles:     nFiles,
				nReduce:    c.nReduce,
			},
		}
		task.SetNow()
		c.reduceTaskWaiting.Push(&task)
	}
	// fmt.Printf(">>> Distribute TaskReduce(%v)\n", c.reduceTaskWaiting.Size())
}

func (c *Coordinator) timeoutCheck() {
	for !c.isDone {
		for _, taskStat := range c.mapTaskRunning.taskArray {
			if taskStat.OutOfTime() {
				c.mapTaskRunning.RemoveTask(taskStat.GetFileIndex(), taskStat.GetPartIndex())
				taskStat.SetNow()
				c.mapTaskWaiting.Push(taskStat)
			}
		}

		for _, taskStat := range c.reduceTaskRunning.taskArray {
			if taskStat.OutOfTime() {
				c.reduceTaskRunning.RemoveTask(taskStat.GetFileIndex(), taskStat.GetPartIndex())
				taskStat.SetNow()
				c.reduceTaskWaiting.Push(taskStat)
			}
		}

		// for _, taskStat := range c.mapTaskWaiting.taskArray {
		// 	if taskStat.OutOfTime() {
		// 		c.isDone = true
		// 	}
		// }

		// for _, taskStat := range c.reduceTaskWaiting.taskArray {
		// 	if taskStat.OutOfTime() {
		// 		c.isDone = true
		// 	}
		// }

		// check every 2s
		time.Sleep(time.Duration(time.Second * 2))
	}
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		files:   files,
		nReduce: nReduce,
	}

	// Your code here.
	c.distributeMap()
	go c.timeoutCheck()

	c.server()
	return &c
}
