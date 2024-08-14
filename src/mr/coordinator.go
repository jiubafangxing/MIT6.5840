package mr

import (
	"fmt"
	"log"
	"strings"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

const (
	MAP_TASK = iota
	REDUCE_TASK
)
const (
	INIT = iota
)

type Coordinator struct {
	// Your definitions here.
	WorkerId                      []int
	AllMapTasks                   []string
	FreeTasks                     []string
	RunningMapTaskMap             map[int]TaskState
	FinishMapTaskMap              map[int]TaskState
	FinishFileOfMapTaskMap        map[string]TaskState
	RunningRuducemap              map[int]TaskState
	FinishReduceTaskMap           map[string]TaskState
	FinishFileOfReduceTaskMap     map[string]TaskState
	AllReduceNums                 int
	RemainingReduceCount          int
	CurMapNo                      int
	CurReduceNo                   int
	taskAllocateMutex             sync.Mutex
	cacheIntermediateFileNamesMap map[string][]string
}

// Your code here -- RPC handlers for the worker to call.
type TaskState struct {
	TaskNo int
	//0 means
	TaskType int
	//0 means init
	//1 means start running
	//2 means end
	//-1 means running fail
	State           int
	BeginTime       int64
	EndTime         int64
	InputFileNames  []string
	ResultFileNames []string
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) ApplyTask(args *WorkerArgs, reply *WorkerReply) error {
	defer c.taskAllocateMutex.Unlock()
	c.taskAllocateMutex.Lock()
	if len(c.FreeTasks) > 0 {
		tasksize := len(c.FreeTasks)
		allocateTask := c.FreeTasks[tasksize-1 : tasksize]
		c.FreeTasks = c.FreeTasks[:tasksize-1]
		taskState := TaskState{
			TaskType:       MAP_TASK,
			InputFileNames: allocateTask,
			State:          INIT,
			BeginTime:      time.Now().UnixNano(),
			EndTime:        -1,
			TaskNo:         c.CurMapNo,
		}
		c.RunningMapTaskMap[c.CurMapNo] = taskState
		reply.FileName = allocateTask
		c.CurMapNo++
		return nil
	} else {
		if len(c.FinishFileOfMapTaskMap) == len(c.AllMapTasks) && c.RemainingReduceCount >= 0 {
			reduceInputFileNames := c.allocateIntermediateFileNames()
			taskState := TaskState{
				TaskType:       REDUCE_TASK,
				InputFileNames: reduceInputFileNames,
				State:          INIT,
				BeginTime:      time.Now().UnixNano(),
				EndTime:        -1,
				TaskNo:         c.CurReduceNo,
			}
			c.RunningRuducemap[c.CurReduceNo] = taskState
			c.RemainingReduceCount -= 1
			c.CurReduceNo++
		}
		return nil
	}
}
func (c *Coordinator) allocateIntermediateFileNames() []string {
	if len(c.cacheIntermediateFileNamesMap) == 0 {
		for _, v := range c.FinishFileOfMapTaskMap {
			for _, intermediateFileName := range v.ResultFileNames {
				fileNameItems := strings.Split(intermediateFileName, "-")
				match := fileNameItems[len(fileNameItems)-1]
				if _, ok := c.cacheIntermediateFileNamesMap[match]; !ok {
					intermediateFileNames := make([]string, 1)
					c.cacheIntermediateFileNamesMap[match] = intermediateFileNames
				}
				c.cacheIntermediateFileNamesMap[match] = append(c.cacheIntermediateFileNamesMap[match], intermediateFileName)
			}
		}
	}
	allocateKey := c.RemainingReduceCount
	allocateKeyStr := fmt.Sprintf("%d", allocateKey)
	c.RemainingReduceCount--
	return c.cacheIntermediateFileNamesMap[allocateKeyStr]
}
func (c *Coordinator) CallFinish(args *FinishArgs, reply *FinishReply) error {
	var taskState = c.RunningMapTaskMap[args.TaskNo]
	taskState.EndTime = time.Now().UnixNano()
	taskState.ResultFileNames = args.ResultFileNames
	taskState.State = 2
	delete(c.RunningMapTaskMap, taskState.TaskNo)
	c.FinishMapTaskMap[taskState.TaskNo] = taskState
	taskFileName := taskState.InputFileNames[0]
	if v, ok := c.FinishFileOfMapTaskMap[taskFileName]; !ok {
		c.FinishFileOfMapTaskMap[taskFileName] = v
	}
	if taskState.TaskType == REDUCE_TASK {

	}
	reply.TaskNo = taskState.TaskNo
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
	c.AllMapTasks = files
	freeList := make([]string, len(files))
	copy(files, freeList)
	c.FreeTasks = freeList
	c.AllReduceNums = nReduce
	c.RemainingReduceCount = nReduce
	c.CurMapNo = 0
	c.server()
	return &c
}
