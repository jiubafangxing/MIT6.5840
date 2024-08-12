package mr

import (
	"log"
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
	WorkerId	[]int
	AllMapTasks       []string
	FreeTasks         []string
	RunningMapTaskMap    map[string]TaskState
	FinishMapTaskMap     map[string]TaskState
	FinishFileOfMapTaskMap map[string]TaskState
	RunningRuducemap[string]TaskState
	FinishReduceTaskMap     map[string]TaskState
	FinishFileOfReduceTaskMap map[string]TaskState
	AllReduceNums        int
	RemainingReduceCount int	
	CurMapNo             int
	CurReduceNo 		int
	taskAllocateMutex sync.Mutex
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
	State     int
	BeginTime int64
	EndTime   int64
	FileName string
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
		allocateTask := c.FreeTasks[tasksize-1]
		c.FreeTasks = c.FreeTasks[:tasksize-1]
		taskState := TaskState{
			TaskType:  MAP_TASK,
			FileName : allocateTask,
			State:     INIT,
			BeginTime: time.Now().UnixNano(),
			EndTime:   -1,
			TaskNo:    c.CurMapNo,
			WorkerId, args.WorkerId,
		}
		c.RunningMapTaskMap[c.CurMapNo] = taskState
		reply.FileName = allocateTask 
		c.CurMapNo++
		return nil
	} else {
		if len(c.FinishFileOfMapTaskMap) == len(c.AllMapTasks)	&& c.RemainingReduceCount >=0 {
			taskState := TaskState{
				TaskType:  REDUCE_TASK,
				FileName : allocateTask,
				State:     INIT,
				BeginTime: time.Now().UnixNano(),
				EndTime:   -1,
				TaskNo:    c.CurMapNo,
				WorkerId, args.WorkerId,
			}
			c.RemainingReduceCount -=1
		}
		return nil
	}
	func (c *Coordinator)CallFinish(args *FinishArgs, reply *FinishReply) error{
		var taskState TaskState =  c.RunningMapTaskMap[args.TaskNo]
		taskState.EndTime = time.Now().UnixNano()
		taskState.ResultFileName = args.ResultFileNames
		taskState.State =2
		delete(c.RunningMapTaskMap, taskState.TaskNo)
		c.FinishMapTaskMap[taskState.TaskNo] = taskState
		if nil == c.FinishFileOfMapTaskMap[taskState.FileName]{
			c.FinishFileOfMapTaskMap[taskState.FileName] = taskState
		}
		if taskState.TaskType == REDUCE_TASK{

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
		c.ReduceNums = nReduce
		c.= nReduce
		c.CurMapNo = 0
		c.server()
		return &c
	}
