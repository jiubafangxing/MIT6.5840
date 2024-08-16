package mr

import (
	"fmt"
	"log"
	"strconv"
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
	WAIT_TASK
	EXIT_TASK
)
const (
	INIT = iota
)

const (
	ALLOCATE_MAP_TASK_STATE = iota
	CHECK_MAP_TASK_STATE
)

type Coordinator struct {
	MapTaskNos                    []int
	AllMapTasksInputFiles         []string
	AllReduceTasks                []string
	RunningMapTaskMap             map[int]TaskState
	FinishMapTaskMap              map[int]TaskState
	RunningReduceMap              map[int]TaskState
	FinishReduceTaskMap           map[int]TaskState
	cacheIntermediateFileNamesMap map[string][]string
	AllReduceNums                 int
	RemainingReduceNo             []int
	taskAllocateMutex             sync.Mutex
	StateWriteMutex               sync.Mutex
}

type TaskState struct {
	TaskNo          int
	TaskType        int
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
	if len(c.MapTaskNos) > 0 {
		taskNo := c.allocateMapTaskNo()
		allocateTask := c.AllMapTasksInputFiles[taskNo : taskNo+1]
		taskState := BuildTask(allocateTask, taskNo, MAP_TASK)
		c.RunningMapTaskMap[taskState.TaskNo] = taskState
		reply.FileName = allocateTask
		reply.TaskNo = taskState.TaskNo
		reply.REDUCE_NUMS = c.AllReduceNums
		return nil
	} else {
		if len(c.FinishMapTaskMap) == len(c.AllMapTasksInputFiles) {
			if len(c.RemainingReduceNo) > 0 {
				allocateNo := c.RemainingReduceNo[len(c.RemainingReduceNo)-1:][0]
				reduceInputFileNames := c.allocateIntermediateFileNames(allocateNo)
				taskState := BuildTask(reduceInputFileNames, allocateNo, REDUCE_TASK)
				c.RunningReduceMap[allocateNo] = taskState
				c.RemainingReduceNo = c.RemainingReduceNo[0 : len(c.RemainingReduceNo)-1]
				reply.FileName = reduceInputFileNames
				reply.TaskNo = taskState.TaskNo
				reply.TaskType = REDUCE_TASK
				reply.REDUCE_NUMS = c.AllReduceNums
			} else {
				exitTask := true
				if len(c.FinishReduceTaskMap) != len(c.AllReduceTasks) {
					for _, v := range c.RunningReduceMap {
						if _, ok := c.FinishReduceTaskMap[v.TaskNo]; !ok {
							exitTask = false
							taskState := TaskState{
								TaskType:       REDUCE_TASK,
								InputFileNames: v.InputFileNames,
								State:          INIT,
								BeginTime:      time.Now().UnixNano(),
								EndTime:        -1,
								TaskNo:         v.TaskNo,
							}
							c.RunningReduceMap[v.TaskNo] = taskState
							reply.FileName = taskState.InputFileNames
							reply.TaskNo = taskState.TaskNo
							reply.TaskType = REDUCE_TASK
							reply.REDUCE_NUMS = c.AllReduceNums
							return nil
						}
					}
				}
				if exitTask {
					log.Println("exit")
					reply.TaskType = EXIT_TASK
				}
			}
		} else {
			exeWait := true
			for _, v := range c.RunningMapTaskMap {
				if time.Now().UnixNano()-v.BeginTime > int64(10*time.Second) {
					v.BeginTime = time.Now().UnixNano()
					c.RunningMapTaskMap[v.TaskNo] = v
					reply.FileName = v.InputFileNames
					reply.TaskNo = v.TaskNo
					reply.TaskType = MAP_TASK
					reply.REDUCE_NUMS = c.AllReduceNums
					exeWait = false
					return nil
				}
			}
			if exeWait {
				reply.TaskType = WAIT_TASK
			}
		}
		return nil
	}
}

func BuildTask(allocateTask []string, taskNo int, taskType int) TaskState {
	return TaskState{
		TaskType:       taskType,
		InputFileNames: allocateTask,
		State:          INIT,
		BeginTime:      time.Now().UnixNano(),
		EndTime:        -1,
		TaskNo:         taskNo,
	}
}
func (c *Coordinator) allocateIntermediateFileNames(curNo int) []string {
	if len(c.cacheIntermediateFileNamesMap) == 0 {
		for _, v := range c.FinishMapTaskMap {
			for _, intermediateFileName := range v.ResultFileNames {
				fileNameItems := strings.Split(intermediateFileName, "-")
				match := fileNameItems[len(fileNameItems)-1]
				if _, ok := c.cacheIntermediateFileNamesMap[match]; !ok {
					intermediateFileNames := make([]string, 0)
					c.cacheIntermediateFileNamesMap[match] = intermediateFileNames
				}
				c.cacheIntermediateFileNamesMap[match] = append(c.cacheIntermediateFileNamesMap[match], intermediateFileName)
			}
		}
	}
	allocateKey := curNo % c.AllReduceNums
	allocateKeyStr := fmt.Sprintf("%d", allocateKey)
	taskNames := c.cacheIntermediateFileNamesMap[allocateKeyStr]
	//log.Printf("allocate taskNames %v for map  taskNo %d\n", taskNames, allocateKey)
	return taskNames
}
func (c *Coordinator) State(args *FinishArgs, reply *FinishReply) error {
	var taskState TaskState
	c.StateWriteMutex.Lock()
	defer c.StateWriteMutex.Unlock()
	switch args.TaskType {
	case MAP_TASK:
		taskState = c.RunningMapTaskMap[args.TaskNo]
		taskState.EndTime = time.Now().UnixNano()
		taskState.ResultFileNames = args.ResultFileNames
		taskState.State = 2
		delete(c.RunningMapTaskMap, taskState.TaskNo)
		if len(taskState.InputFileNames) > 0 {
			if _, ok := c.FinishMapTaskMap[taskState.TaskNo]; !ok {
				for _, r := range args.ResultFileNames {
					rnameitems := strings.Split(r, "-")
					if !c.ContainsReduce(rnameitems[len(rnameitems)-1]) {
						c.AllReduceTasks = append(c.AllReduceTasks, rnameitems[len(rnameitems)-1])
						r, e := strconv.Atoi(rnameitems[len(rnameitems)-1])
						if nil == e {
							c.RemainingReduceNo = append(c.RemainingReduceNo, r)
						}
					}
				}
				c.FinishMapTaskMap[taskState.TaskNo] = taskState
			}
		}
	case REDUCE_TASK:
		taskState = c.RunningReduceMap[args.TaskNo]
		delete(c.RunningReduceMap, taskState.TaskNo)
		taskNo := args.TaskNo
		taskState.ResultFileNames = args.ResultFileNames
		taskState.EndTime = time.Now().UnixNano()
		taskState.State = 2
		if _, ok := c.FinishReduceTaskMap[taskNo]; !ok {
			//log.Printf("reduce finsih %d\n", taskState.TaskNo)
			c.FinishReduceTaskMap[taskNo] = taskState
		}
	}
	reply.TaskNo = taskState.TaskNo
	return nil
}
func (c *Coordinator) ContainsReduce(target string) bool {
	for _, str := range c.AllReduceTasks {
		if strings.Index(str, target) != -1 {
			return true
		}
	}
	return false
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
	fin := len(c.FinishReduceTaskMap)
	all := len(c.AllReduceTasks)
	if len(c.AllMapTasksInputFiles) == 0 {
		return fin == all
	} else {
		return fin != 0 && fin == all
	}
}

func (c *Coordinator) allocateMapTaskNo() int {
	if len(c.MapTaskNos) > 0 {
		result := c.MapTaskNos[len(c.MapTaskNos)-1]
		c.MapTaskNos = c.MapTaskNos[:len(c.MapTaskNos)-1]
		return result
	} else {
		return -1
	}
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	//log.Println("files")
	//log.Println(files)
	freeList := make([]string, len(files))
	mapTaskNos := initMapTaskNos(files)
	copy(freeList, files)
	c := Coordinator{
		AllMapTasksInputFiles:         files,
		MapTaskNos:                    mapTaskNos,
		RunningMapTaskMap:             make(map[int]TaskState),
		FinishMapTaskMap:              make(map[int]TaskState),
		RunningReduceMap:              make(map[int]TaskState),
		FinishReduceTaskMap:           make(map[int]TaskState),
		cacheIntermediateFileNamesMap: make(map[string][]string),
		AllReduceNums:                 nReduce,
		RemainingReduceNo:             []int{},
	}
	c.server()
	return &c
}

func initMapTaskNos(files []string) []int {
	var results = make([]int, 0)
	for idx, _ := range files {
		results = append(results, idx)
	}
	return results
}
