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
	WAIT_TASK
	EXIT_TASK
)
const (
	INIT = iota
)

type Coordinator struct {
	// Your definitions here.
	WorkerId                      []int
	AllMapTasks                   []string
	AllReduceTasks                []string
	FreeTasks                     []string
	RunningMapTaskMap             map[int]TaskState
	FinishMapTaskMap              map[int]TaskState
	FinishFileOfMapTaskMap        map[string]TaskState
	RunningRuducemap              map[int]TaskState
	FinishReduceTaskMap           map[int]TaskState
	FinishFileOfReduceTaskMap     map[int]TaskState
	cacheIntermediateFileNamesMap map[string][]string
	AllReduceNums                 int
	RemainingReduceCount          int
	CurMapNo                      int
	CurReduceNo                   int
	taskAllocateMutex             sync.Mutex
	StateWriteMutex               sync.Mutex
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
		//log.Printf("allocate map task for %d is %s", c.CurMapNo, allocateTask)
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
		reply.TaskNo = taskState.TaskNo
		reply.REDUCE_NUMS = c.AllReduceNums
		c.CurMapNo = c.CurMapNo + 1
		return nil
	} else {
		if len(c.FinishFileOfMapTaskMap) == len(c.AllMapTasks) {
			if c.RemainingReduceCount > 0 {
				reduceInputFileNames := c.allocateIntermediateFileNames(c.CurReduceNo)
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
				c.CurReduceNo = c.CurReduceNo + 1
				reply.FileName = reduceInputFileNames
				reply.TaskNo = taskState.TaskNo
				reply.TaskType = REDUCE_TASK
				reply.REDUCE_NUMS = c.AllReduceNums
			} else {
				exitTask := true
				if len(c.FinishReduceTaskMap) != c.AllReduceNums {
					for _, v := range c.RunningRuducemap {
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
							c.RunningRuducemap[v.TaskNo] = taskState
							reply.FileName = taskState.InputFileNames
							reply.TaskNo = taskState.TaskNo
							reply.TaskType = REDUCE_TASK
							reply.REDUCE_NUMS = c.AllReduceNums
							//log.Printf("重新执行%d reduce task %d", reply.TaskNo, v.TaskNo)
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
			//如果超过10s仍然没有完成，会分配新的task
			for _, v := range c.RunningMapTaskMap {
				if time.Now().UnixNano()-v.BeginTime > int64(10*time.Second) {
					//log.Println("maptask : taskno :%d running failed: reallocate", v.TaskNo)
					names := v.InputFileNames
					taskState := TaskState{
						TaskType:       MAP_TASK,
						InputFileNames: names,
						State:          INIT,
						BeginTime:      time.Now().UnixNano(),
						EndTime:        -1,
						TaskNo:         c.CurReduceNo,
					}
					c.RunningRuducemap[c.CurReduceNo] = taskState
					c.RemainingReduceCount -= 1
					c.CurReduceNo = c.CurReduceNo + 1
					reply.FileName = names
					reply.TaskNo = taskState.TaskNo
					reply.TaskType = MAP_TASK
					reply.REDUCE_NUMS = c.AllReduceNums
					exeWait = false
					break
				}
			}
			if exeWait {
				log.Println("WAIT_TASK")
				reply.TaskType = WAIT_TASK
			}
		}
		return nil
	}
}
func (c *Coordinator) allocateIntermediateFileNames(curNo int) []string {
	if len(c.cacheIntermediateFileNamesMap) == 0 {
		for _, v := range c.FinishFileOfMapTaskMap {
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
		//log.Printf("State MAP_TASK:%d\n", args.TaskNo)
		taskState = c.RunningMapTaskMap[args.TaskNo]
		//log.Printf("State MAP_TASK taskState:%v\n", taskState)
		taskState.EndTime = time.Now().UnixNano()
		taskState.ResultFileNames = args.ResultFileNames
		taskState.State = 2
		delete(c.RunningMapTaskMap, taskState.TaskNo)

		c.FinishMapTaskMap[taskState.TaskNo] = taskState
		//log.Println("taskState")
		//log.Println(taskState)
		//jsonData, err := json.Marshal(taskState)
		//if err != nil {
		//	log.Printf("Error marshalling taskState: %v", err)
		//}
		taskFileName := taskState.InputFileNames[0]
		if _, ok := c.FinishFileOfMapTaskMap[taskFileName]; !ok {
			//log.Printf("all input files %v\n", c.AllMapTasks)
			//log.Printf("map task finish for taskNo %d of taskFileName %s\n", args.TaskNo, taskFileName)
			log.Printf("map taskNo %d of mapInputName %s\n", args.TaskNo, taskFileName)
			log.Printf("map taskNo %d of mapoutputName %s\n", args.TaskNo, args.ResultFileNames)
			for _, r := range args.ResultFileNames {
				rnameitems := strings.Split(r, "-")
				c.AllReduceTasks = append(c.AllReduceTasks, rnameitems[len(rnameitems)-1])
			}
			//
			c.FinishFileOfMapTaskMap[taskFileName] = taskState
		}
	case REDUCE_TASK:
		taskState = c.RunningRuducemap[args.TaskNo]
		delete(c.RunningRuducemap, taskState.TaskNo)
		taskNo := args.TaskNo
		taskState.ResultFileNames = args.ResultFileNames
		taskState.EndTime = time.Now().UnixNano()
		taskState.State = 2
		if _, ok := c.FinishReduceTaskMap[taskNo]; !ok {
			log.Printf("reduce finsih %d\n", taskState.TaskNo)
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
	ret := len(c.FinishReduceTaskMap) == c.AllReduceNums
	//log.Printf("finished %v\n ", ret)
	//log.Printf("%d", len(c.FinishMapTaskMap))
	//log.Printf("%d", c.AllReduceNums)
	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	//log.Println("files")
	//log.Println(files)
	freeList := make([]string, len(files))
	copy(freeList, files)
	c := Coordinator{
		WorkerId:                      []int{},
		AllMapTasks:                   files,
		FreeTasks:                     freeList,
		RunningMapTaskMap:             make(map[int]TaskState),
		FinishMapTaskMap:              make(map[int]TaskState),
		FinishFileOfMapTaskMap:        make(map[string]TaskState),
		RunningRuducemap:              make(map[int]TaskState),
		FinishReduceTaskMap:           make(map[int]TaskState),
		cacheIntermediateFileNamesMap: make(map[string][]string),
		AllReduceNums:                 nReduce,
		RemainingReduceCount:          nReduce,
		CurMapNo:                      0,
		CurReduceNo:                   0,
	}
	c.server()
	return &c
}
