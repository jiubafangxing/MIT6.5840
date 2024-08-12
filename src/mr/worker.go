package mr

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
)
import "log"
import "net/rpc"
import "hash/fnv"
import "os"
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

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	// Your worker implementation here.
	for {
		reply, err := ApplyTask()
		intermediate := []KeyValue{}
		switch reply.TaskType {
		case MAP_TASK:
			MapWorkTask(mapf, err, reply, intermediate)
		case REDUCE_TASK:

		}
	}

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

func MapWorkTask(mapf func(string, string) []KeyValue, err error, reply WorkerReply, intermediate []KeyValue) {
	resultFileNames := make([]string)
	if nil != err && len(reply.FileName) > 0 {
		file, err := os.Open(reply.FileName)
		if err != nil {
			log.Fatalf("cannot open %v", reply.FileName)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", reply.FileName)
		}
		file.Close()
		kva := mapf(reply.FileName, string(content))
		intermediate = append(intermediate, kva...)
		var resultData map[string][]KeyValue = make(map[string][]KeyValue)
		var openFiles map[string]*json.Encoder
		for _, item := range intermediate {
			intermediateFileName := "mr"
			reduceNo := ihash(item.Key)
			elems := []string{intermediateFileName, strconv.Itoa(reply.TaskNo), strconv.Itoa(reduceNo)}
			intermediateFileName = strings.Join(elems, "-")
			if nil == openFiles[intermediateFileName] {
				ofile, _ := os.Create(intermediateFileName)
				writer := json.NewEncoder(ofile)
				openFiles[intermediateFileName] = writer
			}
			var existData []KeyValue = resultData[intermediateFileName]
			if nil != existData && len(existData) > 0 {
				existData = append(existData, item)
			} else {
				existData = make([]KeyValue, 1)
				existData[0] = item
			}
			resultData[intermediateFileName] = existData

		}
		for resultDateItemK, resultDateItemV := range resultData {
			encoder := openFiles[resultDateItemK]
			resultFileNames = append(resultFileNames,resultDataItemK)
			for _, item := range resultDateItemV {
				encoder.Encode(item)
			}
		}
	}
	defer CallFinish(reply.TaskNo,resultFileNames)
}

func CallFinish(taskNo int, resultFileNames []string) {
	if nil == TaskNo {
		return
	}
	args := WorkerArgs{}
	args.TaskNo =  taskNo
	args.ResultFileNames = resultFileNames
	reply := WorkerReply{}
	call("Coordinator.State", &args, &reply)
}
func ApplyTask() (filname WorkerReply, err error) {
	// it could be more standalone if append ip
	pid := os.GetPid()
	args := WorkerArgs{WorkerId:pid,}
	reply := WorkerReply{}
	ok := call("Coordinator.ApplyTask", &args, &reply)
	if ok {
		return reply, nil
	} else {
		return reply, errors.New("call fail")
	}
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
