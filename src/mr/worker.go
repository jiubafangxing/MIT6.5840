package mr

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

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
		reply, err := ApplyTask()
		intermediate := []KeyValue{}
		switch reply.TaskType {
		case MAP_TASK:
			MapWorkTask(mapf, err, reply, intermediate)
		case REDUCE_TASK:
			ReduceWorkTask(reducef, err, reply)
		case WAIT_TASK:
			WaitTask()
		case EXIT_TASK:
			log.Println("worker exit")
			return
		}
	}

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

func WaitTask() {
	time.Sleep(time.Second * 1)
}

func ReduceWorkTask(reducef func(string, []string) string, err error, reply WorkerReply) {
	toParseFiles := reply.FileName
	//log.Printf("重新执行，filenames%v\n", toParseFiles)
	results := make([]string, 0)
	if len(toParseFiles) > 0 {
		files := make([]*os.File, 0) // 注意：初始大小为0，而不是1
		for i := 0; i < len(toParseFiles); i++ {
			file, err := os.Open(toParseFiles[i])
			if err != nil {
				log.Fatalf("cannot open %v, error: %v", toParseFiles[i], err)
			}
			files = append(files, file)
		}

		//log.Printf("exe 1")
		intermediate := []KeyValue{}
		for _, file := range files {
			reader := json.NewDecoder(file)
			for {
				var kv KeyValue
				if err := reader.Decode(&kv); err != nil {
					if err.Error() == "EOF" {
						break
					}
				}
				intermediate = append(intermediate, kv)
			}

		}
		//log.Printf("exe 2")
		sort.Sort(ByKey(intermediate))
		oname := fmt.Sprintf("mr-out-%d", reply.TaskNo)
		results = append(results, oname)
		ofile, _ := os.Create(oname)

		//
		// call Reduce on each distinct key in intermediate[],
		// and print the result to mr-out-0.
		//
		i := 0
		//lo gg.Printf("exe 3")
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

		ofile.Close()
		defer func() {
			for _, file := range files {
				file.Close()
			}
		}()
	}
	defer CallFinish(reply.TaskNo, reply.TaskType, results)
}
func MapWorkTask(mapf func(string, string) []KeyValue, err error, reply WorkerReply, intermediate []KeyValue) {
	resultFileNames := make([]string, 0)
	if nil == err && len(reply.FileName) > 0 {
		file, err := os.Open(reply.FileName[0])
		if err != nil {
			log.Fatalf("cannot open %v", reply.FileName)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", reply.FileName)
		}
		file.Close()
		kva := mapf(reply.FileName[0], string(content))
		intermediate = append(intermediate, kva...)
		var resultData map[string][]KeyValue = make(map[string][]KeyValue)
		var openFiles map[string]*json.Encoder = make(map[string]*json.Encoder)
		for _, item := range intermediate {
			intermediateFileName := "mr"
			reduceNo := ihash(item.Key) % reply.REDUCE_NUMS
			elems := []string{intermediateFileName, strconv.Itoa(reply.TaskNo), strconv.Itoa(reduceNo)}
			intermediateFileName = strings.Join(elems, "-")
			if nil == openFiles[intermediateFileName] {
				ofile, _ := os.Create(intermediateFileName)
				writer := json.NewEncoder(ofile)
				openFiles[intermediateFileName] = writer
				defer ofile.Close()
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
			resultFileNames = append(resultFileNames, resultDateItemK)
			for _, item := range resultDateItemV {
				encoder.Encode(item)
			}
		}
	}
	defer CallFinish(reply.TaskNo, reply.TaskType, resultFileNames)
}

func CallFinish(taskNo int, taskType int, resultFileNames []string) {
	args := FinishArgs{}
	args.TaskNo = taskNo
	args.TaskType = taskType
	args.ResultFileNames = resultFileNames
	reply := WorkerReply{}
	call("Coordinator.State", &args, &reply)
}
func ApplyTask() (filname WorkerReply, err error) {
	// it could be more standalone if append ip
	pid := os.Getpid()
	args := WorkerArgs{WorkerId: pid}
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
