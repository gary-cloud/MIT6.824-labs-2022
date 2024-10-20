package mr

import "fmt"
import "log"
// import "io"
// import "os"
import "net/rpc"
import "hash/fnv"
// import "encoding/json"


//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

	assignArgs := AssignTaskArgs{}
	assignReply := AssignTaskReply{}
	
	ok := call("Coordinator.AssignTaskHandler", &assignArgs, &assignReply)
	for  {
		if ok {
			if assignReply.Type == "map" {
				filename := assignReply.Map.FileName
				taskId := assignReply.Map.TaskId
				// nReduce := assignReply.NReduce

				fmt.Println("map: ", filename, " + ", taskId)

				// file, err := os.Open(filename)
				// if err != nil {
				// 	log.Fatalf("cannot open %v", filename)
				// }
				// content, err := io.ReadAll(file)
				// if err != nil {
				// 	log.Fatalf("cannot read %v", filename)
				// }
				// file.Close()
				// kva := mapf(filename, string(content))

				// for i != 0; i < nReduce; i++ {
				// 	// 创建一个临时文件
				// 	tempFile, err := os.CreateTemp("", "example*.txt")
				// 	if err != nil {
				// 		fmt.Println("Error creating temp file:", err)
				// 		return
				// 	}
				// 	// 使用完临时文件后删除它
				// 	defer os.Remove(tempFile.Name())
				// }
				commitArgs := CommitTaskArgs{taskId, "map"}
				commitReply := CommitTaskReply{}

				call("Coordinator.CommitTaskHandler", &commitArgs, &commitReply)

			} else if assignReply.Type == "reduce" {
				taskId := assignReply.Reduce.TaskId
				// nReduce := assignReply.NReduce

				fmt.Println("reduce: ", taskId)

				commitArgs := CommitTaskArgs{taskId, "reduce"}
				commitReply := CommitTaskReply{}

				call("Coordinator.CommitTaskHandler", &commitArgs, &commitReply)

			} else {
				break
			}

			assignArgs = AssignTaskArgs{}
			assignReply = AssignTaskReply{}
			ok = call("Coordinator.AssignTaskHandler", &assignArgs, &assignReply)
		} else {
			fmt.Printf("call failed!\n")
		}
	}
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
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

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
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
