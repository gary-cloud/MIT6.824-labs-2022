package mr

import "fmt"
import "log"
import "io"
import "os"
import "sort"
import "regexp"
import "strconv"
import "net/rpc"
import "hash/fnv"
import "encoding/json"
import "path/filepath"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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
		if !ok {
			fmt.Printf("call failed!\n")
			continue
		}

		if assignReply.Type == "map" {
			filename := assignReply.Map.FileName
			taskId := assignReply.Map.TaskId
			nReduce := assignReply.NReduce

			fmt.Println("map: ", filename, " - ", taskId)

			file, err := os.Open(filename)
			if err != nil {
				log.Fatalf("cannot open %v", filename)
			}
			content, err := io.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", filename)
			}
			file.Close()
			kva := mapf(filename, string(content))


			// Create temp file and json encoder slice.
			fileSlice := make([]*os.File, nReduce)
			jsonEncoderSlice := make([]*json.Encoder, nReduce)

			for i := 0; i < nReduce; i++ {
				// Create a temp file.
				tempFile, err := os.CreateTemp("", "example-*.txt")
				if err != nil {
					fmt.Println("Error creating temp file:", err)
					return
				}
				fileSlice[i] = tempFile
				jsonEncoderSlice[i] = json.NewEncoder(tempFile)
				// Delete the tempfile at the end of function.
				defer tempFile.Close()
				defer os.Remove(tempFile.Name())
			}

			// Record k-v in json format.
			for _, kv := range kva {
				err := jsonEncoderSlice[ihash(kv.Key) % nReduce].Encode(&kv)
				if err != nil {
					fmt.Println("Error encoding k-v to json:", err)
					return
				}
			}

			// Rename temp files.
			for i := 0; i < nReduce; i++{
				newName := "mr-" + strconv.Itoa(taskId) + "-" + strconv.Itoa(i)
				err = os.Rename(fileSlice[i].Name(), newName)
				if err != nil {
					fmt.Println("Error renaming file:", err)
					return
				}
			}
			
			// Inform coordinator that the map task had been completed.
			commitArgs := CommitTaskArgs{taskId, "map"}
			commitReply := CommitTaskReply{}
			call("Coordinator.CommitTaskHandler", &commitArgs, &commitReply)

		} else if assignReply.Type == "reduce" {
			taskId := assignReply.Reduce.TaskId

			fmt.Println("reduce: ", taskId)

			// Open intermediate file and json decode slice.
			var fileSlice []*os.File

			// Get the current directory
			currentDir, err := os.Getwd()
			if err != nil {
				fmt.Println("Error getting current directory:", err)
				return
			}

			// Use regular expressions to match filenames
			// mr-X-0 regex
			pattern := `mr-\d+-` + strconv.Itoa(taskId)
			re := regexp.MustCompile(pattern)

			// The Walk() traverses the current directory
			err = filepath.Walk(currentDir, func(path string, info os.FileInfo, err error) error {
				if err != nil {
					return err
				}

				// Check if the filenames match
				if info.Mode().IsRegular() && re.MatchString(info.Name()) {
					// Open the file and add it to fileSlice
					file, err := os.Open(path)
					if err != nil {
						fmt.Println("Error opening file:", err)
						// Continue to the next file
						return nil
					}
					fileSlice = append(fileSlice, file)
				}
				return nil
			})

			if err != nil {
				fmt.Println("Error walking the directory:", err)
				return
			}

			// Accumulate the intermediate Map output.
			intermediate := []KeyValue{}

			for _, file := range fileSlice {
				dec := json.NewDecoder(file)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					intermediate = append(intermediate, kv)
				}
				// Close the file with defer, 
				// making sure to close when the function returns.
				defer file.Close()
			}

			sort.Sort(ByKey(intermediate))

			oname := "mr-out-" + strconv.Itoa(taskId)
			ofile, _ := os.Create(oname)
			defer ofile.Close()

			//
			// call Reduce on each distinct key in intermediate[],
			// and print the result to mr-out-taskId.
			//
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

			// Inform coordinator that the reduce task had been completed.
			commitArgs := CommitTaskArgs{taskId, "reduce"}
			commitReply := CommitTaskReply{}
			call("Coordinator.CommitTaskHandler", &commitArgs, &commitReply)

		} else {
			break
		}

		assignArgs = AssignTaskArgs{}
		assignReply = AssignTaskReply{}
		ok = call("Coordinator.AssignTaskHandler", &assignArgs, &assignReply)
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
