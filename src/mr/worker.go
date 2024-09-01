package mr

import (
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strings"
)

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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

func readIntermediateFiles(filenames []string) ([]KeyValue, error) {
	keyValueArray := []KeyValue{}
	for _, filename := range filenames {
		file, err := os.ReadFile(filename)
		if err != nil {
			return nil, err
		}
		lines := strings.Split(string(file), "\n")
		for _, line := range lines {
			fields := strings.Split(line, " ")
			if len(fields) != 2 {
				return nil, fmt.Errorf("Intermediate key does not have 2 fields in the file %s", filename)
			}
			keyValueArray = append(keyValueArray, KeyValue{Key: fields[0], Value: fields[1]})
		}
	}
	return keyValueArray, nil
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	args := CoordinatorTaskArgs{}

	reply := CoordinatorTaskReply{}

	ok := call("Coordinator.AssignTask", &args, &reply)
	fmt.Println("[Worker] Asking Tasks from the Coordinator")
	// TODO:
	// Add the case for a reduce task as well
	// Clean the map code
	if ok {
		if reply.TaskType == "Map" {
			fmt.Println("[Worker] MAP Task received for file: ", reply)

			intermediateKeysMap := make(map[int][]KeyValue)

			// read the contents of the file
			content, err := ioutil.ReadFile(reply.InputFiles[0])
			if err != nil {
				log.Fatalf("cannot read %v", reply.InputFiles[0])
			}

			intermediateKeys := mapf(reply.InputFiles[0], string(content))
			fmt.Println("[Worker] First three intermediate keys: ", intermediateKeys[:3])
			for i := 0; i < len(intermediateKeys); i++ {
				hashValue := ihash(intermediateKeys[i].Key) % reply.NReduce
				intermediateKeysMap[hashValue] = append(intermediateKeysMap[hashValue], intermediateKeys[i])
			}

			// save the intermediate keys in files with the naming convention
			// mr-x-y
			// where x is mapTaskID and y is the hashValue
			count := 0
			for i := 0; i < len(intermediateKeysMap); i++ {
				count += 1

				sort.Sort(ByKey(intermediateKeysMap[i]))
				fileName := fmt.Sprintf("mr-%v-%v", reply.MapTaskID, i)
				file, err := os.Create(fileName)
				if err != nil {
					log.Fatalf("Cannot create %v", fileName)
				}

				for j := 0; j < len(intermediateKeysMap[i]); j++ {
					file.Write([]byte(intermediateKeysMap[i][j].Key + " " + intermediateKeysMap[i][j].Value + "\n"))
				}

			}
			args.AssignedID = reply.MapTaskID
			call("Coordinator.InformCompletion", &args, &reply)
			fmt.Println("[Worker] Number of intermediate files created: ", count)
		} else {
			fmt.Println("[Worker] Assign Task Recieved for files: ", reply.InputFiles)
			intermediateKeys := readIntermediateFile(reply.InputFiles)
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
		}
		// uncomment to send the Example RPC to the coordinator.
		// CallExample()

	}
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
// func CallExample() {

// 	// declare an argument structure.
// 	args := ExampleArgs{}

// 	// fill in the argument(s).
// 	args.X = 99

// 	// declare a reply structure.
// 	reply := ExampleReply{}

// 	// send the RPC request, wait for the reply.
// 	// the "Coordinator.Example" tells the
// 	// receiving server that we'd like to call
// 	// the Example() method of struct Coordinator.
// 	ok := call("Coordinator.Example", &args, &reply)
// 	if ok {
// 		// reply.Y should be 100.
// 		fmt.Printf("reply.Y %v\n", reply.Y)
// 	} else {
// 		fmt.Printf("call failed!\n")
// 	}
// }

// func GetTaskFromCoordinator() {
// 	args := CoordinatorTaskArgs{}

// 	reply := CoordinatorTaskReply{}

// 	ok := call("Coordinator.AssignTask", &args, &reply)
// 	if ok {
// 		if reply.TaskType == "Map" {

// 		}
// 	}
// }

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
