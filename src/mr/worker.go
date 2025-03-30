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
	"time"

	"6.5840/logger"
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
		lines = lines[:len(lines)-1]
		// fmt.Println("[Worker] lines in the intermediate file: ")
		// spew.Dump(lines)

		for _, line := range lines {
			fields := strings.Split(line, " ")
			// fmt.Printf("[Worker] Fields in the line: %v\n", fields)
			if len(fields) != 2 {

				return nil, fmt.Errorf("Intermediate key does not have 2 fields in the file %s", filename)
			}
			keyValueArray = append(keyValueArray, KeyValue{Key: fields[0], Value: fields[1]})
		}
	}
	// sort the key value array by keys
	sort.Sort(ByKey(keyValueArray))
	return keyValueArray, nil
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	logger.InitLogger(os.Stdout)
	logger.Log(logger.INFO, "Worker started")
	// Your worker implementation here.
	args := CoordinatorTaskArgs{}
	args.AssignedID = -1

	reply := CoordinatorTaskReply{}
	logger.Log(logger.DEBUG, " reply.AllTaskCompleted %t\n", reply.AllTasksCompleted)

	for !reply.AllTasksCompleted && !reply.Terminate {
		time.Sleep(100 * time.Millisecond)
		logger.Log(logger.INFO, "Asking Tasks from the Coordinator")
		ok := call("Coordinator.AssignTask", &args, &reply)

		if reply.Terminate {
			logger.Log(logger.INFO, "Coordinator has completed the tasks")
			return
		}

		if reply.TaskID == -1 {
			logger.Log(logger.INFO, "No task assigned. Sleeping for 10 seconds")
			time.Sleep(10 * time.Second)
			continue
		}

		// TODO:
		// Add the case for a reduce task as well
		// Clean the map code
		if ok {
			// Assign the worker a permanent ID
			logger.Dump(logger.DEBUG, "Reply Recieved ", reply)
			if args.AssignedID == -1 {
				args.AssignedID = reply.WorkerID
			}

			if reply.TaskType == "Map" {

				logger.Log(logger.INFO, "MAP Task received for file: %s", reply.InputFiles[0])

				args.MapFileID = reply.MapFileID
				intermediateKeysMap := make(map[int][]KeyValue)

				// read the contents of the file
				content, err := ioutil.ReadFile(reply.InputFiles[0])
				if err != nil {
					logger.Log(logger.ERROR, "cannot read %v", reply.InputFiles[0])
				}

				logger.Log(logger.INFO, "File read successfully. About to apply the map function")
				intermediateKeys := mapf(reply.InputFiles[0], string(content))
				logger.Log(logger.DEBUG, "First three intermediate keys for %d reduce buckets %s", reply.NReduce, intermediateKeys)
				for i := 0; i < len(intermediateKeys); i++ {
					hashValue := ihash(intermediateKeys[i].Key) % reply.NReduce
					intermediateKeysMap[hashValue] = append(intermediateKeysMap[hashValue], intermediateKeys[i])
				}
				logger.Log(logger.DEBUG, "Intermediate Keys Map %s", intermediateKeysMap)

				// save the intermediate keys in files with the naming convention
				// mr-x-y
				// where x is mapTaskID and y is the hashValue
				count := 0
				// for i := 0; i < len(intermediateKeysMap); i++ {
				for i := 0; i < reply.NReduce; i++ {
					count += 1
					sort.Sort(ByKey(intermediateKeysMap[i]))
					fileName := fmt.Sprintf("mr-%v-%v", reply.MapFileID, i)
					file, err := os.Create(fileName)
					if err != nil {
						logger.Log(logger.ERROR, "cannot create %v", fileName)
					}

					for j := 0; j < len(intermediateKeysMap[i]); j++ {
						file.Write([]byte(intermediateKeysMap[i][j].Key + " " + intermediateKeysMap[i][j].Value + "\n"))
					}

				}
				// args.AssignedID = reply.TaskID
				//TODO: make this a function
				logger.Log(logger.INFO, "Keys generated, informing the coordinator")
				args.TaskID = reply.TaskID
				args.TaskType = "Map"
				call("Coordinator.InformCompletion", &args, &reply)
				if reply.Terminate {
					logger.Log(logger.INFO, "Coordinator has asked to terminate%d.\n", args.AssignedID)
					return
				}
				logger.Log(logger.INFO, "Number of intermediate files created: %d", count)

			} else if reply.TaskType == "Reduce" {
				logger.Log(logger.INFO, "Reduce Task received for file: %s", reply.InputFiles)
				// get the reduce ID from the file name (the last integer in the mr-x-y file)
				reduceId := reply.InputFiles[0][len(reply.InputFiles[0])-1:]
				intermediatekva, err := readIntermediateFiles(reply.InputFiles)
				if err != nil {
					log.Fatalf("[Worker] Error in reading intermediate files: %v", err)
				}
				tempFile := "mr-tmp-" + reduceId
				// tempFile := "mr-tmp-" + fmt.Sprint(reply.MapFileID)
				ofile, _ := os.Create(tempFile)
				i := 0
				// reduceComplete := true
				for i < len(intermediatekva) {
					j := i + 1
					for j < len(intermediatekva) && intermediatekva[j].Key == intermediatekva[i].Key {
						j++
					}
					values := []string{}
					for k := i; k < j; k++ {
						values = append(values, intermediatekva[k].Value)
					}
					output := reducef(intermediatekva[i].Key, values)
					// if the output is empty, it means that the reduce function has crashed
					// break the loop and inform the coordinator
					// if output == "" {
					// 	reduceComplete = false
					// 	break
					// }

					// this is the correct format for each line of Reduce output.
					fmt.Fprintf(ofile, "%v %v\n", intermediatekva[i].Key, output)

					i = j
				}
				ofile.Close()

				// if !reduceComplete {
				// 	logger.Log(logger.INFO, "Reduce function has crashed. Informing the coordinator")
				// 	call("Coordinator.InformCrash", &args, &reply)
				// 	continue
				// }

				logger.Log(logger.INFO, "Final file generated, informing the coordinator\n")
				args.TaskID = reply.TaskID
				args.TaskType = "Reduce"
				call("Coordinator.InformCompletion", &args, &reply)
				if reply.Terminate {
					logger.Log(logger.INFO, " Coordinator has asked to terminate %d.\n", args.AssignedID)
					return

				}
				// rename the temp file to the final file
				finalFile := "mr-out-" + reduceId
				os.Rename(tempFile, finalFile)
			}

			// uncomment to send the Example RPC to the coordinator.
			// CallExample()

		}
		// time.Sleep(15 * time.Second)
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
