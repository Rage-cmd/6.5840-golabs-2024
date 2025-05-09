package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"time"

	"6.5840/logger"
)

type Task struct {
	Id    int
	Type  string
	Files []string
}
type Coordinator struct {
	// Your definitions here.
	// states of all the workers []
	//  - idle, -completed, -in-progress
	// Location of the intermediary files or keys
	// WorkerStates []WorkerState

	// Related to Map tasks
	numMapTasks         int
	mapCompleted        bool
	completedMapTaskIDs []int

	// Related to Reduce tasks
	numReduceTasks         int
	reduceCompleted        bool
	availableReduceTaskIDs []int
	completedReduceTaskIDs []int

	// Related to general tasks and workers
	allFiles         []string
	workers          []WorkerState
	mu               sync.Mutex
	uncompletedTasks []CoordinatorTaskReply
	currentFileIndex int
	currentWorkerID  int
	taskCounter      int
	availableTasks   []Task
}

type WorkerState int

const (
	IDLE WorkerState = iota
	IN_PROGRESS
	COMPLETED
	DEAD
)

// var currentFileIndex = 0

// TODO:
// complete the coordinator implementation
// worker rpc interactions with coordinator

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// func (c *Coordinator) canAssignReduceTask() (bool, error) {
// 	fmt.Println("[Coordinator] Checking if intermediate files are present")
// 	if len(c.availableReduceTaskIDs) > 0 {
// 		return true, nil
// 	}
// 	return false, nil
// }

// This functions checks the state of the worker after 10 seconds
// It returns true if the worker has successfully completed the task
func (c *Coordinator) checkWorkerCompletion(assignedTask Task, workerID int) {
	// sleep for ten seconds
	time.Sleep(10 * time.Second)
	// if c.workers[workerID] == IDLE {
	// 	return
	// }

	// if the assigned task id is in the list of completed tasks, then simply return
	if assignedTask.Type == "Map" && slices.Contains(c.completedMapTaskIDs, assignedTask.Id) {
		return
	}

	if assignedTask.Type == "Reduce" && slices.Contains(c.completedReduceTaskIDs, assignedTask.Id) {
		return
	}

	logger.Log(logger.INFO, "Checking worker completion")
	logger.Dump(logger.DEBUG, "Coordinator struct", c)

	// if c.workers[workerID] == COMPLETED {
	// 	fmt.Printf("[Coordinator] Worker %d has completed the task\n", workerID)
	// 	// Woker available to do the next task
	// 	c.mu.Lock()
	// 	c.workers[workerID] = IDLE
	// 	c.mu.Unlock()
	// 	return
	// }

	// If the worker has not completed the task yet
	// push the task to the list of uncompleted tasks
	logger.Log(logger.INFO, "Worker %d has not completed the task and is considered dead", workerID)
	c.mu.Lock()
	defer c.mu.Unlock()
	// c.availableTasks = append(c.availableTasks, assignedTask)
	// it can also happen that the worker has completed the first task and started a new one
	// in that case the first task will be considered incomplete and it will be reassigned
	c.workers[workerID] = DEAD
	c.availableTasks = append(c.availableTasks, assignedTask)
	logger.Log(logger.INFO, "A %s task has been added to the list of available tasks", assignedTask.Type)
}

func (c *Coordinator) assignMapTask(args *CoordinatorTaskArgs, reply *CoordinatorTaskReply, task Task) error {
	// c.mu.Lock()
	// defer c.mu.Unlock()

	// if c.currentFileIndex >= len(c.allFiles) {
	// 	c.mapCompleted = true
	// 	return nil
	// }

	reply.TaskType = task.Type
	reply.TaskID = task.Id
	// c.workers[c.currentFileIndex] = IN_PROGRESS

	// This is the case when the worker is assigned a fresh file that has never been assigned
	reply.MapFileID = task.Id
	if len(task.Files) == 0 {
		task.Files = append(task.Files, c.allFiles[c.currentFileIndex])
		reply.MapFileID = c.currentFileIndex
		c.currentFileIndex += 1
	}
	reply.InputFiles = task.Files

	// // reply.AllTasksCompleted = false

	if args.AssignedID == -1 {
		// c.currentWorkerID += 1
		args.AssignedID = len(c.workers)
		reply.WorkerID = args.AssignedID
		c.currentWorkerID = len(c.workers)
		c.workers = append(c.workers, IN_PROGRESS)
	}
	logger.Log(logger.INFO, " Map task assigned to worker: %d", args.AssignedID)
	go c.checkWorkerCompletion(task, args.AssignedID)

	return nil
}

func (c *Coordinator) assignReduceTask(args *CoordinatorTaskArgs, reply *CoordinatorTaskReply, task Task) error {
	// c.mu.Lock()
	// defer c.mu.Unlock()
	// create reply struct using task info
	reply.TaskID = task.Id
	reply.TaskType = task.Type
	// fileRegex := "mr-" + string(rune(task.Id)) + "-*"
	// reply.InputFiles = getListOfFiles("./", fileRegex)
	// task.Files = reply.InputFiles
	reply.InputFiles = task.Files

	// spew.Dump("[Coordinator] assigning tasks ", reply)
	logger.Dump(logger.DEBUG, "Reply struct Dump", reply)

	c.workers[c.currentWorkerID] = IN_PROGRESS
	go c.checkWorkerCompletion(task, c.currentWorkerID)
	if args.AssignedID == -1 {
		c.currentWorkerID += 1
	}
	return nil
}

func getListOfFiles(directory string, keysToFind string) []string {

	files, err := filepath.Glob(filepath.Join(directory, keysToFind))
	logger.Dump(logger.DEBUG, "Files in the directory", files)
	if err != nil {
		log.Fatal(err)
	}

	return files
}

// The worker calls this task when the task has been completed
func (c *Coordinator) InformCompletion(arg *CoordinatorTaskArgs, reply *CoordinatorTaskReply) error {
	if c.workers[arg.AssignedID] == DEAD {
		logger.Log(logger.INFO, "Telling the worker %d to terminate as it is dead.", arg.AssignedID)
		reply.Terminate = true
		return nil
	}

	logger.Log(logger.INFO, "Worker %d has completed the task", arg.AssignedID)
	c.mu.Lock()

	c.workers[arg.AssignedID] = IDLE
	logger.Dump(logger.DEBUG, "Worker states", c.workers)

	// fileRegex := "mr-" + fmt.Sprint(arg.MapFileID) + "-*"
	// inputFiles := getListOfFiles("./", fileRegex)

	// Reduce task are now available for these keys
	// c.availableReduceTaskIDs = append(c.availableReduceTaskIDs, arg.AssignedID)
	if arg.TaskType == "Map" {
		c.completedMapTaskIDs = append(c.completedMapTaskIDs, arg.TaskID)

		// Once the MAP tasks are completed, create all the REDUCE tasks
		if len(c.completedMapTaskIDs) == len(c.allFiles) {
			for i := 0; i < c.numReduceTasks; i++ {
				// Only want mr-<int>-<int> files, do now want mr-out-<int> files
				fileRegex := "mr-*-" + fmt.Sprint(i)
				inputFiles := getListOfFiles("./", fileRegex)
				newReduceTask := Task{
					Id:    c.taskCounter,
					Type:  "Reduce",
					Files: inputFiles,
				}
				c.taskCounter += 1
				c.availableTasks = append(c.availableTasks, newReduceTask)
			}
		}

		// c.taskCounter += 1
		// c.availableTasks = append(c.availableTasks, newReduceTask)
	} else {
		c.completedReduceTaskIDs = append(c.completedReduceTaskIDs, arg.TaskID)
		// remove the taskID from the available tasks
		// indexOfTask := -1
		// for i, task := range c.availableTasks {
		// 	if task.Id == arg.TaskID {
		// 		indexOfTask = i
		// 		break
		// 	}
		// }
		// if indexOfTask != -1 {
		// 	c.availableTasks = append(c.availableTasks[:indexOfTask], c.availableTasks[indexOfTask+1:]...)
		// }

		// create a final reduce task and add it to available tasks
		// if len(c.completedReduceTaskIDs) == len(c.allFiles) {
		// 	c.taskCounter += 1
		// 	fileRegex := "mr-out-*"
		// 	inputFiles := getListOfFiles("./", fileRegex)
		// 	finalReduceTask := Task{
		// 		Id:    c.taskCounter,
		// 		Type:  "Reduce",
		// 		Files: inputFiles,
		// 	}
		// 	c.availableTasks = append(c.availableTasks, finalReduceTask)
		// }
	}
	c.mu.Unlock()

	return nil
}

func (c *Coordinator) InformCrash(arg *CoordinatorTaskArgs, reply *CoordinatorTaskReply) error {
	if c.workers[arg.AssignedID] == DEAD {
		logger.Log(logger.INFO, "Worker %d said it crashed, terminating it.", arg.AssignedID)
		reply.Terminate = true
		return nil
	}

	logger.Log(logger.INFO, "Worker %d has NOT completed the task.", arg.AssignedID)
	c.mu.Lock()

	c.workers[arg.AssignedID] = IDLE
	crashedTask := Task{
		Id:    reply.TaskID,
		Type:  reply.TaskType,
		Files: reply.InputFiles,
	}
	c.availableTasks = append(c.availableTasks, crashedTask)
	c.mu.Unlock()

	return nil
}

// This function assigns taks to a worker
// Depending upon the type of the task,
// it returns relevant input file and task type
func (c *Coordinator) AssignTask(args *CoordinatorTaskArgs, reply *CoordinatorTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	// TODO:
	// Make reduce task wait until a map task has been completed
	logger.Log(logger.INFO, "Attempting to assign tasks to the worker")
	logger.Dump(logger.DEBUG, "Coordinator struct before assigning a task", c)

	reply.AllTasksCompleted = false
	reply.Terminate = false

	if args.AssignedID == -1 {
		// c.currentWorkerID += 1
		args.AssignedID = len(c.workers)
		reply.NReduce = c.numReduceTasks
		reply.WorkerID = args.AssignedID
		c.currentWorkerID = len(c.workers)
		c.workers = append(c.workers, IN_PROGRESS)
	}

	logger.Log(logger.INFO, "Available tasks: %v", c.availableTasks)

	if len(c.availableTasks) > 0 {
		task := c.availableTasks[0]
		c.availableTasks = c.availableTasks[1:]
		if task.Type == "Reduce" {
			logger.Log(logger.INFO, "Assigning Reduce task to the worker")
			err := c.assignReduceTask(args, reply, task)
			if err != nil {
				// log.Fatalf("[Coordinator] Error in assigning reduce task")
				logger.Log(logger.ERROR, "Error in assigning reduce task: %s", err)
				return err
			}
			// err = c.assignReduceTask(args, reply, task)
			return nil
		}
		// If the execution reaches here, it means that the available tasks is
		// if of the type "Map"
		err := c.assignMapTask(args, reply, task)
		if err != nil {
			logger.Log(logger.ERROR, "Error in assigning map task: %s", err)
			return err
		}
		return nil
	}

	if c.currentFileIndex < len(c.allFiles) {
		// This means that there are available Map Tasks
		task := Task{
			Id:    c.taskCounter,
			Type:  "Map",
			Files: []string{},
		}
		c.taskCounter += 1

		err := c.assignMapTask(args, reply, task)
		if err != nil {
			logger.Log(logger.ERROR, "Error in assigning map task: %s", err)
			return err
		}
		logger.Log(logger.INFO, "MAP task assigned")
		return nil
	}

	reply.TaskID = -1
	if len(c.completedMapTaskIDs) >= len(c.allFiles) {
		c.mapCompleted = true
	}

	if len(c.completedReduceTaskIDs) >= c.numReduceTasks {
		c.reduceCompleted = true
	}

	if c.mapCompleted && c.reduceCompleted {
		reply.AllTasksCompleted = true
		reply.Terminate = true
	}

	return nil
}

func (c *Coordinator) CloseCoordinator(args *CoordinatorTaskArgs, reply *CoordinatorTaskReply) {

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
	// ret := c.completed``
	ret := c.mapCompleted && c.reduceCompleted
	// Your code here.
	logger.Log(logger.INFO, "Tasks Completed: %t", ret)

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.numReduceTasks = nReduce
	c.allFiles = files
	c.mapCompleted = false
	c.reduceCompleted = false
	c.currentFileIndex = 0
	c.currentWorkerID = 0
	c.availableTasks = []Task{}
	c.completedMapTaskIDs = []int{}
	c.completedReduceTaskIDs = []int{}
	c.taskCounter = 0
	logger.InitLogger(os.Stdout)

	// Your code here.

	// Doubtful as the number of workers need not equal
	// the number of input files????
	// what was I thinking?
	// for i := 0; i < len(files); i++ {
	// 	c.workers = append(c.workers, IDLE)
	// }

	c.server()
	return &c
}
