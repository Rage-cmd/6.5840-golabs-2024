package mr

import (
	"fmt"
	"io/fs"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"slices"
	"sync"
	"time"

	"github.com/davecgh/go-spew/spew"
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

func (c *Coordinator) canAssignReduceTask() (bool, error) {
	fmt.Println("[Coordinator] Checking if intermediate files are present")
	if len(c.availableReduceTaskIDs) > 0 {
		return true, nil
	}
	return false, nil
}

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

	// ------- Logging for debugging-----------
	fmt.Printf("[Coordinator] Checking worker completion, Coordinator struct: \n")
	spew.Dump(c)
	// ----------------------------------------

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
	fmt.Printf("[Coordinator] Worker %d has not completed the task and is considered dead.\n", workerID)
	c.mu.Lock()
	defer c.mu.Unlock()
	// c.availableTasks = append(c.availableTasks, assignedTask)
	// it can also happen that the worker has completed the first task and started a new one
	// in that case the first task will be considered incomplete and it will be reassigned
	c.workers[workerID] = DEAD
	c.availableTasks = append(c.availableTasks, assignedTask)
}

func (c *Coordinator) assignMapTask(args *CoordinatorTaskArgs, reply *CoordinatorTaskReply, task Task) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.currentFileIndex >= len(c.allFiles) {
		c.mapCompleted = true
		return nil
	}

	reply.TaskType = task.Type
	reply.NReduce = c.numReduceTasks
	reply.TaskID = task.Id
	// c.workers[c.currentFileIndex] = IN_PROGRESS

	// This is the case when the worker is assigned a fresh file that has never been assigned
	if len(task.Files) == 0 {
		task.Files = append(task.Files, c.allFiles[c.currentFileIndex])
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
	fmt.Printf("[Coordinator] Map task assigned to worker: %d", args.AssignedID)
	// c.workers[]
	go c.checkWorkerCompletion(task, args.AssignedID)

	return nil
}

func (c *Coordinator) assignReduceTask(args *CoordinatorTaskArgs, reply *CoordinatorTaskReply, task Task) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	// create reply struct using task info
	reply.TaskID = task.Id
	reply.TaskType = task.Type
	fileRegex := "mr-" + string(rune(task.Id)) + "-*"
	reply.InputFiles = getListOfFiles("./", fileRegex)
	task.Files = reply.InputFiles
	c.workers[c.currentWorkerID] = IN_PROGRESS
	go c.checkWorkerCompletion(task, c.currentWorkerID)
	if args.AssignedID == -1 {
		c.currentWorkerID += 1
	}
	return nil
}

func getListOfFiles(directory string, keysToFind string) []string {
	root := os.DirFS(directory)

	files, err := fs.Glob(root, keysToFind)
	if err != nil {
		log.Fatal(err)
	}

	return files
}

// The worker calls this task when the task has been completed
func (c *Coordinator) InformCompletion(arg *CoordinatorTaskArgs, reply *CoordinatorTaskReply) error {
	if c.workers[arg.AssignedID] == DEAD {
		fmt.Printf("[Coordinator] Telling the worker %d to terminate as it is dead.\n", arg.AssignedID)
		reply.Terminate = true
		return nil
	}

	fmt.Printf("[Coordinator] Worker %d has completed the task\n", arg.AssignedID)
	c.mu.Lock()
	defer c.mu.Unlock()

	c.workers[arg.AssignedID] = IDLE
	fmt.Println("[Coordinator] Worker states", c.workers)
	inputFiles := []string{}
	for i := 0; i < c.numReduceTasks; i++ {
		inputFiles = append(inputFiles, "mr-"+string(rune(arg.AssignedID))+"-"+fmt.Sprint(i))
	}
	newReduceTask := Task{
		Id:    arg.AssignedID,
		Type:  "Reduce",
		Files: inputFiles,
	}
	// Reduce task are now available for these keys
	// c.availableReduceTaskIDs = append(c.availableReduceTaskIDs, arg.AssignedID)
	c.availableTasks = append(c.availableTasks, newReduceTask)
	if arg.TaskType == "Map" {
		c.completedMapTaskIDs = append(c.completedMapTaskIDs, arg.TaskID)
	} else {
		c.completedReduceTaskIDs = append(c.completedReduceTaskIDs, arg.TaskID)
	}

	return nil
}

// This function assigns taks to a worker
// Depending upon the type of the task,
// it returns relevant input file and task type
func (c *Coordinator) AssignTask(args *CoordinatorTaskArgs, reply *CoordinatorTaskReply) error {
	// TODO:
	// Make reduce task wait until a map task has been completed
	fmt.Println("[Coordinator] Coordinator struct: ", c)
	fmt.Println("[Coordinator] Assigning Tasks to the worker")

	if len(c.availableTasks) > 0 {
		task := c.availableTasks[0]
		c.availableTasks = c.availableTasks[1:]
		if task.Type == "Reduce" {
			err := c.assignReduceTask(args, reply, task)
			if err != nil {
				log.Fatalf("[Coordinator] Error in assigning reduce task")
				return err
			}
			err = c.assignReduceTask(args, reply, task)
			return err
		}
		// If the execution reaches here, it means that the available tasks is
		// if of the type "Map"
		err := c.assignMapTask(args, reply, task)
		if err != nil {
			log.Fatalf("[Coordinator] Error in assigning reduce task")
			return err
		}
		return nil
	}

	task := Task{
		Id:    c.currentWorkerID,
		Type:  "Map",
		Files: []string{},
	}
	err := c.assignMapTask(args, reply, task)
	if err != nil {
		fmt.Println("[Coordinator] Error in assigning map task")
		return err
	}

	if c.currentFileIndex == len(c.allFiles) {
		reply.AllTasksCompleted = true
		c.mapCompleted = true
	}
	fmt.Println("[Coordinator] MAP task assigned")
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
	// ret := c.completed
	ret := false
	// Your code here.
	fmt.Println("[Coordinator] Tasks Completed: ", c.mapCompleted || c.reduceCompleted)

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
