package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	// states of all the workers []
	//  - idle, -completed, -in-progress
	// Location of the intermediary files or keys
	// WorkerStates []WorkerState
	numMapTasks          int
	numReduceTasks       int
	allFiles             []string
	completed            bool
	workers              []WorkerState
	mu                   sync.Mutex
	uncompletedTasks     []CoordinatorTaskReply
	currentFileIndex     int
	currentWorkerID      int
	availableReduceTasks []int
}

type WorkerState int

const (
	IDLE WorkerState = iota
	IN_PROGRESS
	COMPLETED
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
	if len(c.availableReduceTasks) > 0 {
		return true, nil
	}
	return false, nil
}

// This functions checks the state of the worker after 10 seconds
// It returns true if the worker has successfully completed the task
func (c *Coordinator) checkWorkerCompletion(assignedTask CoordinatorTaskReply, wokerID int) {
	// sleep for ten seconds
	time.Sleep(10 * time.Second)
	if c.workers[wokerID] == COMPLETED {
		fmt.Printf("[Coordinator] Worker %d has completed the task\n", wokerID)
		// Woker available to do the next task
		c.workers[wokerID] = IDLE
		return
	}
	// If the worker has not completed the task yet
	// push the task to the list of uncompleted tasks
	fmt.Printf("[Coordinator] Worker %d has not completed the task\n", wokerID)
	c.mu.Lock()
	defer c.mu.Unlock()
	c.uncompletedTasks = append(c.uncompletedTasks, assignedTask)
}

func (c *Coordinator) assignMapTask(args *CoordinatorTaskArgs, reply *CoordinatorTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Check if there are any uncompleted tasks and assign to the current worker
	if len(c.uncompletedTasks) > 0 {
		*reply = c.uncompletedTasks[0]
		c.uncompletedTasks = c.uncompletedTasks[1:]
		reply.MapTaskID = c.currentWorkerID
		go c.checkWorkerCompletion(*reply, c.currentWorkerID)
		return nil
	}
	reply.TaskType = "Map"
	reply.InputFiles = append(reply.InputFiles, c.allFiles[c.currentFileIndex])
	reply.NReduce = c.numReduceTasks
	reply.MapTaskID = c.currentWorkerID
	c.workers[c.currentFileIndex] = IN_PROGRESS
	// reply.AllTasksCompleted = false

	c.currentWorkerID += 1
	go c.checkWorkerCompletion(*reply, c.currentFileIndex)
	c.currentFileIndex++

	return nil
}

func (c *Coordinator) assignReduceTask(args *CoordinatorTaskArgs, reply *CoordinatorTaskReply) error {
	return nil
}

// The worker calls this task when the task has been completed
func (c *Coordinator) InformCompletion(arg *CoordinatorTaskArgs, reply *CoordinatorTaskReply) error {
	fmt.Printf("[Coordinator] Worker %d has completed the task\n", arg.AssignedID)
	c.workers[arg.AssignedID] = COMPLETED
	fmt.Println("[Coordinator] Worker states", c.workers)
	// Reduce task are now available for these keys
	c.availableReduceTasks = append(c.availableReduceTasks, arg.AssignedID)
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

	flag, nil := c.canAssignReduceTask()
	if flag {

		return nil
	}

	err := c.assignMapTask(args, reply)
	if err != nil {
		fmt.Println("[Coordinator] Error in assigning map task")
		return err
	}

	if c.currentFileIndex == len(c.allFiles) {
		reply.AllTasksCompleted = true
		c.completed = true
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
	fmt.Println("[Coordinator] Tasks Completed: ", c.completed)

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.numReduceTasks = nReduce
	c.allFiles = files
	c.completed = false
	c.currentFileIndex = 0
	c.currentWorkerID = 0
	// Your code here.

	for i := 0; i < len(files); i++ {
		c.workers = append(c.workers, IDLE)
	}

	c.server()
	return &c
}
