package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "sync"
import "time"

type TaskInfo struct {
	filename string
	done bool
	assigned bool
}

type Coordinator struct {
	// Your definitions here.

	mapTasks map[int]TaskInfo
	mapAllAssigned bool
	mapDone bool
	mapDoneCond sync.Cond
	mapMu sync.Mutex

	reduceTasks map[int]TaskInfo
	reduceAllAssigned bool
	reduceDone bool
	reduceDoneCond sync.Cond
	reduceMu sync.Mutex

	nReduce int
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) CommitTaskHandler(args *CommitTaskArgs, reply *CommitTaskReply) error {
	if args.Type == "map" {
		c.mapMu.Lock()
		value := c.mapTasks[args.TaskId]
		value.done = true
		c.mapTasks[args.TaskId] = value
		c.updateMapMeta()
		c.mapDoneCond.Broadcast()
		c.mapMu.Unlock()
	} else if args.Type == "reduce" {
		c.reduceMu.Lock()
		value := c.reduceTasks[args.TaskId]
		value.done = true
		c.reduceTasks[args.TaskId] = value
		c.updateReduceMeta()
		c.reduceDoneCond.Broadcast()
		c.reduceMu.Unlock()
	} 
	return nil
}

func (c *Coordinator) AssignTaskHandler(args *AssignTaskArgs, reply *AssignTaskReply) error {
	reply.NReduce = c.nReduce
	// There are unassigned map tasks.
	c.mapMu.Lock()
	if !c.mapAllAssigned {
		for key, value := range c.mapTasks {
			if (value.assigned) {
				continue
			}

			reply.Map.TaskId = key
			reply.Map.FileName = value.filename
			reply.Type = "map"

			value.assigned = true
			c.mapTasks[key] = value

			c.updateMapMeta()
			break
		}

		go func(taskId int) {
			time.Sleep(time.Second * 10)
			c.mapMu.Lock()
			if !c.mapTasks[taskId].done {
				value := c.mapTasks[taskId]
				value.assigned = false
				c.mapTasks[taskId] = value

				c.updateMapMeta()
				c.mapDoneCond.Broadcast()
			}
			c.mapMu.Unlock()
		} (reply.Map.TaskId)

		c.mapMu.Unlock()
		return nil
	}

	// There are no unassigned map tasks, but there are still map tasks to complete.
	for !c.mapDone {
		c.mapDoneCond.Wait()
		if !c.mapAllAssigned {
			return c.AssignTaskHandler(args, reply)
		}
	}
	c.mapMu.Unlock()

	// 
	// All map tasks are done, and there are unassigned reduce tasks.
	// 
	c.reduceMu.Lock()
	if !c.reduceAllAssigned {
		for key, value := range c.reduceTasks {
			if (value.assigned) {
				continue
			}

			reply.Reduce.TaskId = key
			reply.Type = "reduce"

			value.assigned = true
			c.reduceTasks[key] = value

			c.updateReduceMeta()
			break
		}

		go func(taskId int) {
			time.Sleep(time.Second * 10)
			c.reduceMu.Lock()
			if !c.reduceTasks[taskId].done {
				value := c.reduceTasks[taskId]
				value.assigned = false
				c.reduceTasks[taskId] = value

				c.updateReduceMeta()
				c.reduceDoneCond.Broadcast()
			}
			c.reduceMu.Unlock()
		} (reply.Reduce.TaskId)

		c.reduceMu.Unlock()
		return nil
	}

	// There are no unassigned reduce tasks, but there are still reduce tasks to complete.
	for !c.reduceDone {
		c.reduceDoneCond.Wait()
		if !c.reduceAllAssigned {
			return c.AssignTaskHandler(args, reply)
		}
	}
	c.reduceMu.Unlock()
	
	// All tasks had been completed.
	reply.Type = "none"
	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) updateMapMeta() {
	c.mapDone = true
	c.mapAllAssigned = true
	for _, value := range c.mapTasks {
		if (!value.done) {
			c.mapDone = false
		}
		if (!value.assigned) {
			c.mapAllAssigned = false
		}
	}
}

func (c *Coordinator) updateReduceMeta() {
	c.reduceDone = true
	c.reduceAllAssigned = true
	for _, value := range c.reduceTasks {
		if (!value.done) {
			c.reduceDone = false
		}
		if (!value.assigned) {
			c.reduceAllAssigned = false
		}
	}
}

//
// start a thread that listens for RPCs from worker.go
//
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

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	c.reduceMu.Lock()
	if c.reduceDone {
		ret = true
	}
	c.reduceMu.Unlock()

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.nReduce = nReduce

	// Your code here.
	c.mapTasks = make(map[int]TaskInfo)
	for i, filename := range files {
		c.mapTasks[i] = TaskInfo{filename, false, false}
	}
	c.mapAllAssigned = false
	c.mapDone = false;

	c.reduceTasks = make(map[int]TaskInfo)
	for i := 0; i < nReduce; i++ {
		c.reduceTasks[i] = TaskInfo{"", false, false}
	}
	c.reduceAllAssigned = false
	c.reduceDone = false

	// Initialize condition variable
	c.mapDoneCond = *sync.NewCond(&c.mapMu)
    c.reduceDoneCond = *sync.NewCond(&c.reduceMu)

	c.server()
	return &c
}
