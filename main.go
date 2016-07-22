package main

import (
	"container/list"
	"flag"
	"fmt"
	"github.com/golang/protobuf/proto"
	mesos "github.com/mesos/mesos-go/mesosproto"
	"github.com/mesos/mesos-go/mesosutil"
	sched "github.com/mesos/mesos-go/scheduler"
	"log"
	"os"
	"os/signal"
	"time"
)

const (
	taskCPUs        = 0.1
	taskMem         = 32.0
	shutdownTimeout = time.Duration(1) * time.Second
)

var (
	defaultFilter = &mesos.Filters{RefuseSeconds: proto.Float64(1)}
)

func checkOffer(task Task, offer *mesos.Offer) bool {
	var cpusum, memsum float64
	for _, resource := range offer.Resources {
		switch resource.GetName() {
		case "cpus":
			cpusum += *resource.GetScalar().Value
		case "mem":
			memsum += *resource.GetScalar().Value
		}
	}
	portsResources := mesosutil.FilterResources(offer.Resources, func(res *mesos.Resource) bool {
		return res.GetName() == "ports"
	})

	var ports uint64
	for _, res := range portsResources {
		portRanges := res.GetRanges().GetRange()
		for _, portRange := range portRanges {
			ports += portRange.GetEnd() - portRange.GetBegin()
			log.Println(ports)
		}
	}

	for cpusum >= task.cpu && memsum > task.mem {
		return true
	}
	return false
}

func NameFor(state *mesos.TaskState) string {
	switch *state {
	case mesos.TaskState_TASK_STAGING:
		return "TASK_STAGING"
	case mesos.TaskState_TASK_STARTING:
		return "TASK_STARTING"
	case mesos.TaskState_TASK_RUNNING:
		return "TASK_RUNNING"
	case mesos.TaskState_TASK_FINISHED:
		return "TASK_FINISHED" // TERMINAL
	case mesos.TaskState_TASK_FAILED:
		return "TASK_FAILED" // TERMINAL
	case mesos.TaskState_TASK_KILLED:
		return "TASK_KILLED" // TERMINAL
	case mesos.TaskState_TASK_LOST:
		return "TASK_LOST" // TERMINAL
	default:
		return "UNKNOWN"
	}
}

// IsTerminal determines if a TaskState is a terminal state, i.e. if it singals
// that the task has stopped running.
func IsTerminal(state *mesos.TaskState) bool {
	switch *state {
	case mesos.TaskState_TASK_FINISHED,
		mesos.TaskState_TASK_FAILED,
		mesos.TaskState_TASK_KILLED,
		mesos.TaskState_TASK_LOST:
		return true
	default:
		return false
	}
}

type svcExecutor struct {
}

func (e *svcExecutor) Registered(driver exec.ExecutorDriver, execInfo *mesos.ExecutorInfo, fwinfo *mesos.FrameworkInfo, slaveInfo *mesos.SlaveInfo) {
	fmt.Println("Registered Executor on slave ", slaveInfo.GetHostname())
}

func (e *svcExecutor) Reregistered(driver exec.ExecutorDriver, slaveInfo *mesos.SlaveInfo) {
	fmt.Println("Re-registered Executor on slave ", slaveInfo.GetHostname())
}

func (e *svcExecutor) Disconnected(exec.ExecutorDriver) {
	fmt.Println("Executor disconnected.")
}

func (e *svcExecutor) LaunchTask(driver exec.ExecutorDriver, taskInfo *mesos.TaskInfo) {
	fmt.Println("Launching task", taskInfo.GetName(), "with command", taskInfo.Command.GetValue())

	runStatus := &mesos.TaskStatus{
		TaskId: taskInfo.GetTaskId(),
		State:  mesos.TaskState_TASK_STARTING.Enum(),
	}
	_, err := driver.SendStatusUpdate(runStatus)
	if err != nil {
		fmt.Println("Got error", err)
	}
}

func (e *svcExecutor) KillTask(exec.ExecutorDriver, *mesos.TaskID) {
	fmt.Println("Kill task")
}

func (e *svcExecutor) FrameworkMessage(driver exec.ExecutorDriver, msg string) {
	fmt.Println("Got framework message: ", msg)
}

func (e *svcExecutor) Shutdown(exec.ExecutorDriver) {
	fmt.Println("Shutting down the executor")
}

func (e *svcExecutor) Error(driver exec.ExecutorDriver, err string) {
	fmt.Println("Got error message:", err)
}

func sendTaskStatusUpdate(driver exec.ExecutorDriver, taskId string, state mesos.TaskState) (mesos.Status, error) {
	taskStatus := mesos.TaskStatus{
		TaskId: &mesos.TaskID{Value: proto.String(taskId)},
		State:  &state,
	}
	driverStatus, err := driver.SendStatusUpdate(&taskStatus)
	if err != nil {
		fmt.Printf("Send task status error, driverStatus: %v, err: %v\n", driverStatus.String(), err)
	}
	return driverStatus, err
}

type svcScheduler struct {
	tasksCreated int
	tasksRunning int

	taskQueue *list.List

	//executor *mesos.ExecutorInfo

	// This channel is close when the program receives an interrupt,
	// signalling that the program should shut down
	shutdown chan struct{}
	// This channel is close after shutdown is closed, and only when all
	// outstanding tasks have been cleanned up
	done chan struct{}
}

type Task struct {
	cpu   float64
	mem   float64
	image string
}

func newSvcScheduler() *svcScheduler {
	s := &svcScheduler{
		taskQueue: list.New(),

		//executor: &mesos.ExecutorInfo{
		//	ExecutorId: &mesos.ExecutorID{
		//		Value: proto.String("test-executor"),
		//	},
		//	Command: &mesos.CommandInfo{
		//		Value: proto.String("mkdir a"),
		//	},
		//	Name: proto.String("svc"),
		//},

		shutdown: make(chan struct{}),
		done:     make(chan struct{}),
	}
	return s
}

func (s *svcScheduler) newSvcTask(task Task, offer *mesos.Offer) *mesos.TaskInfo {
	var taskID *mesos.TaskID
	var taskInfo *mesos.TaskInfo

	s.tasksCreated++
	taskID = &mesos.TaskID{
		Value: proto.String(fmt.Sprintf("svc-%d", s.tasksCreated)),
	}

	containerType := mesos.ContainerInfo_DOCKER
	network := mesos.ContainerInfo_DockerInfo_BRIDGE
	portmappings := []*mesos.ContainerInfo_DockerInfo_PortMapping{
		&mesos.ContainerInfo_DockerInfo_PortMapping{
			HostPort:      proto.Uint32(31004),
			ContainerPort: proto.Uint32(80),
			Protocol:      proto.String("tcp"),
		},
	}
	taskInfo = &mesos.TaskInfo{
		Name:    proto.String("task-" + taskID.GetValue()),
		TaskId:  taskID,
		SlaveId: offer.SlaveId,
		Container: &mesos.ContainerInfo{
			Type: &containerType,
			Docker: &mesos.ContainerInfo_DockerInfo{
				Image:        proto.String(task.image),
				Network:      &network,
				PortMappings: portmappings,
			},
		},
		Command: &mesos.CommandInfo{
			Shell: proto.Bool(false),
		},
		Resources: []*mesos.Resource{
			mesosutil.NewScalarResource("cpus", task.cpu),
			mesosutil.NewScalarResource("mem", task.mem),
		},
	}
	return taskInfo
}

func (s *svcScheduler) Registered(
	_ sched.SchedulerDriver,
	frameworkID *mesos.FrameworkID,
	masterInfo *mesos.MasterInfo) {
	log.Printf("Framework %s registered with master %s", frameworkID, masterInfo)
}

func (s *svcScheduler) Reregistered(
	_ sched.SchedulerDriver,
	masterInfo *mesos.MasterInfo) {
	log.Printf("Framework re-registered with master %s", masterInfo)
}

func (s *svcScheduler) Disconnected(sched.SchedulerDriver) {
	log.Println("Framework disconnected with master")
}

func (s *svcScheduler) ResourceOffers(driver sched.SchedulerDriver, offers []*mesos.Offer) {
	log.Printf("Received %d resource offers", len(offers))
	for _, offer := range offers {
		select {
		case <-s.shutdown:
			log.Println("Shutting down: declining offer on [", offer.Hostname, "]")
			driver.DeclineOffer(offer.Id, defaultFilter)
			if s.tasksRunning == 0 {
				close(s.done)
			}
			continue
		default:
		}

		tasks := []*mesos.TaskInfo{}
		task := s.taskQueue.Front()
		if task != nil {
			if checkOffer(task.Value.(Task), offer) {
				taskInfo := s.newSvcTask(task.Value.(Task), offer)
				tasks = append(tasks, taskInfo)
				s.taskQueue.Remove(task)
			}
		}

		if len(tasks) == 0 {
			driver.DeclineOffer(offer.Id, defaultFilter)
		} else {
			driver.LaunchTasks([]*mesos.OfferID{offer.Id}, tasks, defaultFilter)
		}
	}
}

func (s *svcScheduler) StatusUpdate(driver sched.SchedulerDriver, status *mesos.TaskStatus) {
	log.Printf("Received task status [%s] for task [%s] for [%s]", NameFor(status.State), *status.TaskId.Value, status.GetReason())

	if *status.State == mesos.TaskState_TASK_RUNNING {
		s.tasksRunning++
	} else if IsTerminal(status.State) {
		s.tasksRunning--
		if s.tasksRunning == 0 {
			select {
			case <-s.shutdown:
				close(s.done)
			default:
			}
		}
	}
}

func (s *svcScheduler) FrameworkMessage(
	driver sched.SchedulerDriver,
	executorID *mesos.ExecutorID,
	slaveID *mesos.SlaveID,
	message string) {

	log.Println("Getting a framework message")
	//switch *executorID.Value {
	//case *s.executor.ExecutorId.Value:
	//	log.Print("Received framework message from svc")
	//default:
	//	log.Printf("Received a framework message from some unknown source: %s", *executorID.Value)
	//}
}

func (s *svcScheduler) OfferRescinded(_ sched.SchedulerDriver, offerID *mesos.OfferID) {
	log.Printf("Offer %s rescined", offerID)
}

func (s *svcScheduler) SlaveLost(_ sched.SchedulerDriver, slaveID *mesos.SlaveID) {
	log.Printf("Slave %s lost", slaveID)
}

func (s *svcScheduler) ExecutorLost(_ sched.SchedulerDriver, executorID *mesos.ExecutorID, slaveID *mesos.SlaveID, status int) {
	log.Printf("Executor %s on slave %s was lost", executorID, slaveID)
}

func (s *svcScheduler) Error(_ sched.SchedulerDriver, err string) {
	log.Printf("Receiving an error: %s", err)
}

func main() {
	master := flag.String("master", "10.16.51.127:5050", "Location of leading Mesos master")

	flag.Parse()

	scheduler := newSvcScheduler()
	task := Task{
		cpu:   1.0,
		mem:   128,
		image: "nginx",
	}

	scheduler.taskQueue.PushBack(task)
	driver, err := sched.NewMesosSchedulerDriver(sched.DriverConfig{
		Master: *master,
		Framework: &mesos.FrameworkInfo{
			Name: proto.String("SVC"),
			User: proto.String(""),
		},
		Scheduler: scheduler,
	})

	if err != nil {
		log.Printf("Unable to create scheduler driver: %s", err)
		return
	}

	go func() {
		c := make(chan os.Signal, 1)
		signal.Notify(c, os.Interrupt, os.Kill)
		s := <-c
		if s != os.Interrupt {
			return
		}

		log.Println("SVC is shutting down")
		close(scheduler.shutdown)

		select {
		case <-scheduler.done:
		case <-time.After(shutdownTimeout):
		}

		driver.Stop(false)
	}()

	if status, err := driver.Run(); err != nil {
		log.Printf("Framework stopped with status %s and error: %s\n", status.String(), err.Error())
	}

	log.Println("Exiting...")
}
