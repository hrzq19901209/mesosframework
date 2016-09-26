package main

import (
	"container/list"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/golang/protobuf/proto"
	mesos "github.com/mesos/mesos-go/mesosproto"
	"github.com/mesos/mesos-go/mesosutil"
	sched "github.com/mesos/mesos-go/scheduler"
	"log"
	_ "os"
	_ "os/signal"
	"time"
)

const (
	taskCPUs        = 0.1
	taskMem         = 32.0
	shutdownTimeout = time.Duration(1) * time.Second
	CMD             = "./sohu_docker_executor_back"
	ExecutorPath    = "/opt/haoran/tmpGoWork/src/github.com/executorserver/sohu_docker_executor_back"
)

var (
	defaultFilter = &mesos.Filters{RefuseSeconds: proto.Float64(1)}
)

func getOfferScalar(offer *mesos.Offer, name string) float64 {
	resources := mesosutil.FilterResources(offer.Resources, func(res *mesos.Resource) bool {
		return res.GetName() == name
	})

	value := 0.0
	for _, res := range resources {
		value += res.GetScalar().GetValue()
	}

	return value
}

func getOfferCpu(offer *mesos.Offer) float64 {
	return getOfferScalar(offer, "cpus")
}

func getOfferMem(offer *mesos.Offer) float64 {
	return getOfferScalar(offer, "mem")
}

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

	for cpusum >= task.Cpus && memsum > task.Mem {
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

type svcScheduler struct {
	tasksCreated int
	tasksRunning int

	taskQueue *list.List

	executor *mesos.ExecutorInfo

	// This channel is close when the program receives an interrupt,
	// signalling that the program should shut down
	shutdown chan struct{}
	// This channel is close after shutdown is closed, and only when all
	// outstanding tasks have been cleanned up
	done chan struct{}
}

type Task struct {
	Cpus          float64  `json:"cpus"`
	Mem           float64  `json:"mem"`
	Image         string   `json:"image"`
	ContainerName string   `json:"containerName"`
	Port          string   `json:"port"`
	Volume        []string `json:"volume"`
	NetworkMode   string   `json:"networkmode"`
	Cmd           []string `json:"cmd"`
}

func newSvcScheduler(exec *mesos.ExecutorInfo) *svcScheduler {
	s := &svcScheduler{
		taskQueue: list.New(),
		executor:  exec,
		shutdown:  make(chan struct{}),
		done:      make(chan struct{}),
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

	//containerType := mesos.ContainerInfo_DOCKER
	//network := mesos.ContainerInfo_DockerInfo_BRIDGE
	//portmappings := []*mesos.ContainerInfo_DockerInfo_PortMapping{
	//	&mesos.ContainerInfo_DockerInfo_PortMapping{
	//		HostPort:      proto.Uint32(31004),
	//		ContainerPort: proto.Uint32(80),
	//		Protocol:      proto.String("tcp"),
	//	},
	//}

	b, err := json.Marshal(task)
	fmt.Println(string(b))
	if err != nil {
		log.Printf("json marshal error: %s", err)
		panic(err)
	}

	taskInfo = &mesos.TaskInfo{
		Name:     proto.String("task-" + taskID.GetValue()),
		TaskId:   taskID,
		SlaveId:  offer.SlaveId,
		Executor: s.executor,
		//Container: &mesos.ContainerInfo{
		//	Type: &containerType,
		//	Docker: &mesos.ContainerInfo_DockerInfo{
		//		Image:        proto.String(task.image),
		//		Network:      &network,
		//		PortMappings: portmappings,
		//	},
		//},
		//Command: &mesos.CommandInfo{
		//	Shell: proto.Bool(false),
		//},
		Resources: []*mesos.Resource{
			mesosutil.NewScalarResource("cpus", task.Cpus),
			mesosutil.NewScalarResource("mem", task.Mem),
		},
		Data: b,
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
		remainingCpus := getOfferCpu(offer)
		remainingMem := getOfferMem(offer)

		log.Println("Cpus:", remainingCpus, "offerId:", offer.Id)

		for e := s.taskQueue.Front(); e != nil; {
			task := e.Value.(Task)
			if task.Cpus <= remainingCpus &&
				task.Mem <= remainingMem {
				taskInfo := s.newSvcTask(task, offer)
				tasks = append(tasks, taskInfo)
				remainingCpus -= task.Cpus
				remainingMem -= task.Mem
				t := e.Next()
				s.taskQueue.Remove(e)
				e = t
			} else {
				e = e.Next()
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

	executorUris := []*mesos.CommandInfo_URI{
		{
			Value:      proto.String(ExecutorPath),
			Executable: proto.Bool(true),
		},
	}

	executor := &mesos.ExecutorInfo{
		ExecutorId: mesosutil.NewExecutorID("default"),
		Name:       proto.String("Test Executor (Go)"),
		Source:     proto.String("go_test"),
		Command: &mesos.CommandInfo{
			Value: proto.String(CMD),
			Uris:  executorUris,
		},
	}

	scheduler := newSvcScheduler(executor)
	task := Task{
		Cpus:          2.0,
		Mem:           2048,
		Image:         "server",
		Port:          "31226",
		ContainerName: "",
		Volume:        []string{"/var/log/server:/var/log/server"},
		NetworkMode:   "host",
		Cmd:           []string{"--port=1314"},
	}
	scheduler.taskQueue.PushBack(task)

	driver, err := sched.NewMesosSchedulerDriver(sched.DriverConfig{
		Master: *master,
		Framework: &mesos.FrameworkInfo{
			Name:            proto.String("SVC"),
			User:            proto.String(""),
			Checkpoint:      proto.Bool(true),
			FailoverTimeout: proto.Float64(3600 * 24 * 7),

			Id: &mesos.FrameworkID{
				Value: proto.String("2ad17b28-1cb8-4039-a6b1-820d84455d98-0010"),
			},
		},
		Scheduler: scheduler,
	})

	if err != nil {
		log.Printf("Unable to create scheduler driver: %s", err)
		return
	}

	/*go func() {
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
	}()*/

	if status, err := driver.Run(); err != nil {
		log.Printf("Framework stopped with status %s and error: %s\n", status.String(), err.Error())
	}

	log.Println("Exiting...")
}
