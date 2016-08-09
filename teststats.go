package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/Sirupsen/logrus"
	"github.com/docker/engine-api/client"
	"github.com/docker/engine-api/types"
	"github.com/docker/engine-api/types/events"
	eventtypes "github.com/docker/engine-api/types/events"
	"github.com/docker/engine-api/types/filters"
	"github.com/docker/go-units"
	"golang.org/x/net/context"
	"io"
	"os"
	"strings"
	"sync"
	"text/tabwriter"
	"time"
)

// EventHandler is abstract interface for user to customize
// own handle functions of each type of events
type eventProcessor func(event eventtypes.Message, err error) error

type EventHandler interface {
	Handle(action string, h func(eventtypes.Message))
	Watch(c <-chan eventtypes.Message)
}

// InitEventHandler initializes and returns an EventHandler
func InitEventHandler() EventHandler {
	return &eventHandler{handlers: make(map[string]func(eventtypes.Message))}
}

type eventHandler struct {
	handlers map[string]func(eventtypes.Message)
	mu       sync.Mutex
}

func (w *eventHandler) Handle(action string, h func(eventtypes.Message)) {
	w.mu.Lock()
	w.handlers[action] = h
	w.mu.Unlock()
}

// Watch ranges over the passed in event chan and processes the events based on the
// handlers created for a given action.
// To stop watching, close the event chan.
func (w *eventHandler) Watch(c <-chan eventtypes.Message) {
	for e := range c {
		w.mu.Lock()
		h, exists := w.handlers[e.Action]
		w.mu.Unlock()
		if !exists {
			continue
		}
		logrus.Debugf("event handler: received event: %v", e)
		go h(e)
	}
}

// DecodeEvents decodes event from input stream
func DecodeEvents(input io.Reader, ep eventProcessor) error {
	dec := json.NewDecoder(input)
	for {
		var event eventtypes.Message
		err := dec.Decode(&event)
		if err != nil && err == io.EOF {
			break
		}

		if procErr := ep(event, err); procErr != nil {
			return procErr
		}
	}
	return nil
}

type containerStats struct {
	Name             string
	CPUPercentage    float64
	Memory           float64
	MemoryLimit      float64
	MemoryPercentage float64
	NetworkRx        float64
	NetworkTx        float64
	BlockRead        float64
	BlockWrite       float64
	PidsCurrent      uint64
	mu               sync.Mutex
	err              error
}

func calculateCPUPercent(previousCPU, previousSystem uint64, v *types.StatsJSON) float64 {
	var (
		cpuPercent = 0.0
		// calculate the change for the cpu usage of the container in between readings
		cpuDelta = float64(v.CPUStats.CPUUsage.TotalUsage) - float64(previousCPU)
		// calculate the change for the entire system between readings
		systemDelta = float64(v.CPUStats.SystemUsage) - float64(previousSystem)
	)

	if systemDelta > 0.0 && cpuDelta > 0.0 {
		cpuPercent = (cpuDelta / systemDelta) * float64(len(v.CPUStats.CPUUsage.PercpuUsage)) * 100.0
	}
	return cpuPercent
}

func calculateBlockIO(blkio types.BlkioStats) (blkRead uint64, blkWrite uint64) {
	for _, bioEntry := range blkio.IoServiceBytesRecursive {
		switch strings.ToLower(bioEntry.Op) {
		case "read":
			blkRead = blkRead + bioEntry.Value
		case "write":
			blkWrite = blkWrite + bioEntry.Value
		}
	}
	return
}

func calculateNetwork(network map[string]types.NetworkStats) (float64, float64) {
	var rx, tx float64

	for _, v := range network {
		rx += float64(v.RxBytes)
		tx += float64(v.TxBytes)
	}
	return rx, tx
}

func (s *containerStats) Collect(ctx context.Context, cli client.APIClient, streamStats bool, waitFirst *sync.WaitGroup) {
	logrus.Debugf("collecting stats for %s", s.Name)
	var (
		getFirst       bool
		previousCPU    uint64
		previousSystem uint64
		u              = make(chan error, 1)
	)

	defer func() {
		if !getFirst {
			getFirst = true
			waitFirst.Done()
		}
	}()

	responseBody, err := cli.ContainerStats(ctx, s.Name, streamStats)
	if err != nil {
		s.mu.Lock()
		s.err = err
		s.mu.Unlock()
		return
	}
	defer responseBody.Close()

	dec := json.NewDecoder(responseBody)
	go func() {
		for {
			var v *types.StatsJSON

			if err := dec.Decode(&v); err != nil {
				dec = json.NewDecoder(io.MultiReader(dec.Buffered(), responseBody))
				u <- err
				if err == io.EOF {
					break
				}
				time.Sleep(100 * time.Millisecond)
				continue
			}

			var memPercent = 0.0
			var cpuPercent = 0.0

			// MemoryStats.Limit will never be 0 unless the container is not running and we haven't
			// got any data from cgroup
			if v.MemoryStats.Limit != 0 {
				memPercent = float64(v.MemoryStats.Usage) / float64(v.MemoryStats.Limit) * 100.0
			}

			previousCPU = v.PreCPUStats.CPUUsage.TotalUsage
			previousSystem = v.PreCPUStats.SystemUsage
			cpuPercent = calculateCPUPercent(previousCPU, previousSystem, v)
			blkRead, blkWrite := calculateBlockIO(v.BlkioStats)
			s.mu.Lock()
			s.CPUPercentage = cpuPercent
			s.Memory = float64(v.MemoryStats.Usage)
			s.MemoryLimit = float64(v.MemoryStats.Limit)
			s.MemoryPercentage = memPercent
			s.NetworkRx, s.NetworkTx = calculateNetwork(v.Networks)
			s.BlockRead = float64(blkRead)
			s.BlockWrite = float64(blkWrite)
			s.PidsCurrent = v.PidsStats.Current
			s.mu.Unlock()
			u <- nil
			if !streamStats {
				return
			}
		}
	}()
	for {
		select {
		case <-time.After(2 * time.Second):
			// zero out the values if we have not received an update within
			// the specified duration.
			s.mu.Lock()
			s.CPUPercentage = 0
			s.Memory = 0
			s.MemoryPercentage = 0
			s.MemoryLimit = 0
			s.NetworkRx = 0
			s.NetworkTx = 0
			s.BlockRead = 0
			s.BlockWrite = 0
			s.PidsCurrent = 0
			s.err = errors.New("timeout waiting for stats")
			s.mu.Unlock()
			// if this is the first stat you get, release WaitGroup
			if !getFirst {
				getFirst = true
				waitFirst.Done()
			}
		case err := <-u:
			if err != nil {
				s.mu.Lock()
				s.err = err
				s.mu.Unlock()
				continue
			}
			s.err = nil
			// if this is the first stat you get, release WaitGroup
			if !getFirst {
				getFirst = true
				waitFirst.Done()
			}
		}
		if !streamStats {
			return
		}
	}
}

func (s *containerStats) Display(w io.Writer) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	// NOTE: if you change this format, you must also change the err format below!
	format := "%s\t%.2f%%\t%s / %s\t%.2f%%\t%s / %s\t%s / %s\t%d\n"
	if s.err != nil {
		format = "%s\t%s\t%s / %s\t%s\t%s / %s\t%s / %s\t%s\n"
		errStr := "--"
		fmt.Fprintf(w, format,
			s.Name, errStr, errStr, errStr, errStr, errStr, errStr, errStr, errStr, errStr,
		)
		err := s.err
		return err
	}
	fmt.Fprintf(w, format,
		s.Name,
		s.CPUPercentage,
		units.BytesSize(s.Memory), units.BytesSize(s.MemoryLimit),
		s.MemoryPercentage,
		units.HumanSize(s.NetworkRx), units.HumanSize(s.NetworkTx),
		units.HumanSize(s.BlockRead), units.HumanSize(s.BlockWrite),
		s.PidsCurrent)
	return nil
}

type stats struct {
	mu sync.Mutex
	cs []*containerStats
}

func (s *stats) add(cs *containerStats) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.isKnownContainer(cs.Name); !exists {
		s.cs = append(s.cs, cs)
		return true
	}
	return false
}

func (s *stats) remove(id string) {
	s.mu.Lock()
	if i, exists := s.isKnownContainer(id); exists {
		s.cs = append(s.cs[:i], s.cs[i+1:]...)
	}
	s.mu.Unlock()
}

func (s *stats) isKnownContainer(cid string) (int, bool) {
	for i, c := range s.cs {
		if c.Name == cid {
			return i, true
		}
	}
	return -1, false
}

type statsOptions struct {
	all      bool
	noStream bool

	containers []string
}

func main() {

	defaultHeaders := map[string]string{"User-Agent": "engine-api-cli-1.0"}
	dockerCli, err := client.NewClient("unix:///var/run/docker.sock", "v1.12", nil, defaultHeaders)
	if err != nil {
		panic(err)
	}

	showAll := true
	opts := statsOptions{
		all:      true,
		noStream: false,
	}

	closeChan := make(chan error)
	ctx := context.Background()
	monitorContainerEvents := func(started chan<- struct{}, c chan events.Message) {
		f := filters.NewArgs()
		f.Add("type", "container")
		options := types.EventsOptions{
			Filters: f,
		}
		resBody, err := dockerCli.Events(ctx, options)
		// Whether we successfully subscribed to events or not, we can now
		// unblock the main goroutine.
		close(started)
		if err != nil {
			closeChan <- err
			return
		}
		defer resBody.Close()

		DecodeEvents(resBody, func(event events.Message, err error) error {
			if err != nil {
				closeChan <- err
				return nil
			}
			c <- event
			return nil
		})
	}

	// waitFirst is a WaitGroup to wait first stat data's reach for each container
	waitFirst := &sync.WaitGroup{}

	cStats := stats{}
	// getContainerList simulates creation event for all previously existing
	// containers (only used when calling `docker stats` without arguments).
	getContainerList := func() {
		options := types.ContainerListOptions{
			All: opts.all,
		}
		cs, err := dockerCli.ContainerList(ctx, options)
		if err != nil {
			closeChan <- err
		}
		for _, container := range cs {
			s := &containerStats{Name: container.ID[:12]}
			if cStats.add(s) {
				waitFirst.Add(1)
				go s.Collect(ctx, dockerCli, !opts.noStream, waitFirst)
			}
		}
	}

	started := make(chan struct{})
	eh := InitEventHandler()
	eh.Handle("create", func(e events.Message) {
		if opts.all {
			s := &containerStats{Name: e.ID[:12]}
			if cStats.add(s) {
				waitFirst.Add(1)
				go s.Collect(ctx, dockerCli, !opts.noStream, waitFirst)
			}
		}
	})

	eh.Handle("start", func(e events.Message) {
		s := &containerStats{Name: e.ID[:12]}
		if cStats.add(s) {
			waitFirst.Add(1)
			go s.Collect(ctx, dockerCli, !opts.noStream, waitFirst)
		}
	})

	eh.Handle("die", func(e events.Message) {
		if !opts.all {
			cStats.remove(e.ID[:12])
		}
	})

	eventChan := make(chan events.Message)
	go eh.Watch(eventChan)
	go monitorContainerEvents(started, eventChan)
	defer close(eventChan)
	<-started

	// Start a short-lived goroutine to retrieve the initial list of
	// containers.
	getContainerList()
	waitFirst.Wait()

	w := tabwriter.NewWriter(os.Stdout, 20, 1, 3, ' ', 0)
	printHeader := func() {
		if !opts.noStream {
			fmt.Fprint(os.Stdout, "\033[2J")
			fmt.Fprint(os.Stdout, "\033[H")
		}
		io.WriteString(w, "CONTAINER\tCPU %\tMEM USAGE / LIMIT\tMEM %\tNET I/O\tBLOCK I/O\tPIDS\n")
	}

	for range time.Tick(500 * time.Millisecond) {
		printHeader()
		toRemove := []string{}
		cStats.mu.Lock()
		for _, s := range cStats.cs {
			if err := s.Display(w); err != nil && !opts.noStream {
				logrus.Debugf("stats: got error for %s: %v", s.Name, err)
				if err == io.EOF {
					toRemove = append(toRemove, s.Name)
				}
			}
		}
		cStats.mu.Unlock()
		for _, name := range toRemove {
			cStats.remove(name)
		}
		if len(cStats.cs) == 0 && !showAll {
			return
		}
		w.Flush()
		if opts.noStream {
			break
		}
		select {
		case err, ok := <-closeChan:
			if ok {
				if err != nil {
					// this is suppressing "unexpected EOF" in the cli when the
					// daemon restarts so it shutdowns cleanly
					if err == io.ErrUnexpectedEOF {
						return
					}
					return
				}
			}
		default:
			// just skip
		}
	}
}
