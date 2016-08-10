package main

import (
	"encoding/json"
	"fmt"
	"github.com/docker/engine-api/client"
	"github.com/docker/engine-api/types"
	"github.com/docker/go-units"
	"golang.org/x/net/context"
	"io"
	"os"
	"text/tabwriter"
	"time"
)

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

func main() {
	defaultHeaders := map[string]string{"User-Agent": "engine-api-cli-1.0"}
	cli, err := client.NewClient("unix:///var/run/docker.sock", "v1.12", nil, defaultHeaders)
	if err != nil {
		panic(err)
	}
	ctx := context.Background()
	responseBody, err := cli.ContainerStats(ctx, "b25122a90b00", true)
	defer responseBody.Close()
	dec := json.NewDecoder(responseBody)
	stop := make(chan int, 1)
	errors := make(chan error, 1)
	go func() {
		w := tabwriter.NewWriter(os.Stdout, 20, 1, 3, ' ', 0)
		for {
			var v *types.StatsJSON
			if err := dec.Decode(&v); err != nil {
				dec = json.NewDecoder(io.MultiReader(dec.Buffered(), responseBody))
				errors <- err
				if err == io.EOF {
					stop <- 1
					break
				}
				time.Sleep(100 * time.Millisecond)
				continue
			}
			memory := float64(v.MemoryStats.Usage)
			memPercent := 0.0
			if v.MemoryStats.Limit != 0 {
				memPercent = float64(v.MemoryStats.Usage) / float64(v.MemoryStats.Limit) * 100.0
			}
			previousCPU := v.PreCPUStats.CPUUsage.TotalUsage
			previousSystem := v.PreCPUStats.SystemUsage
			cpuPercent := calculateCPUPercent(previousCPU, previousSystem, v)

			fmt.Fprint(os.Stdout, "\033[2J")
			fmt.Fprint(os.Stdout, "\033[H")
			fmt.Fprintf(w, "memusage:%s\tmemPercent:%.2f%%\tcpuPercent:%.2f%%\n", units.BytesSize(memory), memPercent, cpuPercent)
			w.Flush()
		}
	}()
	for {
		select {
		case <-stop:
			fmt.Println("exit....")
			return
		case err := <-errors:
			fmt.Println(err)
		}
	}
}
