package main

import (
	"github.com/influxdata/influxdb/client/v2"
	"log"
	"time"
)

const (
	MyDB     = "docker"
	username = "bughunter"
	password = "lol2012919"
)

func main() {
	c, err := client.NewHTTPClient(client.HTTPConfig{
		Addr:     "http://localhost:8086",
		Username: username,
		Password: password,
	})

	if err != nil {
		log.Fatalln("Error:", err)
	}
	defer c.Close()

	bp, err := client.NewBatchPoints(client.BatchPointsConfig{
		Database:  MyDB,
		Precision: "s",
	})

	if err != nil {
		log.Fatalln("Error:", err)
	}

	tags := map[string]string{"cpu": "cpu-total"}
	fields := map[string]interface{}{
		"idle":   10.1,
		"system": 53.3,
		"user":   46.9,
	}

	pt, err := client.NewPoint("cpu_usage", tags, fields, time.Now())

	if err != nil {
		log.Fatalln("Error:", err)
	}

	bp.AddPoint(pt)
	c.Write(bp)
}
