package main

import (
	log "github.com/Sirupsen/logrus"
)

func test() {
	log.Info("Hello Walrus after FullTimestamp=true")
}

func main() {
	customFormatter := new(log.TextFormatter)
	customFormatter.TimestampFormat = "2006-01-02 15:04:05"
	customFormatter.FullTimestamp = true
	log.SetFormatter(customFormatter)
	test()
	log.Info("Hello Walrus after FullTimestamp=true")
}
