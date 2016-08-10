package main

import (
	"fmt"
)

func main() {
	var getFirst bool
	var cpu uint64
	if getFirst {
		fmt.Println("hello")
	} else {
		fmt.Println("world", cpu)
	}
}
