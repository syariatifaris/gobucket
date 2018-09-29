package main

import (
	"log"

	"github.com/syariatifaris/gobucket"
)

func main() {
	tcpServer := gobucket.NewServer("6666", true)
	errChan := make(chan error)
	log.Println("running server..")
	go tcpServer.RunServer(errChan)
	select {
	case err := <-errChan:
		log.Println("error on server", err.Error())
	}
}
