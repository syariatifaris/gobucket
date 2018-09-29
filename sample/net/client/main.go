package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"strings"

	"github.com/syariatifaris/gobucket"
)

func main() {
	conn, err := net.Dial("tcp", "127.0.0.1:6666")
	if err != nil {
		panic(fmt.Sprint("panic, unable to dial server ", err.Error()))
	}

	stop := make(chan bool)
	go handleListen(conn, stop)

	req := &gobucket.Req{
		Cmd: "REG",
	}
	bytes, _ := json.Marshal(req)
	_, err = fmt.Fprintf(conn, string(bytes)+"\n")
	if err != nil {
		log.Println("unable to send socket", err.Error())
		return
	}

	req = &gobucket.Req{
		Cmd: "PING",
	}
	bytes, _ = json.Marshal(req)
	_, err = fmt.Fprintf(conn, string(bytes)+"\n")
	if err != nil {
		log.Println("unable to ping socket", err.Error())
		return
	}

	select {
	case <-stop:
		return
	}
}

func handleListen(conn net.Conn, stop chan bool) {
	log.Println("start listening..")
	for {
		message, err := bufio.NewReader(conn).ReadString('\n')
		if err != nil {
			if err.Error() == gobucket.ErrEOF {
				conn.Close()
				log.Printf("server %s terminated..\n", conn.RemoteAddr().String())
				stop <- true
				return
			}
			log.Println("unable to send receive data", err.Error())
			continue
		}
		log.Println("message from server: ", message)
		if strings.Contains(strings.Replace(message, "\n", "", -1), "KILL") {
			return
			stop <- true
		}
	}
}
