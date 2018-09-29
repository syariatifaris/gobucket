package gobucket

import (
	"fmt"
	"log"
	"net"

	"encoding/json"

	"github.com/tokopedia/cartapp/errors"
)

const (
	REG  = "REG"
	RET  = "RET"
	KILL = "KILL"
	PING = "PING"
)

func doRegister(b *baseServer, conn net.Conn, req *Req) error {
	b.rcMux.Lock()
	defer b.rcMux.Unlock()
	addr := conn.RemoteAddr().String()
	if _, ok := b.regConns[addr]; !ok {
		b.regConns[addr] = conn
		log.Println("connection to", addr, "has been registered")
		reply(conn, &Ret{
			Cmd: RET,
		})
		return nil
	}
	msg := fmt.Sprintf("connection to %s is already established", addr)
	reply(conn, &Ret{
		Cmd: RET,
		Err: msg,
	})
	return errors.New(msg)
}

func doPing(b *baseServer, conn net.Conn, req *Req) error {
	if !isRegisteredConn(b, conn) {
		return ErrNotRegistered
	}
	reply(conn, &Ret{
		Cmd:  RET,
		Data: "ALIVE",
	})
	return nil
}

//#region utilities
func isRegisteredConn(b *baseServer, conn net.Conn) bool {
	b.rcMux.Lock()
	defer b.rcMux.Unlock()
	addr := conn.RemoteAddr().String()
	_, ok := b.regConns[addr]
	return ok
}

func reply(conn net.Conn, ret *Ret) error {
	bytes, err := json.Marshal(ret)
	if err != nil {
		return err
	}
	linedBytes := []byte(fmt.Sprintf("%s\n", string(bytes)))
	_, err = conn.Write(linedBytes)
	return err
}
