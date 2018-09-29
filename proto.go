package gobucket

import (
	"fmt"
	"net"

	"github.com/tokopedia/cartapp/errors"
)

const (
	REG  = "REG"
	RET  = "RET"
	KILL = "KILL"
	PING = "PING"
	PONG = "PONG"
)

func doRegister(b *baseServer, mc *tconn, req *Req) error {
	b.rcMux.Lock()
	defer b.rcMux.Unlock()
	addr := mc.addr()
	if _, ok := b.regConns[addr]; !ok {
		b.regConns[addr] = mc.conn
		b.debug("connection to", addr, "has been registered")
		mc.pushSend(&Ret{
			Cmd: REG,
		})
		return nil
	}
	msg := fmt.Sprintf("connection to %s is already established", addr)
	mc.pushSend(&Ret{
		Cmd: REG,
		Err: msg,
	})
	return errors.New(msg)
}

func doPing(b *baseServer, mc *tconn, req *Req) error {
	if !isRegisteredConn(b, mc.conn) {
		return ErrNotRegistered
	}
	b.debug(fmt.Sprintf("%s: ping server", mc.addr()))
	mc.pushSend(&Ret{
		Cmd: PONG,
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
