package gobucket

import (
	"bufio"
	"net"
	"sync"
)

type mconn struct {
	conn    net.Conn
	address string

	retMux  sync.Mutex
	retBuff []*Ret
	reqMux  sync.Mutex
	reqBuff []*Req
}

func (m *mconn) close() error {
	return m.conn.Close()
}

func (m *mconn) read() (string, error) {
	return bufio.NewReader(m.conn).ReadString('\n')
}

func (m *mconn) addr() string {
	return m.address
}

func (m *mconn) pushReq(req *Req) {
	m.reqMux.Lock()
	defer m.reqMux.Unlock()
	m.reqBuff = append(m.reqBuff, req)
}

func (m *mconn) pushRet(ret *Ret) {
	m.retMux.Lock()
	defer m.retMux.Unlock()
	m.retBuff = append(m.retBuff, ret)
}
