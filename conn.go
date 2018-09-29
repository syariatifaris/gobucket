package gobucket

import (
	"bufio"
	"net"
	"sync"
)

type tconn struct {
	conn     net.Conn
	address  string
	sendMux  sync.Mutex
	sendBuff []*Ret
	recvMux  sync.Mutex
	recvBuff []*Req
}

func (m *tconn) close() error {
	return m.conn.Close()
}

func (m *tconn) read() (string, error) {
	return bufio.NewReader(m.conn).ReadString('\n')
}

func (m *tconn) addr() string {
	return m.address
}

func (m *tconn) pushRecv(req *Req) {
	m.recvMux.Lock()
	defer m.recvMux.Unlock()
	m.recvBuff = append(m.recvBuff, req)
}

func (m *tconn) pushSend(ret *Ret) {
	m.sendMux.Lock()
	defer m.sendMux.Unlock()
	m.sendBuff = append(m.sendBuff, ret)
}
