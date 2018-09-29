package gobucket

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"strings"
	"sync"

	"github.com/tokopedia/cartapp/errors"
)

const ErrEOF = "EOF"

var ErrNotRegistered = errors.New("conn not registered")

type Req struct {
	Cmd   string `json:"cmd"`
	PID   string `json:"pid"`
	Group string `json:"group"`
	Data  string `json:"data"`
}

type Ret struct {
	Cmd  string `json:"cmd"`
	Data string `json:"data,omitempty"`
	Err  string `json:"err,omitempty"`
}

func NewTcpServer(port string) Server {
	return &tcpServer{
		baseServer: &baseServer{
			port:     port,
			members:  []string{"127.0.0.1"},
			regConns: make(map[string]net.Conn, 0),
		},
	}
}

type Server interface {
	RunServer(chan error)
	HandleConnection(c net.Conn) error
}

type baseServer struct {
	port     string
	members  []string
	rcMux    sync.Mutex
	regConns map[string]net.Conn
}

type tcpServer struct {
	*baseServer
}

type muxConn struct {
	mux  sync.Mutex
	conn net.Conn
}

func (m *muxConn) close() error {
	return m.conn.Close()
}

func (s *tcpServer) RunServer(errChan chan error) {
	defer close(errChan)
	ln, err := net.Listen("tcp4", fmt.Sprintf(":%s", s.port))
	if err != nil {
		errChan <- err
		return
	}
	for {
		conn, err := ln.Accept()
		if err != nil {
			errChan <- err
			return
		}
		if !isConnAllowed(conn, s.members) {
			conn.Close()
			log.Println("closing unauthorized conn from", conn.RemoteAddr().String())
			continue
		}
		go s.HandleConnection(conn)
	}
}

func (s *tcpServer) HandleConnection(mconn *muxConn) error {
	defer mconn.conn.Close()
	for {
		msg, err := bufio.NewReader(conn).ReadString('\n')
		if err != nil {
			if err.Error() == ErrEOF {
				s.removeConn(conn)
				log.Println("connection closed, addr:", conn.RemoteAddr().String(), err.Error())
			} else {
				log.Println("read data error", conn.RemoteAddr().String(), err.Error())
			}
			return err
		}
		req, err := parseReq(trimLine(msg))
		if err != nil {
			log.Println("unable to parse command", err.Error())
			continue
		}
		err = s.resolveProto(conn, req)
		if err != nil {
			log.Println("unable to resolve protocol", err.Error())
			continue
		}
	}
	return nil
}

func (s *tcpServer) resolveProto(conn net.Conn, req *Req) error {
	switch req.Cmd {
	case REG:
		return doRegister(s.baseServer, conn, req)
	case PING:
		return doPing(s.baseServer, conn, req)
	default:
		return errors.New("unresolved command")
	}
}

func (s *tcpServer) removeConn(conn net.Conn) {
	if isRegisteredConn(s.baseServer, conn) {
		delete(s.baseServer.regConns, conn.RemoteAddr().String())
	}
}

func isConnAllowed(conn net.Conn, members []string) bool {
	addrs := strings.Split(conn.RemoteAddr().String(), ":")
	if len(addrs) == 0 {
		return false
	}
	for _, m := range members {
		if m == addrs[0] {
			return true
		}
	}
	return false
}

func parseReq(str string) (*Req, error) {
	var req *Req
	err := json.Unmarshal([]byte(str), &req)
	if err != nil {
		return nil, err
	}
	return req, nil
}

func trimLine(str string) string {
	return strings.Replace(str, "\n", "", -1)
}
