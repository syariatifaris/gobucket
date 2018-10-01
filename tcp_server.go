package gobucket

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"strings"
	"sync"
)

const eof = "EOF"

var errNotRegistered = errors.New("mconn not registered")

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

func newServer(port string, debug bool, ctrl *bucketsCtrl, members []string) server {
	return &tcpServer{
		bserver: &bserver{
			port:     port,
			members:  members,
			regConns: make(map[string]net.Conn, 0),
			dbg:      debug,
			ctrl:     ctrl,
		},
	}
}

type server interface {
	run(chan error)
}

type bserver struct {
	port     string
	members  []string
	rcMux    sync.Mutex
	regConns map[string]net.Conn
	dbg      bool
	ctrl     *bucketsCtrl
}

func (b *bserver) debug(args ...interface{}) {
	if b.dbg {
		log.Println(args...)
	}
}

func isConnAllowed(conn net.Conn, members []string) bool {
	addrs := strings.Split(conn.RemoteAddr().String(), ":")
	if len(addrs) == 0 {
		return false
	}
	for _, m := range members {
		if ms := strings.Split(m, ":"); len(ms) > 0 {
			if ms[0] == addrs[0] {
				return true
			}
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

//#region tcp server implementation

type tcpServer struct {
	*bserver
}

func (s *tcpServer) run(errChan chan error) {
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
			s.debug("bserver: closing unauthorized mconn from", conn.RemoteAddr().String())
			log.Println()
			continue
		}
		mc := &mconn{
			conn:    conn,
			address: conn.RemoteAddr().String(),
		}
		go s.listen(mc)
	}
}

func (s *tcpServer) listen(mc *mconn) error {
	defer mc.close()
	stopReq := make(chan bool)
	stopRet := make(chan bool)
	go s.down(mc, stopReq)
	go s.up(mc, stopRet)
	for {
		msg, err := mc.read()
		if err != nil {
			if err.Error() == eof {
				s.remove(mc.conn)
				s.debug("bserver: connection closed, addr:", mc.addr())
			} else {
				s.debug("bserver: read data error", mc.addr(), err.Error())
			}
			stopReq <- true
			stopRet <- true
			return err
		}
		req, err := parseReq(trimLine(msg))
		if err != nil {
			s.debug("bserver: unable to parse request data", msg, err.Error())
			continue
		}
		mc.pushReq(req)
	}
	stopReq <- true
	stopRet <- true
	return nil
}

func (s *tcpServer) down(mc *mconn, stopReq chan bool) {
	for {
		select {
		case <-stopReq:
			return
		default:
			if mc.reqBuff != nil {
				mc.reqMux.Lock()
				if reqs := mc.reqBuff; len(reqs) > 0 {
					err := s.resolve(mc, reqs[0])
					if err != nil {
						s.debug("bserver: unable to resolve protocol", err.Error())
					}
					mc.reqBuff = append(mc.reqBuff[:0], mc.reqBuff[1:]...)
				}
				mc.reqMux.Unlock()
			}
			continue
		}
	}
}

func (s *tcpServer) up(mc *mconn, stopRet chan bool) {
	for {
		select {
		case <-stopRet:
			return
		default:
			if mc.retBuff != nil {
				mc.retMux.Lock()
				if sends := mc.retBuff; len(sends) > 0 {
					err := s.reply(mc, sends[0])
					if err != nil {
						s.debug("bserver: unable to reply/return data to", mc.addr(), err.Error())
					}
					mc.retBuff = append(mc.retBuff[:0], mc.retBuff[1:]...)
				}
				mc.retMux.Unlock()
			}
			continue
		}
	}
}

func (s *tcpServer) reply(mc *mconn, ret *Ret) error {
	bytes, err := json.Marshal(ret)
	if err != nil {
		return err
	}
	linedBytes := []byte(fmt.Sprintf("%s\n", string(bytes)))
	_, err = mc.conn.Write(linedBytes)
	return err
}

func (s *tcpServer) resolve(mc *mconn, req *Req) error {
	switch req.Cmd {
	case REG:
		return sreg(s.bserver, mc, req)
	case PING:
		return sping(s.bserver, mc, req)
	case TASK:
		return stask(s.bserver, mc, req)
	default:
		return errors.New("unresolved command")
	}
}

func (s *tcpServer) remove(conn net.Conn) {
	if isReg(s.bserver, conn) {
		delete(s.bserver.regConns, conn.RemoteAddr().String())
	}
}
