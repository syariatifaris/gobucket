package gobucket

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
)

type tcpServer struct {
	*baseServer
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
			s.debug("closing unauthorized tconn from", conn.RemoteAddr().String())
			log.Println()
			continue
		}
		mc := &tconn{
			conn:    conn,
			address: conn.RemoteAddr().String(),
		}
		go s.handleConn(mc)
	}
}

func (s *tcpServer) handleConn(mc *tconn) error {
	defer mc.close()
	stopRecv := make(chan bool)
	stopSend := make(chan bool)
	go s.handleRecvBuff(mc, stopRecv)
	go s.handleSendBuff(mc, stopSend)
	for {
		msg, err := mc.read()
		if err != nil {
			if err.Error() == ErrEOF {
				s.removeConn(mc.conn)
				s.debug("connection closed, addr:", mc.addr(), err.Error())
			} else {
				s.debug("read data error", mc.addr(), err.Error())
			}
			stopRecv <- true
			return err
		}
		req, err := parseReq(trimLine(msg))
		if err != nil {
			s.debug("unable to parse command", err.Error())
			continue
		}
		mc.pushRecv(req)
	}
	stopRecv <- true
	return nil
}

func (s *tcpServer) handleRecvBuff(mc *tconn, stopRecv chan bool) {
	for {
		select {
		case <-stopRecv:
			return
		default:
			if mc.recvBuff != nil {
				mc.recvMux.Lock()
				if reqs := mc.recvBuff; len(reqs) > 0 {
					err := s.resolveProto(mc, reqs[0])
					if err != nil {
						s.debug("unable to resolve protocol", err.Error())
					}
					mc.recvBuff = append(mc.recvBuff[:0], mc.recvBuff[1:]...)
				}
				mc.recvMux.Unlock()
			}
			continue
		}
	}
}

func (s *tcpServer) handleSendBuff(mc *tconn, stopSend chan bool) {
	for {
		select {
		case <-stopSend:
			return
		default:
			if mc.sendBuff != nil {
				mc.sendMux.Lock()
				if sends := mc.sendBuff; len(sends) > 0 {
					err := s.reply(mc, sends[0])
					if err != nil {
						s.debug("unable to send return data to", mc.addr(), err.Error())
					}
					mc.sendBuff = append(mc.sendBuff[:0], mc.sendBuff[1:]...)
				}
				mc.sendMux.Unlock()
			}
			continue
		}
	}
}

func (s *tcpServer) reply(mc *tconn, ret *Ret) error {
	bytes, err := json.Marshal(ret)
	if err != nil {
		return err
	}
	linedBytes := []byte(fmt.Sprintf("%s\n", string(bytes)))
	_, err = mc.conn.Write(linedBytes)
	return err
}

func (s *tcpServer) resolveProto(mc *tconn, req *Req) error {
	switch req.Cmd {
	case REG:
		return doRegister(s.baseServer, mc, req)
	case PING:
		return doPing(s.baseServer, mc, req)
	default:
		return errors.New("unresolved command")
	}
}

func (s *tcpServer) removeConn(conn net.Conn) {
	if isRegisteredConn(s.baseServer, conn) {
		delete(s.baseServer.regConns, conn.RemoteAddr().String())
	}
}
