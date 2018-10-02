package gobucket

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"sync"
)

type OnPeerScheduleFailed func(server, task, id, err string)

type pclient struct {
	mux   sync.Mutex
	mc    *mconn
	srvup bool
	dbg   bool
	infs  []*TaskInfo
	fail  OnPeerScheduleFailed
}

func (p *pclient) dial(addr string) error {
	conn, err := net.Dial("tcp4", addr)
	if err != nil {
		p.srvup = false
		return err
	}
	p.srvup = true
	p.mux.Lock()
	defer p.mux.Unlock()
	p.mc = &mconn{
		conn:    conn,
		address: conn.RemoteAddr().String(),
	}
	go p.listen(p.mc)
	return nil
}

func (p *pclient) listen(mc *mconn) error {
	defer mc.close()
	stopReq := make(chan bool)
	stopRet := make(chan bool)
	go p.up(mc, stopReq)
	go p.down(mc, stopRet)
	for {
		msg, err := mc.read()
		if err != nil {
			stopReq <- true
			stopRet <- true
			p.srvup = false
			if err.Error() == eof {
				p.debug("pclient: unable to read from server:", p.mc.addr(),
					",closed/rejecting")
				mc.close()
				return err
			}
			p.debug("pclient: unable to up receive data", err.Error())
			return err
		}
		ret, err := parseRet(trimLine(msg))
		if err != nil {
			p.debug("pclient: unable to parse ret data", err.Error())
			continue
		}
		mc.pushRet(ret)
	}
	stopReq <- true
	return nil
}

func (p *pclient) up(mc *mconn, stopReq chan bool) {
	for {
		select {
		case <-stopReq:
			return
		default:
			if mc.reqBuff != nil {
				mc.reqMux.Lock()
				if reqs := mc.reqBuff; len(reqs) > 0 {
					err := p.request(mc, reqs[0])
					if err != nil {
						p.debug("pclient: unable to send request to:", mc.addr(), err.Error())
					}
					mc.reqBuff = append(mc.reqBuff[:0], mc.reqBuff[1:]...)
				}
				mc.reqMux.Unlock()
			}
			continue
		}
	}
}

func (p *pclient) down(mc *mconn, stopRet chan bool) {
	for {
		select {
		case <-stopRet:
			return
		default:
			if mc.reqBuff != nil {
				mc.retMux.Lock()
				if rets := mc.retBuff; len(rets) > 0 {
					err := p.resolve(mc, rets[0])
					if err != nil {
						p.debug("pclient: resolve err=", err.Error())
					}
					mc.retBuff = append(mc.retBuff[:0], mc.retBuff[1:]...)
				}
				mc.retMux.Unlock()
			}
			continue
		}
	}
}

func (p *pclient) resolve(mc *mconn, ret *Ret) error {
	switch ret.Cmd {
	case PONG:
		return cpong(p, mc, ret)
	case TASK:
		return ctask(p, mc, ret)
	default:
		return errors.New("unresolved command")
	}
}

func (p *pclient) request(mc *mconn, req *Req) error {
	bytes, err := json.Marshal(req)
	if err != nil {
		return err
	}
	linedBytes := []byte(fmt.Sprintf("%s\n", string(bytes)))
	_, err = mc.conn.Write(linedBytes)
	return err
}

func parseRet(str string) (*Ret, error) {
	var ret *Ret
	err := json.Unmarshal([]byte(str), &ret)
	if err != nil {
		return nil, err
	}
	return ret, nil
}

func (p *pclient) debug(args ...interface{}) {
	if p.dbg {
		log.Println(args...)
	}
}
