package gobucket

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"time"
)

const (
	REG  = "REG"  //Register
	UREG = "UREG" //Not yet registered
	REGD = "REGD" //Registered Already

	RET  = "RET"
	KILL = "KILL"

	PING = "PING"
	PONG = "PONG"

	TASK = "TASK"
)

func sreg(b *bserver, mc *mconn, req *Req) error {
	b.rcMux.Lock()
	defer b.rcMux.Unlock()
	addr := mc.addr()
	if _, ok := b.regConns[addr]; !ok {
		b.regConns[addr] = mc.conn
		b.debug("server: connection to", addr, "has been registered")
		mc.pushRet(&Ret{
			Cmd:  REG,
			Data: fmt.Sprintf("%s registered at %s", mc.addr(), time.Now().String()),
		})
		return nil
	}
	msg := fmt.Sprintf("server: connection to %s is already established", addr)
	mc.pushRet(&Ret{
		Cmd: REGD,
		Err: msg,
	})
	return errors.New(msg)
}

func sping(b *bserver, mc *mconn, req *Req) error {
	if !isReg(b, mc.conn) {
		mc.pushRet(&Ret{
			Cmd: UREG,
			Err: fmt.Sprintf("%s has not registered yet", mc.addr()),
		})
		return errNotRegistered
	}
	b.debug(fmt.Sprintf("server: accept ping from %s", mc.addr()))
	mc.pushRet(&Ret{
		Cmd:  PONG,
		Data: string(b.ctrl.info()),
	})
	return nil
}

func stask(b *bserver, mc *mconn, req *Req) error {
	if !isReg(b, mc.conn) {
		mc.pushRet(&Ret{
			Cmd: UREG,
			Err: fmt.Sprintf("%s has not registered yet", mc.addr()),
		})
		return errNotRegistered
	}
	b.debug(fmt.Sprintf("server: accept task from %s", mc.addr()))
	var reqData interface{}
	err := json.Unmarshal([]byte(req.Data), &reqData)
	if err != nil {
		mc.pushRet(&Ret{
			Cmd: TASK,
			Err: err.Error(),
		})
		return err
	}
	err = b.ctrl.get(req.Group).Fill(context.Background(), ImmidiateTask, req.PID, reqData)
	if err != nil {
		log.Printf("protocol: unable fill from %s err=%s data=%+v\n", mc.addr(), err.Error(), req)
		mc.pushRet(&Ret{
			Cmd: TASK,
			Err: err.Error(),
		})
		return fmt.Errorf("unable fill from %s err=%s", mc.addr(), err.Error())
	}
	mc.pushRet(&Ret{
		Cmd:  TASK,
		Data: "success push the task",
	})
	return nil
}

func cpong(p *pclient, mc *mconn, ret *Ret) error {
	p.debug("pclient: accepting pong from", mc.addr())
	var infs []*TaskInfo
	err := json.Unmarshal([]byte(ret.Data), &infs)
	if err != nil {
		p.debug("pclient: error on marshaling pong ret data", ret, err.Error())
		return err
	}
	bytes, _ := json.Marshal(p.infs)
	p.debug("pclient: pong information=", string(bytes))
	p.infs = infs
	return nil
}

func ctask(p *pclient, mc *mconn, ret *Ret) error {
	p.debug("pclient: accepting task schedule reply from", mc.addr())
	bytes, _ := json.Marshal(ret)
	p.debug("pclient: task data=", string(bytes))
	return nil
}

func isReg(b *bserver, conn net.Conn) bool {
	b.rcMux.Lock()
	defer b.rcMux.Unlock()
	addr := conn.RemoteAddr().String()
	_, ok := b.regConns[addr]
	return ok
}
