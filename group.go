package gobucket

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"
)

const max = 9999

func NewTaskBucketGroup(buckets map[string]TaskBucket, peers []string,
	serverPort string, debug bool) TaskBucketGroup {
	ctrl := &bucketsCtrl{
		tbs: buckets,
	}
	pctrl := &peersCtrl{
		peers: make(map[string]*pclient),
	}
	for _, p := range peers {
		pctrl.add(p, debug)
	}
	return &bucketGroup{
		bctrl:      ctrl,
		pctrl:      pctrl,
		server:     newServer(serverPort, debug, ctrl, peers),
		stopServer: make(chan bool),
	}
}

type TaskBucketGroup interface {
	GetBucket(name string) TaskBucket
	StartWork() error
	StopWork()
	SetOnPeerScheduleFailed(f OnPeerScheduleFailed)
	Fill(ctx context.Context, task, pid string, data interface{}) error
}

type bucketGroup struct {
	server       server
	bctrl        *bucketsCtrl
	pctrl        *peersCtrl
	stopServer   chan bool
	stopDiscover chan bool
}

func (b *bucketGroup) GetBucket(name string) TaskBucket {
	return b.bctrl.get(name)
}

func (b *bucketGroup) StartWork() error {
	go b.discover()

	errChan := make(chan error)
	go b.server.run(errChan)
	select {
	case err := <-errChan:
		return err
	case <-b.stopServer:
		return errors.New("bserver signaled to stop")
	}
}

func (b *bucketGroup) StopWork() {
	b.stopServer <- true
	b.stopDiscover <- true
}

func (b *bucketGroup) Fill(ctx context.Context, task, pid string, data interface{}) error {
	tb := b.GetBucket(task)
	if tb != nil {
		err := tb.Fill(ctx, ImmidiateTask, pid, data)
		if err != nil && err.Error() == efull {
			p, err := b.pctrl.best(task)
			if err != nil {
				return fmt.Errorf("local buffer full & unable to fill to peer, err=%s", err.Error())
			}
			bytes, err := json.Marshal(data)
			if err != nil {
				return fmt.Errorf("local buffer full & unable to fill to peer, err=%s", err.Error())
			}
			p.mc.pushReq(&Req{
				Cmd:   TASK,
				Group: task,
				PID:   pid,
				Data:  string(bytes),
			})
		}
		return err
	}
	return fmt.Errorf("unable to find bucket for task=%s", task)
}

func (b *bucketGroup) SetOnPeerScheduleFailed(fail OnPeerScheduleFailed) {
	for _, peer := range b.pctrl.peers {
		peer.fail = fail
	}
}

func (b *bucketGroup) discover() {
	for {
		select {
		case <-b.stopDiscover:
			return
		default:
			b.pctrl.dials()
			time.Sleep(time.Second / 10)
		}
	}
}

type TaskInfo struct {
	Key string `json:"key"`
	Len int    `json:"len"`
}

type peersCtrl struct {
	mux   sync.Mutex
	peers map[string]*pclient
}

func (p *peersCtrl) add(addr string, debug bool) {
	p.mux.Lock()
	p.mux.Unlock()
	p.peers[addr] = &pclient{
		dbg: debug,
	}
}

func (p *peersCtrl) dials() {
	p.mux.Lock()
	defer p.mux.Unlock()
	for addr, peer := range p.peers {
		if !peer.srvup {
			err := peer.dial(addr)
			if err != nil {
				peer.debug("pclient: unable to dial", addr, "err:", err.Error())
				continue
			}
			peer.debug("pclient: dial success, ready to up register to", addr, "..")
			peer.mc.pushReq(&Req{
				Cmd: REG,
			})
			continue
		}
		peer.mc.pushReq(&Req{
			Cmd: PING,
		})
	}
}

func (p *peersCtrl) best(task string) (*pclient, error) {
	p.mux.Lock()
	defer p.mux.Unlock()
	var best *pclient = nil
	var blen int
	for _, peer := range p.peers {
		if peer.srvup {
			if best == nil {
				best = peer
				l, err := p.count(best.infs, task)
				if err != nil {
					return nil, err
				}
				blen = l
				continue
			}
			tlen, err := p.count(peer.infs, task)
			if err != nil {
				return nil, err
			}
			if blen > tlen {
				best = peer
				blen = tlen
			}
		}
	}
	if best == nil {
		return nil, errors.New("no peer ready/available")
	}
	return best, nil
}

func (*peersCtrl) count(infs []*TaskInfo, task string) (int, error) {
	for _, inf := range infs {
		if inf.Key == task {
			return inf.Len, nil
		}
	}
	return 0, errors.New("task not found in info")
}

type bucketsCtrl struct {
	mux sync.Mutex
	tbs map[string]TaskBucket
}

func (b *bucketsCtrl) get(name string) TaskBucket {
	b.mux.Lock()
	defer b.mux.Unlock()
	return b.tbs[name]
}

func (b *bucketsCtrl) info() []byte {
	var infs []*TaskInfo
	b.mux.Lock()
	defer b.mux.Unlock()
	for k, t := range b.tbs {
		inf := &TaskInfo{
			Key: k,
			Len: t.length(),
		}
		infs = append(infs, inf)
	}
	bytes, _ := json.Marshal(infs)
	return bytes
}
