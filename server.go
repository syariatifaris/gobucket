package gobucket

import (
	"encoding/json"
	"log"
	"net"
	"strings"
	"sync"

	"github.com/tokopedia/cartapp/errors"
)

const ErrEOF = "EOF"

var ErrNotRegistered = errors.New("tconn not registered")

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

func NewServer(port string, debug bool) Server {
	return &tcpServer{
		baseServer: &baseServer{
			port:     port,
			members:  []string{"127.0.0.1"},
			regConns: make(map[string]net.Conn, 0),
			dbg:      debug,
		},
	}
}

type Server interface {
	RunServer(chan error)
}

type baseServer struct {
	port     string
	members  []string
	rcMux    sync.Mutex
	regConns map[string]net.Conn
	dbg      bool
}

func (b *baseServer) debug(args ...interface{}) {
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
