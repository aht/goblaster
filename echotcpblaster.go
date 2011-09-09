package blaster

import (
	"net"
	"log"
	"os"
)

type EchoTCPBlaster struct{
	Addr string
}

func (b EchoTCPBlaster) Dial() (conn net.Conn, err os.Error) {
	conn, err = net.Dial("tcp", b.Addr)
	return
}

func (b EchoTCPBlaster) HandleConn(conn net.Conn, debug bool) (ok bool) {
	n, err := conn.Write([]byte("Hello from tcpblaster!"))
	if debug {
		log.Printf("sent %d byte to %s\n", n, conn.RemoteAddr())
	}
	if err != nil {
		if debug {
			log.Println(err)
		}
		return false
	}
	buf := make([]byte, n)
	n, err = conn.Read(buf)
	if debug {
		log.Printf("read %d byte from %s\n", n, conn.RemoteAddr())
	}
	if err != nil && err != os.EOF {
		if debug {
			log.Println(err)
		}
		return false
	}
	return true
}
