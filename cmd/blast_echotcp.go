// Request body is read from stdin or positional arguments as file names,
// if there are many request files, they are randomly selected to send.

package main

import (
	"blaster"
	"flag"
	"io"
	"net"
	"log"
	"os"
)

var (
	requestTotal = flag.Int("n", 1000, "total number of requests")
	concurrency = flag.Int("c", 10, "concurrency level")
	timeout = flag.Float64("t", 5, "timeout value in second")
	debug = flag.Bool("d", false, "output debug information")
)

type EchoTCPBlaster struct{
	Addr string
	Logger *log.Logger
}

func NewEchoTCPBlaster(addr string) (b EchoTCPBlaster) {
	logger := log.New(os.Stderr, "", log.Ldate | log.Lmicroseconds)
	return EchoTCPBlaster{addr, logger}
}

func (b EchoTCPBlaster) Dial(debug bool) (conn net.Conn, status string) {
	conn, err := net.Dial("tcp", b.Addr)
	if err != nil {
		if err, match := err.(net.Error); match && ! err.Temporary() {
			b.Logger.Fatalln(err.String())
		} else if debug {	
			b.Logger.Println(err.String())
		}
		return conn, "connection errors"
	}
	return conn, ""
}

func (b EchoTCPBlaster) HandleConn(conn net.Conn, debug bool) (size int, status string) {
	n, err := conn.Write([]byte("Hello from tcpblaster!"))
	if debug {
		b.Logger.Printf("sent %d byte to %s\n", n, conn.RemoteAddr())
	}
	if err != nil {
		if debug {
			b.Logger.Println(err)
		}
		return 0, "errors while sending"
	}
	buf := make([]byte, n)
	n, err = conn.Read(buf)
	if debug {
		b.Logger.Printf("read %d byte from %s\n", n, conn.RemoteAddr())
	}
	if err != nil && err != os.EOF {
		if debug {
			b.Logger.Println(err)
		}
		if err, match := err.(net.Error); match && err.Timeout() {
			return n, "timeouts"
		}
		return n, "errors while receiving"
	}
	return n, ""
}

func main() {
	flag.Parse()
	host := "localhost:3640"
	if flag.NArg() >= 1 {
		host = flag.Arg(0)
	}
	b := NewEchoTCPBlaster(host)
	blaster.Blast(b, *requestTotal, *concurrency, int64(*timeout * 1e9), *debug)
}
