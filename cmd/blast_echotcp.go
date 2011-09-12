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
	Timeout int64
	Logger *log.Logger
}

var (
	dialStatusText = map [int] string {
		0: "successful",
		1: "connection errors",
	}
	handlerStatusText = map [int] string {
		0: "successful",
		1: "errors while sending",
		2: "timeout",
		3: "errors while receiving",
	}
)

func NewEchoTCPBlaster(addr string, timeout int64, w io.Writer) (b EchoTCPBlaster) {
	logger := log.New(w, "", log.Ldate | log.Lmicroseconds)
	return EchoTCPBlaster{addr, timeout, logger}
}

func (b EchoTCPBlaster) Dial(debug bool) (conn net.Conn, status int) {
	conn, err := net.Dial("tcp", b.Addr)
	if err != nil {
		if debug {	
			b.Logger.Println(err.String())
		}
		return conn, 1
	}
	err = conn.SetTimeout(b.Timeout)
	if err != nil {
		b.Logger.Fatalln("could not set socket timeout value: ", err.String())
	}
	return conn, 0
}

func (b EchoTCPBlaster) HandleConn(conn net.Conn, debug bool) (size int, status int) {
	n, err := conn.Write([]byte("Hello from tcpblaster!"))
	if debug {
		b.Logger.Printf("sent %d byte to %s\n", n, conn.RemoteAddr())
	}
	if err != nil {
		if debug {
			b.Logger.Println(err)
		}
		return 0, 1
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
			return n, 2
		}
		return n, 3
	}
	return n, 0
}

func (b EchoTCPBlaster) DialStatusString(status int) string {
	return dialStatusText[status]
}

func (b EchoTCPBlaster) HandleConnStatusString(status int) string {
	return handlerStatusText[status]
}

func main() {
	flag.Parse()
	host := "localhost:3640"
	if flag.NArg() >= 1 {
		host = flag.Arg(0)
	}
	b := blaster.NewEchoTCPBlaster(host, int64(*timeout * 1e9), os.Stdout)
	blaster.Blast(b, *requestTotal, *concurrency, *debug, os.Stdout)
}
