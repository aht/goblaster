// Send tcp requests, receive response and gather statistics.

package blaster

import (
	"fmt"
	"io"
	"net"
	"log"
	"os"
	stats "github.com/GaryBoone/GoStats"
	"time"
)

type Interface interface {
	Dial() (net.Conn, os.Error)
	HandleConn(net.Conn, bool) bool
}

func Blast(b Interface, requestTotal int, requestRate int, sampFreq float64, w io.Writer, debug bool) {
	requestTicker := time.NewTicker(int64(1e9 / requestRate))
	sampleTicker := time.NewTicker(int64(1e9 / (float64(requestRate) * sampFreq)))
	defer requestTicker.Stop()
	defer sampleTicker.Stop()

	connErrorTotal := 0
	replyErrorTotal := 0
	var connStats stats.Stats
	var replyStats stats.Stats

	complete := make(chan bool, 42)
	logger := log.New(w, "", log.Ldate | log.Lmicroseconds)

	tStart := time.Nanoseconds()
	for i := 0; i < requestTotal; i++ {
		
		<-requestTicker.C // get a ticket at the request frequency
		
		go func() {
			select { // update statistics at the sampling frequency
			case <-sampleTicker.C:
				t0 := time.Nanoseconds()
				conn, err := b.Dial()
				if err != nil {
					connErrorTotal++
					if debug {
						logger.Println(err.String())
					}
					complete <- true
					return
				}
				defer conn.Close()
				t1 := time.Nanoseconds()
				ok := b.HandleConn(conn, debug)
				t2 := time.Nanoseconds()
				if !ok {
					replyErrorTotal++
				}
				connStats.Update(float64(t1 - t0))
				replyStats.Update(float64(t2 - t0))
			default:
				conn, err := b.Dial()
				if err != nil {
					connErrorTotal++
					if debug {
						logger.Println(err.String())
					}
					complete <- true
					return
				}
				defer conn.Close()
				ok := b.HandleConn(conn, debug)
				if !ok {
					replyErrorTotal++
				}
			}
			complete <- true
		}()
	}
	
	// wait for completion of all handling
	for i := 0; i < requestTotal; i++ {
		<-complete
	}
	tComplete := time.Nanoseconds()
	
	dt := (tComplete - tStart) / 1e9
	effectiveRate := float64(requestTotal) / float64(dt)
	fmt.Fprintf(w, "%d requests, duration %d (s), effective rate %.2f (request/s)\n", requestTotal, dt, effectiveRate)
	fmt.Fprintf(w, "%d samples collected\n", connStats.Size())
	fmt.Fprintf(w, "Connection time: %.2f ± %.2f (ms)\n", connStats.Mean() / 1000, connStats.SampleStandardDeviation() / 1000)
	fmt.Fprintf(w, "Reply time: %.2f ± %.2f (ms)\n", replyStats.Mean() / 1000, replyStats.SampleStandardDeviation() / 1000)
	fmt.Fprintf(w, "Error: %d connection errors, %d reply errors\n", connErrorTotal, replyErrorTotal)
}
