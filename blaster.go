package blaster

import (
	"fmt"
	"io"
	"net"
	stats "github.com/GaryBoone/GoStats"
	"time"
)

type Interface interface {
	
	// should return a net.Conn and a status code, which must be 0 for a successful connection
	Dial(debug bool) (conn net.Conn, status string)
	
	// should return the response size in byte and a status code
	HandleConn(conn net.Conn, debug bool) (size int, status string)
}

const (
	samplingPeriod = 5e9
	samplingFreq = 1e9 / samplingPeriod // number of samples per second
)

func inckey(key string, histogram map [string] int) {
	count, exist := histogram[key]
	if !exist {
		histogram[key] = 1
	}
	histogram[key] = count + 1
}

func Blast(b Interface, requestTotal int, concurrency int, debug bool, w io.Writer) {
	histDial := make(map[string]int) // Dial() status histogram
	histHandleConn := make(map[string]int) // HandleConn() status histogram
	var statsDial stats.Stats
	var statsHandleConn stats.Stats

	ticket := make(chan bool, 42)
	result := make(chan bool, 42)
	
	// issue ticket to workers at the given request rate, optionally tell them to collect a sample
	go func() {
		requestTicker := time.NewTicker(int64(1e9 / concurrency))
		sampleTicker := time.NewTicker(samplingPeriod)
		defer requestTicker.Stop()
		defer sampleTicker.Stop()
		var dosample bool
		for i := 0; i < requestTotal; i++ {
			<-requestTicker.C
			select {
			case <-sampleTicker.C:
				dosample = true
			default:
				dosample = false
			}
			ticket <- dosample
		}
	}()

	tStart := time.Nanoseconds()
	for i := 0; i < concurrency; i++ {
		go func() {
			var t0, t1, t2 int64
			for {
				dosample := <-ticket
				if dosample {
					t0 = time.Nanoseconds()
				}
				conn, status := b.Dial(debug)
				inckey(status, histDial)
				if status != "" {
					result <- false
					continue
				}
				defer conn.Close()
				if dosample {
					t1 = time.Nanoseconds()
				}
				_, status = b.HandleConn(conn, debug)
				inckey(status, histHandleConn)
				if dosample {
					t2 = time.Nanoseconds()
					statsDial.Update(float64(t1 - t0))
					statsHandleConn.Update(float64(t2 - t0))
				}
				if status != "" {
					result <- false
				}
				result <- true
			}
		}()
	}

	// wait for completion of all handling, selecting on sampling reply rate periodically
	sampleTicker := time.NewTicker(samplingPeriod)
	completeTotal := 0
	replyTotal := 0
	intervalTotal := 0
L:	for {
		select {
		case replyReceived := <-result:
			completeTotal++
			if replyReceived {
				replyTotal++
				intervalTotal++
			}
			if completeTotal == requestTotal {
				sampleTicker.Stop()
				break L
			}
		case <-sampleTicker.C:
			fmt.Fprintf(w, "reply rate: %.2f\n", samplingFreq * float64(intervalTotal))
			intervalTotal = 0
		}
	}
	tComplete := time.Nanoseconds()
	
	dt := (tComplete - tStart) / 1e9
	fmt.Fprintf(w, "Blast duration %d s\n", dt)
	fmt.Fprintf(w, "%d requests at %.2f request/s\n", requestTotal, float64(requestTotal) / float64(dt))
	fmt.Fprintf(w, "%d replys at %.2f reply/s\n", replyTotal, float64(replyTotal) / float64(dt))
	fmt.Fprintf(w, "%d samples collected\n", statsHandleConn.Size())
	fmt.Fprintf(w, "Connection time: %.2f ± %.2f (ms)\n", statsDial.Mean() / 1000, statsDial.SampleStandardDeviation() / 1000)
	fmt.Fprintf(w, "Reply time: %.2f ± %.2f (ms)\n", statsHandleConn.Mean() / 1000, statsHandleConn.SampleStandardDeviation() / 1000)
	if replyTotal > 0 {
		fmt.Fprintf(w, "Connection summary:\n")
		for status, count := range histDial {
			fmt.Fprintf(w, "\t%d %s\n", count, status)
		}
		fmt.Fprintf(w, "Response summary:\n")
		for status, count := range histHandleConn {
			fmt.Fprintf(w, "\t%d %s\n", count, status)
		}
	}
}
