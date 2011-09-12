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
}

func Blast(b Interface, requestTotal int, concurrency int, debug bool, w io.Writer) {
	ticket := make(chan bool, 42)

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

	result := make(chan string, 42) // statuses returned by Dial or HandleConn
	tDial := make(chan float64, 42) // Dial timing samples
	tHandleConn := make(chan float64, 42) // HandleConn timing samples
	
	// spawn as many workers as the requested concurrency level
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
				if status != "" {
					result <- status
					continue
				}
				defer conn.Close()
				if dosample {
					t1 = time.Nanoseconds()
				}
				_, status = b.HandleConn(conn, debug)
				if dosample {
					t2 = time.Nanoseconds()
					tDial <- float64(t1 - t0)
					tHandleConn <- float64(t2 - t0)
				}
				result <- status
			}
		}()
	}

	// wait for completion of all handling, selecting on updating statistics periodically
	completeTotal := 0
	replyTotal := 0
	intervalTotal := 0
	histogram := make(map[string]int) // histogram of errors
	
	sampleTicker := time.NewTicker(samplingPeriod)
	var statsDial stats.Stats
	var statsHandleConn stats.Stats
	
L:	for {
		var sample float64
		select {
		case sample = <-tDial:
			statsDial.Update(sample)
		case sample = <-tHandleConn:
			statsHandleConn.Update(sample)
		case <-sampleTicker.C:
			fmt.Fprintf(w, "reply rate: %.2f\n", samplingFreq * float64(intervalTotal))
			intervalTotal = 0
		case status := <-result:
			completeTotal++
			if status == "" {
				replyTotal++
				intervalTotal++
			} else {
				count, exist := histogram[status]
				if !exist {
					histogram[status] = 1
				}
				histogram[status] = count + 1
			}
			if completeTotal == requestTotal {
				break L
			}
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
	if len(histogram) > 0 {
		fmt.Fprintf(w, "Errors breakdown:\n")
		for status, count := range histogram {
			fmt.Fprintf(w, "\t%d %s\n", count, status)
		}
	}
}
