package blaster

import (
	"fmt"
	"net"
	"os"
	stats "github.com/GaryBoone/GoStats"
	"time"
)

type Interface interface {
	
	// should return a net.Conn and a status code, which must be an an empty string for a successful connection
	Dial(debug bool) (conn net.Conn, status string)
	
	// should return the response size in byte and a status code
	HandleConn(conn net.Conn, debug bool) (size int, status string)
}

const (
	samplingPeriod = 5e9
	samplingFreq = 1e9 / samplingPeriod // number of samples per second
)

func Blast(b Interface, requestTotal int, concurrency int, timeout int64, debug bool) {
	requestTicker := time.NewTicker(int64(1e9 / concurrency))
	defer requestTicker.Stop()

	result := make(chan string, 42) // statuses returned by Dial or HandleConn
	tDial := make(chan float64, 42) // Dial timing samples
	tHandleConn := make(chan float64, 42) // HandleConn timing samples
	
	// spawn as many workers as the requested concurrency level
	tStart := time.Nanoseconds()
	for i := 0; i < concurrency; i++ {
		go func() {
			var t0, t1, t2 int64
			var err os.Error
			for {
				<-requestTicker.C
				t0 = time.Nanoseconds()
				conn, status := b.Dial(debug)
				if status != "" {
					result <- status
					continue
				}
				defer conn.Close()
				t1 = time.Nanoseconds()
				nsDial = t1 - t0
				tDial <- float64(nsDial)
				err = conn.SetTimeout(timeout - nsDial)
				if err != nil {
					log.Fatalf("cound't set connection timeout: %s\n", err.String())
				}
				_, status = b.HandleConn(conn, debug)
				t2 = time.Nanoseconds()
				tHandleConn <- float64(t2 - t0)
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
	defer sampleTicker.Stop()
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
	fmt.Printf("Time elapsed: %d s\n", dt)
	fmt.Printf("%d requests at %.2f request/s\n", requestTotal, float64(requestTotal) / float64(dt))
	fmt.Printf("%d replies at %.2f reply/s\n", replyTotal, float64(replyTotal) / float64(dt))
	fmt.Printf("Connection time: %.2f ± %.2f (ms)\n", statsDial.Mean() / 1000, statsDial.SampleStandardDeviation() / 1000)
	fmt.Printf("Reply time: %.2f ± %.2f (ms)\n", statsHandleConn.Mean() / 1000, statsHandleConn.SampleStandardDeviation() / 1000)
	if len(histogram) > 0 {
		fmt.Printf(w, "Errors breakdown:\n")
		for status, count := range histogram {
			fmt.Printf("\t%d %s\n", count, status)
		}
	}
}
