package subscriber

import (
	"fmt"
	"log"
	"net"
	"math"
	"time"
	"sync"
	"bytes"
	"strconv"
	"strings"
)

type SubscribeMessage struct {
	Name string
	Type string
	Values map[string]float64
}

type Subscriber struct {
	In chan SubscribeMessage
	stopCh chan struct{}
	graphiteAddress string
	interval time.Duration
	timers map[string]map[string]float64
	gauges map[string]float64
	counters  map[string]float64
	countInactivity map[string]float64
	wg sync.WaitGroup
	postfix string
}

func ticker(d time.Duration) chan bool {
	tick := make(chan bool)
	go func() {
		minute := int(d.Minutes())
		for {
			time.Sleep(1 * time.Minute)
			if time.Now().Minute() > minute {
				if (time.Now().Minute() % minute) != 0 {
					continue
				}
			} else {
				if (minute % time.Now().Minute()) != 0 {
					continue
				}
			}
			tick <- true
		}
	}()
	return tick
}

func NewSubscriber(Interval time.Duration, wg sync.WaitGroup, graphiteAddress string, postfix string) *Subscriber {
	timers := make(map[string]map[string]float64)
	gauges := make(map[string]float64)
	counters := make(map[string]float64)
	countInactivity := make(map[string]float64)
	In := make(chan SubscribeMessage, 1000)
	stopCh := make(chan struct{})
	return &Subscriber{In: In, countInactivity: countInactivity, counters: counters, wg: wg, gauges: gauges, timers: timers, interval: Interval, stopCh : stopCh}
}

func (s *Subscriber) Monitor() {
	tick := ticker(s.interval)
	for {
		select {
		case <-s.stopCh:
			s.submit(true)
			s.wg.Done()
		case <-tick:
			s.submit(false)
		case message := <-s.In:
			s.processPkt(message)
		}
	}
}
	
func (s *Subscriber) processPkt(message SubscribeMessage) {
	switch message.Type {
	case "timer":
		if _, ok := s.timers[message.Name]; !ok {
			s.timers[message.Name] = make(map[string]float64)
			for k, v := range message.Values {
				s.timers[message.Name][k] = v
			}
		} else {
			for k, v := range message.Values {
				switch k {
				case "upper":
					if s.timers[message.Name]["upper"] < v {
						s.timers[message.Name]["upper"] = v
					}
				case "lower":
					if s.timers[message.Name]["lower"] > v {
						s.timers[message.Name]["lower"] = v
					}
				case "count":
					if s.timers[message.Name]["mean"] < math.MaxFloat64 {
						s.timers[message.Name]["count"] = s.timers[message.Name]["count"]+v
					}
				case "mean":
					if s.timers[message.Name]["mean"] > (math.MaxFloat64 - v) {
						s.timers[message.Name]["mean"] = math.MaxFloat64
					} else {
						s.timers[message.Name]["mean"] = s.timers[message.Name]["mean"]+v
					}
				default:
					if strings.Contains(k , ".upper_") {
						if s.timers[message.Name][k] < v {
							s.timers[message.Name][k] = v
						}
					} else if strings.Contains(k , ".lower_") {
						if s.timers[message.Name][k] > v {
							s.timers[message.Name][k] = v
						}
					} else {
						log.Println("Subscriber: warn: timer unknown subtype", k)
					}
				}
			}
		}
	case "gauge":
		s.gauges[message.Name] = message.Values["gauge"]
	case "counter":
		s.counters[message.Name] += message.Values["count"]
	}
}

func (s *Subscriber) submit(sync bool) {
	var buffer bytes.Buffer
	now := time.Now().Unix()
	for k,v := range s.timers {
		count := s.timers[k]["count"]
		for t, tv := range v {
			if t == "mean" {
				fmt.Fprintf(&buffer, "%s.%s%s %s %d\n", k, t,s.postfix, strconv.FormatFloat(tv/count, 'f', -1, 64), now)
			} else {
				fmt.Fprintf(&buffer, "%s.%s%s %s %d\n", k, t,s.postfix, strconv.FormatFloat(tv, 'f', -1, 64), now)
			}
		}
		delete(s.timers, k)
	}
	for k, v := range s.counters {
		fmt.Fprintf(&buffer, "%s %s %d\n", k, strconv.FormatFloat(v, 'f', -1, 64), now)
		delete(s.counters, k)
	}
	for k, v := range s.gauges {
		fmt.Fprintf(&buffer, "%s %s %d\n", k, strconv.FormatFloat(v, 'f', -1, 64), now)
		delete(s.gauges, k)
	}
	if sync {
		flush(buffer.Bytes(), s.graphiteAddress, 10)
	} else {
		go flush(buffer.Bytes(), s.graphiteAddress, 10)
	}
}

func (s *Subscriber) Stop() {
	log.Println("Stopping", s)
	s.stopCh <- struct{}{}
}

func flush(data []byte, graphiteAddress string, maxTry int) {
	retry := 0
retry_tcp:
	client, err := net.Dial("tcp", graphiteAddress)
	if err != nil {
		retry++
		if retry > maxTry {
			log.Println("Max retry reached, droping", err)
			return
		}
		goto retry_tcp
	}
	defer client.Close()
	_, err = client.Write(data)
	if err != nil {
		log.Println("Error: sending graphite", err)
	}
}
