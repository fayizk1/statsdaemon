package main

import (
	"bytes"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"net"
	"os"
	"os/signal"
	"runtime"
	"sort"
	"sync"
	"strconv"
	"strings"
	"syscall"
	"time"
	"./subscriber"
)

const (
	MAX_UNPROCESSED_PACKETS = 1000
	TCP_READ_SIZE           = 4096
)

var signalchan chan os.Signal

type Packet struct {
	Bucket   string
	Value    interface{}
	Modifier string
	Sampling float32
}

type GaugeData struct {
	Relative bool
	Negative bool
	Value    float64
}

type Float64Slice []float64

func (s Float64Slice) Len() int           { return len(s) }
func (s Float64Slice) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s Float64Slice) Less(i, j int) bool { return s[i] < s[j] }

type Percentiles []*Percentile
type Percentile struct {
	float float64
	str   string
}

func (a *Percentiles) Set(s string) error {
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return err
	}
	*a = append(*a, &Percentile{f, strings.Replace(s, ".", "_", -1)})
	return nil
}
func (p *Percentile) String() string {
	return p.str
}
func (a *Percentiles) String() string {
	return fmt.Sprintf("%v", *a)
}

func sanitizeBucket(bucket string) string {
	b := make([]byte, len(bucket))
	var bl int

	for i := 0; i < len(bucket); i++ {
		c := bucket[i]
		switch {
		case (c >= byte('a') && c <= byte('z')) || (c >= byte('A') && c <= byte('Z')) || (c >= byte('0') && c <= byte('9')) || c == byte('-') || c == byte('.') || c == byte('_'):
			b[bl] = c
			bl++
		case c == byte(' '):
			b[bl] = byte('_')
			bl++
		case c == byte('/'):
			b[bl] = byte('-')
			bl++
		}
	}
	return string(b[:bl])
}

var (
	serviceAddress    = flag.String("address", ":8125", "UDP service address")
	tcpServiceAddress = flag.String("tcpaddr", "", "TCP service address, if set")
	maxUdpPacketSize  = flag.Int64("max-udp-packet-size", 1472, "Maximum UDP packet size")
	graphiteAddress   = flag.String("graphite", "127.0.0.1:2003", "Graphite service address (or - to disable)")
	flushInterval     = flag.Int64("flush-interval", 10, "Flush interval (seconds)")
	debug             = flag.Bool("debug", false, "print statistics sent to graphite")
	showVersion       = flag.Bool("version", false, "print version string")
	deleteGauges      = flag.Bool("delete-gauges", true, "don't send values to graphite for inactive gauges, as opposed to sending the previous value")
	persistCountKeys  = flag.Int64("persist-count-keys", 60, "number of flush-intervals to persist count keys")
	receiveCounter    = flag.String("receive-counter", "", "Metric name for total metrics received per interval")
	percentThreshold  = Percentiles{}
	prefixtimer       = flag.String("prefixt", "stats.timers.", "Prefix for timers")
	prefixgauge       = flag.String("prefixg", "stats.gauges.", "Prefix for gauges")
	prefixset         = flag.String("prefixs", "stats.sets.", "Prefix for all sets")
	prefixcounter       = flag.String("prefixc", "stats_counts.", "Prefix for all counters")
	postfix           = flag.String("postfix", "", "Postfix for all stats")
)

func init() {
	flag.Var(&percentThreshold, "percent-threshold",
		"percentile calculation for timers (0-100, may be given multiple times)")
}

var (
	In              = make(chan *Packet, MAX_UNPROCESSED_PACKETS)
	counters        = make(map[string]float64)
	gauges          = make(map[string]float64)
	lastGaugeValue  = make(map[string]float64)
	timers          = make(map[string]Float64Slice)
	countInactivity = make(map[string]int64)
	sets            = make(map[string][]string)
	Subscriber      []chan subscriber.SubscribeMessage
)

func monitor() {
	period := time.Duration(*flushInterval) * time.Second
	ticker := time.NewTicker(period)
	for {
		select {
		case sig := <-signalchan:
			fmt.Printf("!! Caught signal %v... shutting down\n", sig)
			if err := submit(time.Now().Add(period)); err != nil {
				log.Printf("ERROR: %s", err)
			}
			return
		case <-ticker.C:
			if err := submit(time.Now().Add(period)); err != nil {
				log.Printf("ERROR: %s", err)
			}
		case s := <-In:
			packetHandler(s)
		}
	}
}

func packetHandler(s *Packet) {
	if *receiveCounter != "" {
		v, ok := counters[*receiveCounter]
		if !ok || v < 0 {
			counters[*receiveCounter] = 0
		}
		counters[*receiveCounter] += 1
	}

	switch s.Modifier {
	case "ms":
		_, ok := timers[s.Bucket]
		if !ok {
			var t Float64Slice
			timers[s.Bucket] = t
		}
		timers[s.Bucket] = append(timers[s.Bucket], s.Value.(float64))
	case "g":
		gaugeValue, _ := gauges[s.Bucket]

		gaugeData := s.Value.(GaugeData)
		if gaugeData.Relative {
			if gaugeData.Negative {
				// subtract checking for -ve numbers
				if gaugeData.Value > gaugeValue {
					gaugeValue = 0
				} else {
					gaugeValue -= gaugeData.Value
				}
			} else {
				// watch out for overflows
				if gaugeData.Value > (math.MaxFloat64 - gaugeValue) {
					gaugeValue = math.MaxFloat64
				} else {
					gaugeValue += gaugeData.Value
				}
			}
		} else {
			gaugeValue = gaugeData.Value
		}

		gauges[s.Bucket] = gaugeValue
	case "c":
		_, ok := counters[s.Bucket]
		if !ok {
			counters[s.Bucket] = 0
		}
		counters[s.Bucket] += float64(s.Value.(float64)) * float64(1/s.Sampling)
	case "s":
		_, ok := sets[s.Bucket]
		if !ok {
			sets[s.Bucket] = make([]string, 0)
		}
		sets[s.Bucket] = append(sets[s.Bucket], s.Value.(string))
	}
}

func submit(deadline time.Time) error {
	var buffer bytes.Buffer
	var num int64

	now := time.Now().Unix()

	if *graphiteAddress == "-" {
		return nil
	}

	client, err := net.Dial("tcp", *graphiteAddress)
	if err != nil {
		if *debug {
			log.Printf("WARNING: resetting counters when in debug mode")
			processCounters(&buffer, now)
			processGauges(&buffer, now)
			processTimers(&buffer, now, percentThreshold)
			processSets(&buffer, now)
		}
		errmsg := fmt.Sprintf("dialing %s failed - %s", *graphiteAddress, err)
		return errors.New(errmsg)
	}
	defer client.Close()

	err = client.SetDeadline(deadline)
	if err != nil {
		return err
	}

	num += processCounters(&buffer, now)
	num += processGauges(&buffer, now)
	num += processTimers(&buffer, now, percentThreshold)
	num += processSets(&buffer, now)
	if num == 0 {
		return nil
	}

	if *debug {
		for _, line := range bytes.Split(buffer.Bytes(), []byte("\n")) {
			if len(line) == 0 {
				continue
			}
			log.Printf("DEBUG: %s", line)
		}
	}

	_, err = client.Write(buffer.Bytes())
	if err != nil {
		errmsg := fmt.Sprintf("failed to write stats - %s", err)
		return errors.New(errmsg)
	}

	log.Printf("sent %d stats to %s", num, *graphiteAddress)

	return nil
}

func processCounters(buffer *bytes.Buffer, now int64) int64 {
	var num int64
	// continue sending zeros for counters for a short period of time even if we have no new data
	for bucket, value := range counters {
		tempSubMsg := subscriber.SubscribeMessage{Name : bucket, Type: "counter", Values : make(map[string]float64)}
		tempSubMsg.Values["count"] = value
		fmt.Fprintf(buffer, "%s %s %d\n", bucket, strconv.FormatFloat(value, 'f', -1, 64), now)
		delete(counters, bucket)
		countInactivity[bucket] = 0
		num++
		for _, out := range Subscriber{
			out <-tempSubMsg
		}
	}
	for bucket, purgeCount := range countInactivity {
		if purgeCount > 0 {
			fmt.Fprintf(buffer, "%s 0 %d\n", bucket, now)
			num++
		}
		countInactivity[bucket] += 1
		if countInactivity[bucket] > *persistCountKeys {
			delete(countInactivity, bucket)
		}
	}
	return num
}

func processGauges(buffer *bytes.Buffer, now int64) int64 {
	var num int64

	for bucket, gauge := range gauges {
		currentValue := gauge
		lastValue, hasLastValue := lastGaugeValue[bucket]
		var hasChanged bool

		if gauge != math.MaxFloat64 {
			hasChanged = true
		}
		tempSubMsg := subscriber.SubscribeMessage{Name : bucket, Type: "gauge", Values : make(map[string]float64)}
		switch {
		case hasChanged:
			tempSubMsg.Values["gauge"] = currentValue
			fmt.Fprintf(buffer, "%s %s %d\n", bucket, strconv.FormatFloat(currentValue, 'f', -1, 64), now)
			lastGaugeValue[bucket] = currentValue
			gauges[bucket] = math.MaxFloat64
			num++
		case hasLastValue && !hasChanged && !*deleteGauges:
			tempSubMsg.Values["gauge"] = currentValue
			fmt.Fprintf(buffer, "%s %s %d\n", bucket, strconv.FormatFloat(lastValue, 'f', -1, 64), now)
			num++
		default:
			continue
		}
		if _, ok  :=tempSubMsg.Values["gauge"]; ok {
			for _, out := range Subscriber {
				out <-tempSubMsg
			}
		}
	}
	return num
}

func processSets(buffer *bytes.Buffer, now int64) int64 {
	num := int64(len(sets))
	for bucket, set := range sets {

		uniqueSet := map[string]bool{}
		for _, str := range set {
			uniqueSet[str] = true
		}

		fmt.Fprintf(buffer, "%s %d %d\n", bucket, len(uniqueSet), now)
		delete(sets, bucket)
	}
	return num
}

func processTimers(buffer *bytes.Buffer, now int64, pctls Percentiles) int64 {
	var num int64
	for bucket, timer := range timers {
		bucketWithoutPostfix := bucket[:len(bucket)-len(*postfix)]
		num++

		sort.Sort(timer)
		min := timer[0]
		max := timer[len(timer)-1]
		maxAtThreshold := max
		count := len(timer)
		tempSubMsg := subscriber.SubscribeMessage{Name : bucket, Type: "timer", Values : make(map[string]float64)}
		
		sum := float64(0)
		for _, value := range timer {
			sum += value
		}
		mean := sum / float64(len(timer))

		for _, pct := range pctls {
			if len(timer) > 1 {
				var abs float64
				if pct.float >= 0 {
					abs = pct.float
				} else {
					abs = 100 + pct.float
				}
				// poor man's math.Round(x):
				// math.Floor(x + 0.5)
				indexOfPerc := int(math.Floor(((abs / 100.0) * float64(count)) + 0.5))
				if pct.float >= 0 {
					indexOfPerc -= 1 // index offset=0
				}
				maxAtThreshold = timer[indexOfPerc]
			}

			var tmpl string
			var pctstr string
			var tname string
			if pct.float >= 0 {
				tmpl = "%s.upper_%s%s %s %d\n"
				tname = fmt.Sprintf("upper_%s", pct.str)
				pctstr = pct.str
			} else {
				tmpl = "%s.lower_%s%s %s %d\n"
				tname = fmt.Sprintf("lower_%s", pct.str[1:])
				pctstr = pct.str[1:]
			}
			threshold_s := strconv.FormatFloat(maxAtThreshold, 'f', -1, 64)
			fmt.Fprintf(buffer, tmpl, bucketWithoutPostfix, pctstr, *postfix, threshold_s, now)
			tempSubMsg.Values[tname] = maxAtThreshold
		}

		mean_s := strconv.FormatFloat(mean, 'f', -1, 64)
		max_s := strconv.FormatFloat(max, 'f', -1, 64)
		min_s := strconv.FormatFloat(min, 'f', -1, 64)
		tempSubMsg.Values["mean"] = mean
		tempSubMsg.Values["upper"] = max   
		tempSubMsg.Values["lower"] = min
		tempSubMsg.Values["count"] = float64(count)
		fmt.Fprintf(buffer, "%s.mean%s %s %d\n", bucketWithoutPostfix, *postfix, mean_s, now)
		fmt.Fprintf(buffer, "%s.upper%s %s %d\n", bucketWithoutPostfix, *postfix, max_s, now)
		fmt.Fprintf(buffer, "%s.lower%s %s %d\n", bucketWithoutPostfix, *postfix, min_s, now)
		fmt.Fprintf(buffer, "%s.count%s %d %d\n", bucketWithoutPostfix, *postfix, count, now)

		for _, out := range Subscriber {
			out <-tempSubMsg
		}
		delete(timers, bucket)
	}
	return num
}

type MsgParser struct {
	reader       io.Reader
	buffer       []byte
	partialReads bool
	done         bool
}

func NewParser(reader io.Reader, partialReads bool) *MsgParser {
	return &MsgParser{reader, []byte{}, partialReads, false}
}

func (mp *MsgParser) Next() (*Packet, bool) {
	buf := mp.buffer

	for {
		line, rest := mp.lineFrom(buf)

		if line != nil {
			mp.buffer = rest
			return parseLine(line), true
		}

		if mp.done {
			return parseLine(rest), false
		}

		idx := len(buf)
		end := idx
		if mp.partialReads {
			end += TCP_READ_SIZE
		} else {
			end += int(*maxUdpPacketSize)
		}
		if cap(buf) >= end {
			buf = buf[:end]
		} else {
			tmp := buf
			buf = make([]byte, end)
			copy(buf, tmp)
		}

		n, err := mp.reader.Read(buf[idx:])
		buf = buf[:idx+n]
		if err != nil {
			if err != io.EOF {
				log.Printf("ERROR: %s", err)
			}

			mp.done = true

			line, rest = mp.lineFrom(buf)
			if line != nil {
				mp.buffer = rest
				return parseLine(line), len(rest) > 0
			}

			if len(rest) > 0 {
				return parseLine(rest), false
			}

			return nil, false
		}
	}
}

func (mp *MsgParser) lineFrom(input []byte) ([]byte, []byte) {
	split := bytes.SplitAfterN(input, []byte("\n"), 2)
	if len(split) == 2 {
		return split[0][:len(split[0])-1], split[1]
	}

	if !mp.partialReads {
		if len(input) == 0 {
			input = nil
		}
		return input, []byte{}
	}

	if bytes.HasSuffix(input, []byte("\n")) {
		return input[:len(input)-1], []byte{}
	}

	return nil, input
}

func parseLine(line []byte) *Packet {
	split := bytes.SplitN(line, []byte{'|'}, 3)
	if len(split) < 2 {
		logParseFail(line)
		return nil
	}

	keyval := split[0]
	typeCode := string(split[1])

	sampling := float32(1)
	if strings.HasPrefix(typeCode, "c") || strings.HasPrefix(typeCode, "ms") {
		if len(split) == 3 && len(split[2]) > 0 && split[2][0] == '@' {
			f64, err := strconv.ParseFloat(string(split[2][1:]), 32)
			if err != nil {
				log.Printf(
					"ERROR: failed to ParseFloat %s - %s",
					string(split[2][1:]),
					err,
				)
				return nil
			}
			sampling = float32(f64)
		}
	}

	split = bytes.SplitN(keyval, []byte{':'}, 2)
	if len(split) < 2 {
		logParseFail(line)
		return nil
	}
	name := string(split[0])
	val := split[1]
	if len(val) == 0 {
		logParseFail(line)
		return nil
	}

	var (
		err   error
		value interface{}
		prefix string
	)

	switch typeCode {
	case "c":
		value, err = strconv.ParseFloat(string(val), 64)
		if err != nil {
			log.Printf("ERROR: failed to ParseInt %s - %s", string(val), err)
			return nil
		}
		prefix = *prefixcounter
	case "g":
		var rel, neg bool
		var s string

		switch val[0] {
		case '+':
			rel = true
			neg = false
			s = string(val[1:])
		case '-':
			rel = true
			neg = true
			s = string(val[1:])
		default:
			rel = false
			neg = false
			s = string(val)
		}

		value, err = strconv.ParseFloat(s, 64)
		if err != nil {
			log.Printf("ERROR: failed to ParseUint %s - %s", string(val), err)
			return nil
		}

		value = GaugeData{rel, neg, value.(float64)}
		prefix = *prefixgauge
	case "s":
		value = string(val)
		prefix = *prefixset
	case "ms":
		value, err = strconv.ParseFloat(string(val), 64)
		if err != nil {
			log.Printf("ERROR: failed to ParseFloat %s - %s", string(val), err)
			return nil
		}
		prefix = *prefixtimer
	default:
		log.Printf("ERROR: unrecognized type code %q", typeCode)
		return nil
	}

	return &Packet{
		Bucket:   sanitizeBucket(prefix + string(name) + *postfix),
		Value:    value,
		Modifier: typeCode,
		Sampling: sampling,
	}
}

func logParseFail(line []byte) {
	if *debug {
		log.Printf("ERROR: failed to parse line: %q\n", string(line))
	}
}

func parseTo(conn io.ReadCloser, partialReads bool, out chan<- *Packet) {
	defer conn.Close()

	parser := NewParser(conn, partialReads)
	for {
		p, more := parser.Next()
		if p != nil {
			out <- p
		}

		if !more {
			break
		}
	}
}

func udpListener() {
	address, _ := net.ResolveUDPAddr("udp", *serviceAddress)
	log.Printf("listening on %s", address)
	listener, err := net.ListenUDP("udp", address)
	if err != nil {
		log.Fatalf("ERROR: ListenUDP - %s", err)
	}

	parseTo(listener, false, In)
}

func tcpListener() {
	address, _ := net.ResolveTCPAddr("tcp", *tcpServiceAddress)
	log.Printf("listening on %s", address)
	listener, err := net.ListenTCP("tcp", address)
	if err != nil {
		log.Fatalf("ERROR: ListenTCP - %s", err)
	}
	defer listener.Close()

	for {
		conn, err := listener.AcceptTCP()
		if err != nil {
			log.Fatalf("ERROR: AcceptTCP - %s", err)
		}
		go parseTo(conn, true, In)
	}
}

func main() {
	flag.Parse()	
	if *showVersion {
		fmt.Printf("statsdaemon v%s (built w/%s)\n", VERSION, runtime.Version())
		return
	}

	signalchan = make(chan os.Signal, 1)
	signal.Notify(signalchan, syscall.SIGTERM)

	go udpListener()
	if *tcpServiceAddress != "" {
		go tcpListener()
	}
	var wg sync.WaitGroup
	wg.Add(2)
	h1 := subscriber.NewSubscriber(1 * time.Hour, wg, ":2005", *postfix)
	Subscriber = append(Subscriber, h1.In)
	m1 := subscriber.NewSubscriber(1 * time.Minute, wg, ":2004", *postfix)
	Subscriber = append(Subscriber, m1.In)
	go h1.Monitor()
	go m1.Monitor()
	monitor()
}
