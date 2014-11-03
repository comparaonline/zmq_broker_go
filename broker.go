package main

import (
	"flag"
	zmq "github.com/pebbe/zmq4"
	"log"
	"time"
)

var (
	ping_wait    int
	pub_address  string
	sub_address  string
	mon_address  string
	ping_address string
	ping_channel string
	ping_message string
)

func init() {
	flag.IntVar(&ping_wait, "ping_wait", 60, "wait seconds between sending each ping")
	flag.StringVar(&pub_address, "pub_address", "tcp://*:5556", "address to bind for publishing messages")
	flag.StringVar(&sub_address, "sub_address", "tcp://*:5555", "address to bind for receiving messages")
	flag.StringVar(&mon_address, "mon_address", "ipc://zmq_broker_monitor.ipc", "address to bind for monitoring messages")
	flag.StringVar(&ping_address, "ping_address", "ipc://zmq_broker_pinger.ipc", "address to bind for receiving ping messages")
	flag.StringVar(&ping_channel, "ping_channel", "ping", "the channel to send ping messages")
	flag.StringVar(&ping_message, "ping_message", "{\"action\": \"ping\"}", "the message to send via ping channel")
	flag.Parse()
}

func main() {
	ticker := time.NewTicker(time.Second * time.Duration(ping_wait))

	log.Println("preparing sockets...")

	// setup sockets
	xpub, _ := zmq.NewSocket(zmq.XPUB)
	pinger, _ := zmq.NewSocket(zmq.PUB)
	xsub, _ := zmq.NewSocket(zmq.XSUB)
	monitor, _ := zmq.NewSocket(zmq.SUB)
	for _, socket := range []*zmq.Socket{xpub, pinger, xsub, monitor} {
		defer socket.Close()
	}

	initPubSub(xpub, xsub)
	initMonitoring(xpub, monitor)
	initPinger(pinger, xsub)
	log.Println("sockets prepared")

	time.Sleep(time.Second)
	log.Println("starting message monitoring")
	go startMonitorLoop(monitor)

	time.Sleep(time.Second)
	log.Println("starting XSUB/XPUB broker")
	go zmq.Proxy(xsub, xpub, nil)

	time.Sleep(time.Second)
	log.Println("starting ping messages loop")
	startPingLoop(ticker, pinger)
}

func initPubSub(xpub, xsub *zmq.Socket) {
	if err := xpub.Bind(pub_address); err != nil {
		log.Panic(err)
	}

	if err := xsub.Bind(sub_address); err != nil {
		log.Panic(err)
	}
}

func initMonitoring(xpub, monitor *zmq.Socket) {
	if err := xpub.Bind(mon_address); err != nil {
		log.Panic(err)
	}

	monitor.SetSubscribe("")
	if err := monitor.Connect(mon_address); err != nil {
		log.Panic(err)
	}
}

func initPinger(pinger, xsub *zmq.Socket) {
	if err := pinger.Bind(ping_address); err != nil {
		log.Panic(err)
	}
	if err := xsub.Connect(ping_address); err != nil {
		log.Panic(err)
	}
}

func startMonitorLoop(monitor *zmq.Socket) {
	for {
		content, _ := monitor.RecvMessage(0)
		log.Println("publishing message: \"" + content[1] + "\" on channel: \"" + content[0] + "\"")
	}
}

func startPingLoop(t *time.Ticker, pinger *zmq.Socket) {
	for _ = range t.C {
		pinger.SendMessage(ping_channel, ping_message)
	}
}
