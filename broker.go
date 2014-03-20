package main

import (
	"flag"
	zmq "github.com/alecthomas/gozmq"
	"log"
	"time"
)

var (
	ping_wait     int
	pub_address   string
	sub_address   string
	mon_address   string
	front_address string
	back_address  string
	ping_channel  string
	ping_message  string
)

func init() {
	flag.IntVar(&ping_wait, "ping_wait", 60, "wait seconds between sending each ping")
	flag.StringVar(&pub_address, "pub_address", "tcp://*:5556", "address to bind for publishing messages")
	flag.StringVar(&sub_address, "sub_address", "tcp://*:5555", "address to bind for receiving messages")
	flag.StringVar(&mon_address, "mon_address", "ipc://zmq_broker_monitor.ipc", "address to bind for monitoring messages")
	flag.StringVar(&front_address, "front_address", "tcp://*:5559", "address to bind for clients")
	flag.StringVar(&back_address, "back_address", "tcp://*:5560", "address to bind for workers")
	flag.StringVar(&ping_channel, "ping_channel", "ping", "the channel to send ping messages")
	flag.StringVar(&ping_message, "ping_message", "{\"action\": \"ping\"}", "the message to send via ping channel")
	flag.Parse()
}

func main() {
	ticker := time.NewTicker(time.Second * time.Duration(ping_wait))

	ctx, _ := zmq.NewContext()
	defer ctx.Close()

	log.Println("preparing sockets...")

	// setup sockets
	pub, _ := ctx.NewSocket(zmq.XPUB)
	sub, _ := ctx.NewSocket(zmq.XSUB)
	mon, _ := ctx.NewSocket(zmq.SUB)
	front, _ := ctx.NewSocket(zmq.ROUTER)
	back, _ := ctx.NewSocket(zmq.DEALER)
	for _, socket := range []*zmq.Socket{pub, sub, mon, front, back} {
		defer socket.Close()
	}

	initPubSub(pub, sub)
	initMonitoring(pub, mon)
	initReqRepBroker(front, back)

	log.Println("sockets prepared")

	log.Println("starting message monitoring")
	go startMonitorLoop(mon)

	// give time for monitoring socket to prepare
	time.Sleep(time.Second)

	log.Println("starting ROUTER/DEALER broker")
	go startReqRepBroker(front, back)

	log.Println("starting XSUB/XPUB broker")
	go zmq.Proxy(sub, pub, nil)

	log.Println("starting ping messages loop")
	startPingLoop(ticker, pub)
}

func initPubSub(pub, sub *zmq.Socket) {
	if err := pub.Bind(pub_address); err != nil {
		log.Panic(err)
	}

	if err := sub.Bind(sub_address); err != nil {
		log.Panic(err)
	}
}

func initMonitoring(pub, mon *zmq.Socket) {
	if err := pub.Bind(mon_address); err != nil {
		log.Panic(err)
	}

	mon.SetSubscribe("")
	if err := mon.Connect(mon_address); err != nil {
		log.Panic(err)
	}
}

func initReqRepBroker(front, back *zmq.Socket) {
	if err := front.Bind(front_address); err != nil {
		log.Panic(err)
	}

	if err := back.Bind(back_address); err != nil {
		log.Panic(err)
	}
}

func startMonitorLoop(mon *zmq.Socket) {
	for {
		channel, _ := mon.Recv(0)
		content, _ := mon.Recv(0)

		log.Println("publishing message: \"" + string(content) + "\" on channel: \"" + string(channel) + "\"")
	}
}

func startPingLoop(t *time.Ticker, pub *zmq.Socket) {
	for _ = range t.C {
		// log.Println("sending ping message")
		pub.SendMultipart([][]byte{[]byte(ping_channel), []byte(ping_message)}, 0)
	}
}

func startReqRepBroker(front, back *zmq.Socket) {
	// Initialize poll set
	toPoll := zmq.PollItems{
		zmq.PollItem{Socket: front, Events: zmq.POLLIN},
		zmq.PollItem{Socket: back, Events: zmq.POLLIN},
	}

	for {
		_, _ = zmq.Poll(toPoll, -1)

		switch {
		case toPoll[0].REvents&zmq.POLLIN != 0:
			parts, _ := front.RecvMultipart(0)
			back.SendMultipart(parts, 0)

		case toPoll[1].REvents&zmq.POLLIN != 0:
			parts, _ := back.RecvMultipart(0)
			front.SendMultipart(parts, 0)
		}
	}
}
