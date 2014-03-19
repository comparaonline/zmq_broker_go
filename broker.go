package main

import (
	"flag"
	zmq "github.com/alecthomas/gozmq"
	"log"
	"time"
)

var (
	ping_wait       int
	pub_address     string
	sub_address     string
	monitor_address string
	ping_channel    string
	ping_message    string
)

func init() {
	flag.IntVar(&ping_wait, "ping_wait", 60, "wait seconds between sending each ping")
	flag.StringVar(&pub_address, "pub_address", "tcp://*:5556", "address to bind for publishing messages")
	flag.StringVar(&sub_address, "sub_address", "tcp://*:5555", "address to bind for receiving messages")
	flag.StringVar(&monitor_address, "monitor_address", "ipc://zmq_broker_monitor.ipc", "address to bind for monitoring messages")
	flag.StringVar(&ping_channel, "ping_channel", "ping", "the channel to send ping messages")
	flag.StringVar(&ping_message, "ping_message", "{\"action\": \"ping\"}", "the message to send via ping channel")
	flag.Parse()
}

func main() {
	ticker := time.NewTicker(time.Second * time.Duration(ping_wait))

	context, _ := zmq.NewContext()
	defer context.Close()

	log.Println("preparing sockets...")

	// Socket to receive signals
	subscriber, _ := context.NewSocket(zmq.XSUB)
	defer subscriber.Close()
	if err := subscriber.Bind(sub_address); err != nil {
		log.Panic(err)
	}

	// Socket to talk to clients
	publisher, _ := context.NewSocket(zmq.XPUB)
	defer publisher.Close()
	for _, addr := range []string{pub_address, monitor_address} {
		if err := publisher.Bind(addr); err != nil {
			log.Panic(err)
		}
	}

	// setup monitoring socket
	monitor, _ := context.NewSocket(zmq.SUB)
	defer monitor.Close()
	monitor.SetSubscribe("")
	if err := monitor.Connect(monitor_address); err != nil {
		log.Panic(err)
	}

	log.Println("sockets prepared")

	log.Println("starting message monitoring")
	go monitorLoop(monitor)

	// give time for monitoring socket to prepare
	time.Sleep(time.Second)

	log.Println("starting ping messages")
	go pingLoop(ticker, publisher)

	log.Println("starting xsub/xpub proxy")
	zmq.Proxy(subscriber, publisher, nil)
}

func monitorLoop(mon *zmq.Socket) {
	for {
		channel, _ := mon.Recv(0)
		content, _ := mon.Recv(0)

		log.Println("publishing message: \"" + string(content) + "\" on channel: \"" + string(channel) + "\"")
	}
}

func pingLoop(t *time.Ticker, pub *zmq.Socket) {
	for _ = range t.C {
		// log.Println("sending ping message")
		pub.SendMultipart([][]byte{[]byte(ping_channel), []byte(ping_message)}, 0)
	}
}
