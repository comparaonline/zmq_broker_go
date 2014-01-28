package main

import (
	"encoding/json"
	"flag"
	zmq "github.com/alecthomas/gozmq"
	"log"
	"time"
)

var (
	ping_wait    int
	pub_address  string
	sub_address  string
	ping_channel string
	ping_message string
	pub_hwm      int
	sub_hwm      int
)

func init() {
	flag.IntVar(&ping_wait, "ping_wait", 60, "wait seconds between sending each ping")
	flag.StringVar(&pub_address, "pub_address", "tcp://*:5556", "address to bind for publishing messages")
	flag.StringVar(&sub_address, "sub_address", "tcp://*:5555", "address to bind for receiving messages")
	flag.StringVar(&ping_channel, "ping_channel", "ping", "the channel to send ping messages")
	flag.StringVar(&ping_message, "ping_message", "{\"action\": \"ping\"}", "the message to send via ping channel")
	flag.IntVar(&pub_hwm, "pub_hwm", 5, "pub socket high water mark")
	flag.IntVar(&sub_hwm, "sub_hwm", 5, "sub socket high water mark")
	flag.Parse()
}

func main() {
	ticker := time.NewTicker(time.Second * time.Duration(ping_wait))

	context, _ := zmq.NewContext()
	defer context.Close()

	// Socket to receive signals
	subscriber, _ := context.NewSocket(zmq.SUB)
	defer subscriber.Close()
	subscriber.SetRcvHWM(sub_hwm)
	subscriber.SetSubscribe("")
	if err := subscriber.Bind(sub_address); err != nil {
		log.Panic(err)
	}

	// Socket to talk to clients
	publisher, _ := context.NewSocket(zmq.PUB)
	defer publisher.Close()
	publisher.SetHWM(pub_hwm)
	if err := publisher.Bind(pub_address); err != nil {
		log.Panic(err)
	}

	// wait a little before sending messages
	time.Sleep(time.Second)

	// send ping messages async
	go ping_loop(ticker, publisher)

	// read messages on this goroutine
	messages_loop(publisher, subscriber)
}

func ping_loop(t *time.Ticker, pub *zmq.Socket) {
	for _ = range t.C {
		log.Println("sending ping message")
		pub.SendMultipart([][]byte{[]byte("ping"), []byte(ping_message)}, 0)
	}
}

func messages_loop(pub, sub *zmq.Socket) {
	var envelope map[string]interface{}
	for {
		content, _ := sub.Recv(0)

		if err := json.Unmarshal(content, &envelope); err != nil {
			log.Println(err)
			continue
		}

		_, present := envelope["channel"]
		if !present {
			log.Println("channel missing")
			continue
		}
		channel := envelope["channel"].(string)

		body, err := json.Marshal(envelope["body"])
		if err != nil {
			log.Println(err)
			continue
		}

		log.Println("publishing message " + string(body) + " on channel " + channel)
		pub.SendMultipart([][]byte{[]byte(channel), body}, 0)
	}
}
