package main

import (
	"amqprpc/amqprpc"
	"log"
)

func main() {
	client, err := amqprpc.NewClient("amqp://guest:guest@localhost:5672/", 10)

	if err != nil {
		log.Fatal(err)
	}

	msg, err := client.Call("rpc.test__echo", amqprpc.Message{Body: []byte("test"), ContentType: "text/plain"})

	if err != nil {
		log.Fatal(err)
	}

	log.Println(string(msg))

	if err := client.Close(); err != nil {
		log.Fatal(err)
	}
}
