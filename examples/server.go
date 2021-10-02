package main

import (
	"amqprpc/amqprpc"
	"log"
	"os"
	"os/signal"
)

type EchoMethod struct{}

func (m *EchoMethod) GetName() string {
	return "echo"
}

func (m *EchoMethod) Setup() error {
	return nil
}

func (m *EchoMethod) Cleanup() error {
	return nil
}

func (m *EchoMethod) Call(msg amqprpc.Message) amqprpc.Message {
	log.Println(string(msg.Body))
	return msg
}

func main() {
	server, err := amqprpc.NewServer("amqp://guest:guest@localhost:5672/", "rpc", true)

	if err != nil {
		log.Fatal(err)
	}

	meth := new(EchoMethod)
	registry := amqprpc.NewRegistry("test")
	registry.AddMethod(meth)
	server.AddRegistry(registry)

	if err := server.Setup(); err != nil {
		log.Fatal(err)
	}

	ch := make(chan os.Signal, 1)
	signal.Notify(ch, os.Interrupt)
	<-ch

	if err := server.Close(); err != nil {
		log.Fatal(err)
	}
}
