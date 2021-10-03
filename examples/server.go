package main

import (
	"amqprpc/amqprpc"
	"os"
	"os/signal"

	log "github.com/sirupsen/logrus"
)

func init() {
	log.SetOutput(os.Stdout)
	log.SetLevel(log.DebugLevel)
	log.SetFormatter(&log.TextFormatter{
		DisableColors:   true,
		TimestampFormat: "2006-01-02 15:04:05",
		FullTimestamp:   true,
	})
}

type Args struct {
	A int `msgpack:"a"`
	B int `msgpack:"b"`
}

type Result struct {
	Result int `msgpack:"result"`
}

type MultiplyMethod struct {
	serializer amqprpc.Serializer
}

func (m *MultiplyMethod) GetName() string {
	return "multiply"
}

func (m *MultiplyMethod) Setup(serializer amqprpc.Serializer) error {
	m.serializer = serializer
	return nil
}

func (m *MultiplyMethod) Cleanup() error {
	return nil
}

func (m *MultiplyMethod) Call(body []byte) (interface{}, *amqprpc.RPCError) {
	var params Args
	if err := m.serializer.Unmarshal(body, &params); err != nil {
		return nil, &amqprpc.RPCError{Type: "Unmarshal error", Message: err.Error()}
	}
	res := params.A * params.B
	log.Infof("Result: %d", res)
	return &Result{Result: res}, nil
}

func main() {
	server, err := amqprpc.NewServer(&amqprpc.Config{
		Dsn:               "amqp://guest:guest@localhost:5672/",
		Exchange:          "rpc.method",
		IsDurable:         true,
		ReconnectInterval: 5,
		Log:               log.StandardLogger(),
		PrefetchCount:     100,
	})

	if err != nil {
		log.Fatal(err)
	}

	meth := new(MultiplyMethod)
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
