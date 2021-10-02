package main

import (
	"amqprpc/amqprpc"
	"os"
	"strconv"
	"time"

	log "github.com/sirupsen/logrus"
)

func init() {
	log.SetOutput(os.Stdout)
	log.SetLevel(log.DebugLevel)
}

func main() {
	client, err := amqprpc.NewClient(&amqprpc.Config{
		Dsn:               "amqp://guest:guest@localhost:5672/",
		ClientTimeout:     10,
		ReconnectInterval: 5,
		Log:               log.StandardLogger(),
	})

	if err != nil {
		log.Fatal(err)
	}

	for i := 0; i < 10; i++ {
		msg, err := client.Call("rpc.method.test__echo", amqprpc.Message{Body: []byte("test" + strconv.Itoa(i)), ContentType: "text/plain"})
		time.Sleep(15 * time.Second)

		if err != nil {
			log.Fatal(err)
		}

		log.Println(string(msg))
	}

	if err := client.Close(); err != nil {
		log.Fatal(err)
	}
}
