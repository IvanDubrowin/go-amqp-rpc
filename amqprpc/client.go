package amqprpc

import (
	"errors"
	"sync"
	"time"

	"github.com/streadway/amqp"
)

var ErrTimeout = errors.New("rpc call raise timeout error")

type call struct {
	done   chan struct{}
	result []byte
}

type Client struct {
	calls     map[string]*call
	mu        sync.RWMutex
	timeout   time.Duration
	done      chan struct{}
	transport *RobustTransport
	log       Logger
	queue     string
}

func NewClient(config *Config) (cl *Client, err error) {
	cl = new(Client)
	cl.timeout = time.Duration(config.ClientTimeout) * time.Second
	cl.done = make(chan struct{})
	cl.calls = make(map[string]*call)
	cl.queue = makeRandomQueueName()
	cl.log = NewLogWrapper(config.Log)

	if cl.transport, err = NewTransport(config); err != nil {
		return nil, err
	}

	cl.transport.AddSetupFunc(cl.setup)
	return cl, nil
}

func (cl *Client) getCall(msgId string) *call {
	cl.mu.RLock()
	defer cl.mu.RUnlock()
	call, ok := cl.calls[msgId]
	if !ok {
		return nil
	}
	return call
}

func (cl *Client) removeCall(msgId string) {
	cl.mu.Lock()
	defer cl.mu.Unlock()
	delete(cl.calls, msgId)
}

func (cl *Client) makeCall(msgId string) *call {
	cl.mu.Lock()
	defer cl.mu.Unlock()
	c := &call{done: make(chan struct{})}
	cl.calls[msgId] = c
	return c
}

func (cl *Client) handleDeliveries(msgs <-chan amqp.Delivery) {
	for {
		select {
		case msg, ok := <-msgs:
			if !ok {
				cl.log.Info("handleDeliveries stopped")
				return
			}
			call := cl.getCall(msg.CorrelationId)

			if call != nil {
				call.result = msg.Body
				call.done <- struct{}{}
			}

		case <-cl.done:
			return
		}

	}
}

func (cl *Client) setup(ch *amqp.Channel) error {
	queue, err := ch.QueueDeclare(
		cl.queue, // name
		false,    // durable
		false,    // delete when unused
		true,     // exclusive
		false,    // noWait
		nil,      // arguments
	)

	if err != nil {
		return err
	}

	msgs, err := ch.Consume(
		queue.Name, // queue
		"",         // consumer
		true,       // auto-ack
		false,      // exclusive
		false,      // no-local
		false,      // no-wait
		nil,        // args
	)

	if err != nil {
		return err
	}

	go cl.handleDeliveries(msgs)
	return nil
}

func (cl *Client) Call(method string, msg Message) ([]byte, error) {
	cl.log.Infof("Call rpc method: %s, message: %+v", method, msg)
	corrId := makeCorrelationId()
	call := cl.makeCall(corrId)
	defer cl.removeCall(corrId)

	if err := cl.transport.Publish(
		"",     // exchange
		method, // routing key
		false,  // mandatory
		false,  // immediate
		amqp.Publishing{
			ContentType:   msg.ContentType,
			CorrelationId: corrId,
			ReplyTo:       cl.queue,
			Body:          msg.Body,
		}); err != nil {
		return nil, err
	}

	select {
	case <-call.done:
		return call.result, nil
	case <-time.After(cl.timeout):
		return nil, ErrTimeout
	}
}

func (cl *Client) Close() error {
	cl.log.Info("Close rpc client")
	cl.done <- struct{}{}
	return cl.transport.Close()

}
