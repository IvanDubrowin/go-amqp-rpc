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
	conn    *amqp.Connection
	ch      *amqp.Channel
	queue   amqp.Queue
	calls   map[string]*call
	mu      sync.RWMutex
	timeout time.Duration
	done    chan struct{}
}

func NewClient(dsn string, timeout int) (cl *Client, err error) {
	cl = new(Client)
	cl.timeout = time.Duration(timeout) * time.Second

	if cl.conn, err = amqp.Dial(dsn); err != nil {
		return nil, err
	}

	if cl.ch, err = cl.conn.Channel(); err != nil {
		return nil, err
	}

	cl.done = make(chan struct{})
	cl.calls = make(map[string]*call)

	if cl.queue, err = cl.ch.QueueDeclare(
		"",    // name
		false, // durable
		false, // delete when unused
		true,  // exclusive
		false, // noWait
		nil,   // arguments
	); err != nil {
		return nil, err
	}

	msgs, err := cl.ch.Consume(
		cl.queue.Name, // queue
		"",            // consumer
		true,          // auto-ack
		false,         // exclusive
		false,         // no-local
		false,         // no-wait
		nil,           // args
	)

	if err != nil {
		return nil, err
	}

	go cl.handleDeliveries(msgs)
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
				break
			}
			call := cl.getCall(msg.CorrelationId)

			if call != nil {
				call.result = msg.Body
				call.done <- struct{}{}
			}

		case <-cl.done:
			break
		}

	}
}

func (cl *Client) Call(method string, msg Message) ([]byte, error) {
	corrId := makeCorrelationId()
	call := cl.makeCall(corrId)
	defer cl.removeCall(corrId)

	if err := cl.ch.Publish(
		"",     // exchange
		method, // routing key
		false,  // mandatory
		false,  // immediate
		amqp.Publishing{
			ContentType:   msg.ContentType,
			CorrelationId: corrId,
			ReplyTo:       cl.queue.Name,
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
	cl.done <- struct{}{}
	return cl.conn.Close()
}
