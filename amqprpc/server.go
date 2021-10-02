package amqprpc

import (
	"fmt"
	"log"
	"sync"

	"github.com/streadway/amqp"
)

type Server struct {
	conn       *amqp.Connection
	ch         *amqp.Channel
	queues     map[string]amqp.Queue
	registries []*Registry
	exchange   string
	isDurable  bool
	mu         sync.Mutex
}

func NewServer(dsn, exchange string, isDurable bool) (srv *Server, err error) {
	srv = new(Server)
	srv.exchange = exchange
	srv.isDurable = isDurable

	if srv.conn, err = amqp.Dial(dsn); err != nil {
		return nil, err
	}

	if srv.ch, err = srv.conn.Channel(); err != nil {
		return nil, err
	}

	if err = srv.ch.ExchangeDeclare(
		srv.exchange,
		amqp.ExchangeHeaders,
		srv.isDurable,
		false,
		false,
		false,
		nil,
	); err != nil {
		return nil, err
	}

	srv.queues = make(map[string]amqp.Queue)
	return srv, nil
}

func (s *Server) Setup() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, r := range s.registries {
		for name, meth := range r.GetMethods() {
			if err := s.regMethod(name, meth); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *Server) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, r := range s.registries {
		for name, meth := range r.GetMethods() {
			if err := s.unregMethod(name, meth); err != nil {
				return err
			}
		}
	}

	for name, _ := range s.queues {
		if err := s.ch.QueueUnbind(name, "", s.exchange, nil); err != nil {
			return err
		}
	}

	if err := s.ch.Close(); err != nil {
		return err
	}

	return s.conn.Close()
}

func (s *Server) AddRegistry(reg *Registry) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.registries = append(s.registries, reg)
}

func (s *Server) regMethod(name string, meth Method) error {
	if err := meth.Setup(); err != nil {
		return err
	}

	q, err := s.ch.QueueDeclare(
		fmt.Sprintf("%s.%s", s.exchange, name), // name
		s.isDurable,                            // durable
		false,                                  // delete when unused
		false,                                  // exclusive
		false,                                  // no-wait
		nil,                                    // arguments
	)

	if err != nil {
		return err
	}

	s.queues[q.Name] = q

	if err := s.ch.QueueBind(
		q.Name,
		"",
		s.exchange,
		false,
		amqp.Table{},
	); err != nil {
		return err
	}

	return s.consume(q, meth)
}

func (s *Server) unregMethod(name string, meth Method) error {
	if err := meth.Cleanup(); err != nil {
		return err
	}
	return nil
}

func (s *Server) consume(q amqp.Queue, meth Method) error {
	msgs, err := s.ch.Consume(
		q.Name,         // queue
		meth.GetName(), // consumer
		false,          // auto-ack
		false,          // exclusive
		false,          // no-local
		false,          // no-wait
		nil,            // args
	)

	if err != nil {
		return err
	}

	go func(msgs <-chan amqp.Delivery) {
		for {
			select {
			case inMsg, ok := <-msgs:
				if !ok {
					break
				}

				outMsg := meth.Call(Message{inMsg.Body, inMsg.ContentType})

				if err := s.ch.Publish(
					"",            // exchange
					inMsg.ReplyTo, // routing key
					false,         // mandatory
					false,         // immediate
					amqp.Publishing{
						ContentType:   outMsg.ContentType,
						CorrelationId: inMsg.CorrelationId,
						Body:          outMsg.Body,
					}); err != nil {
					log.Println("Failed to publish a message")
				}
				inMsg.Ack(false)
			}
		}
	}(msgs)

	return nil
}
