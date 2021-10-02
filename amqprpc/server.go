package amqprpc

import (
	"fmt"
	"sync"

	"github.com/streadway/amqp"
)

type Server struct {
	transport  *RobustTransport
	queues     map[string]amqp.Queue
	registries []*Registry
	mu         sync.Mutex
	config     *Config
	log        Logger
}

func NewServer(config *Config) (srv *Server, err error) {
	srv = new(Server)
	srv.config = config
	srv.log = NewLogWrapper(config.Log)
	srv.queues = make(map[string]amqp.Queue)

	if srv.transport, err = NewTransport(config); err != nil {
		return nil, err
	}

	return srv, nil
}

func (s *Server) Setup() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.transport.AddCleanupFunc(s.cleanup)
	return s.transport.AddSetupFunc(s.setup)
}

func (s *Server) setup(ch *amqp.Channel) error {
	for _, r := range s.registries {
		for name, meth := range r.GetMethods() {
			if err := s.regMethod(ch, name, meth); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *Server) cleanup(ch *amqp.Channel) error {
	for _, r := range s.registries {
		for name, meth := range r.GetMethods() {
			if err := s.unregMethod(name, meth); err != nil {
				return err
			}
		}
	}

	for name := range s.queues {
		if err := ch.QueueUnbind(name, "", s.config.Exchange, nil); err != nil {
			return err
		}
	}

	return nil
}

func (s *Server) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.transport.Close()
}

func (s *Server) AddRegistry(reg *Registry) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.registries = append(s.registries, reg)
}

func (s *Server) regMethod(ch *amqp.Channel, name string, meth Method) error {
	if err := meth.Setup(); err != nil {
		return err
	}

	q, err := ch.QueueDeclare(
		fmt.Sprintf("%s.%s", s.config.Exchange, name), // name
		s.config.IsDurable,                            // durable
		false,                                         // delete when unused
		false,                                         // exclusive
		false,                                         // no-wait
		nil,                                           // arguments
	)

	if err != nil {
		return err
	}

	s.queues[q.Name] = q

	if err := ch.QueueBind(
		q.Name,
		"",
		s.config.Exchange,
		false,
		amqp.Table{},
	); err != nil {
		return err
	}

	return s.consume(ch, q, meth)
}

func (s *Server) unregMethod(name string, meth Method) error {
	if err := meth.Cleanup(); err != nil {
		return err
	}
	return nil
}

func (s *Server) consume(ch *amqp.Channel, q amqp.Queue, meth Method) error {
	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		false,  // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)

	if err != nil {
		return err
	}

	go func(msgs <-chan amqp.Delivery) {
		for {
			select {
			case inMsg, ok := <-msgs:
				if !ok {
					s.log.Infof("Stopped consumer: %s", q.Name)
					return
				}

				outMsg := meth.Call(Message{inMsg.Body, inMsg.ContentType})

				if err := s.transport.Publish(
					"",            // exchange
					inMsg.ReplyTo, // routing key
					false,         // mandatory
					false,         // immediate
					amqp.Publishing{
						ContentType:   outMsg.ContentType,
						CorrelationId: inMsg.CorrelationId,
						Body:          outMsg.Body,
					}); err != nil {
					s.log.Errorf("Failed to publish a message: %+v, consumer: %s", outMsg, q.Name)
				}
				inMsg.Ack(false)
			}
		}
	}(msgs)

	return nil
}
