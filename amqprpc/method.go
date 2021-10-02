package amqprpc

type Method interface {
	GetName() string
	Call(msg Message) Message
	Setup() error
	Cleanup() error
}
