package amqprpc

import "fmt"

type RPCError struct {
	Type    string `json,msgpack:"type"`
	Message string `json,msgpack:"message"`
	Args    string `json,msgpack:"args"`
}

func (e *RPCError) Error() string {
	return fmt.Sprintf("type: %s, message: %s, args: %s", e.Type, e.Message, e.Args)
}

type Method interface {
	GetName() string
	Call(body []byte) (interface{}, *RPCError)
	Setup(serializer Serializer) error
	Cleanup() error
}
