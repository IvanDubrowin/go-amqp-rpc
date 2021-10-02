package amqprpc

import (
	"github.com/streadway/amqp"
)

type Config struct {
	Dsn               string
	Exchange          string
	IsDurable         bool
	PrefetchCount     int
	PrefetchSize      int
	QosGlobal         bool
	Log               Logger
	DialConfig        amqp.Config
	ReconnectInterval int
	ClientTimeout     int
}
