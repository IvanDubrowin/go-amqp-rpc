package amqprpc

import (
	"github.com/google/uuid"
)

func makeCorrelationId() string {
	return uuid.NewString()
}
