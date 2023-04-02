package kafka

import (
	"time"

	"github.com/segmentio/kafka-go"
)

func NewProducer(brokerList []string, topic string) (*kafka.Writer, error) {
	// For the access log, we are looking for AP semantics, with high throughput.
	// By creating batches of compressed messages, we reduce network I/O at a cost of more latency.
	producer := kafka.WriterConfig{
		Brokers:      brokerList,
		Topic:        topic,
		Balancer:     &kafka.LeastBytes{},
		BatchTimeout: 100 * time.Millisecond,
	}
	writer := kafka.NewWriter(producer)

	return writer, nil
}
