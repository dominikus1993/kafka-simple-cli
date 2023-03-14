package kafka

import (
	"strings"

	"github.com/Shopify/sarama"
	"go.uber.org/zap"
)

func NewConsumer(broker, topic, group, offsetreset string) (sarama.ConsumerGroup, error) {
	config := sarama.NewConfig()
	config.ClientID = group
	if offsetreset == "earliest" {
		config.Consumer.Offsets.Initial = sarama.OffsetNewest
	}
	if offsetreset == "latest" {
		config.Consumer.Offsets.Initial = sarama.OffsetOldest
	}
	return sarama.NewConsumerGroup(strings.Split(broker, ","), group, config)
}

// Consumer represents a Sarama consumer group consumer
type Consumer struct {
	Ready  chan bool
	logger *zap.Logger
}

func NewKafkaConsumer(ready chan bool, logger *zap.Logger) *Consumer {
	return &Consumer{Ready: ready, logger: logger}
}

func (consumer *Consumer) SetReady(ready chan bool) {
	consumer.Ready = ready
}

// Setup is run at the beginning of a new session, before ConsumeClaim
func (consumer *Consumer) Setup(sarama.ConsumerGroupSession) error {
	// Mark the consumer as ready
	close(consumer.Ready)
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (consumer *Consumer) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
func (consumer *Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	// NOTE:
	// Do not move the code below to a goroutine.
	// The `ConsumeClaim` itself is called within a goroutine, see:
	// https://github.com/Shopify/sarama/blob/main/consumer_group.go#L27-L29
	for {
		select {
		case message := <-claim.Messages():
			consumer.logger.With(zap.String("message", string(message.Value)), zap.Time("timestamp", message.Timestamp), zap.String("topic", message.Topic)).Info("Message claimed")
			session.MarkMessage(message, "")

		// Should return when `session.Context()` is done.
		// If not, will raise `ErrRebalanceInProgress` or `read tcp <ip>:<port>: i/o timeout` when kafka rebalance. see:
		// https://github.com/Shopify/sarama/issues/1192
		case <-session.Context().Done():
			return nil
		}
	}
}
