package kafka

import (
	"bytes"
	"encoding/json"
	"strings"

	"github.com/IBM/sarama"
	"github.com/k0kubun/pp/v3"
)

func NewConsumer(broker, topic, group, offsetreset string) (sarama.ConsumerGroup, error) {
	config := sarama.NewConfig()
	config.ClientID = group
	if offsetreset == "earliest" {
		config.Consumer.Offsets.Initial = sarama.OffsetOldest
	}
	if offsetreset == "latest" {
		config.Consumer.Offsets.Initial = sarama.OffsetNewest
	}
	return sarama.NewConsumerGroup(strings.Split(broker, ","), group, config)
}

// Consumer represents a Sarama consumer group consumer
type Consumer struct {
	Ready chan bool
	Key   string
}

func NewKafkaConsumer(ready chan bool, key string) *Consumer {
	return &Consumer{Ready: ready, Key: key}
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
	// https://github.com/IBM/sarama/blob/main/consumer_group.go#L27-L29
	for {
		select {
		case message := <-claim.Messages():
			if consumer.Key == "" {
				printMessage(message)
			} else if consumer.Key == string(message.Key) {
				printMessage(message)
			}
			session.MarkMessage(message, "")

		// Should return when `session.Context()` is done.
		// If not, will raise `ErrRebalanceInProgress` or `read tcp <ip>:<port>: i/o timeout` when kafka rebalance. see:
		// https://github.com/IBM/sarama/issues/1192
		case <-session.Context().Done():
			return nil
		}
	}
}

func printMessage(message *sarama.ConsumerMessage) {
	jsonDecoder := json.NewDecoder(bytes.NewReader(message.Value))
	jsonDecoder.UseNumber() // enable useNumber for preventing float

	var decodedData map[string]interface{}
	err := jsonDecoder.Decode(&decodedData)
	if err != nil {
		panic(err)
	}

	data := map[string]any{
		"key":       string(message.Key),
		"value":     decodedData,
		"timestamp": message.Timestamp,
		"topic":     message.Topic,
		"headers":   message.Headers,
		"offset":    message.Offset,
		"partition": message.Partition,
	}
	pp.Println(data)
}
