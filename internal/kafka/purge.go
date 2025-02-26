package kafka

import (
	"fmt"

	"github.com/IBM/sarama"
)

func DeleteRecords(broker, topic string, retentionMs string) error {
	admin, err := NewKafkaAdmin(broker)
	if err != nil {
		return fmt.Errorf("unable to create cluster admin; %w", err)
	}
	defer admin.Close()

	// Set the retention policy to delete records older than the specified duration
	config := map[string]*string{
		"retention.ms": &retentionMs,
	}

	err = admin.AlterConfig(sarama.TopicResource, topic, config, false)
	if err != nil {
		return fmt.Errorf("unable to alter topic config; %w", err)
	}

	return nil
}
