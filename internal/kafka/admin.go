package kafka

import (
	"fmt"

	"github.com/Shopify/sarama"
)

func NewKafkaAdmin(broker string) (sarama.ClusterAdmin, error) {
	fmt.Println(broker)
	config := sarama.NewConfig()
	admin, err := sarama.NewClusterAdmin([]string{broker}, config)
	if err != nil {
		return nil, fmt.Errorf("unable to create cluster admin; %w", err)
	}
	return admin, err
}
