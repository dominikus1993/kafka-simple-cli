package cmd

import (
	"fmt"
	"strings"

	"github.com/Shopify/sarama"
	"github.com/dominikus1993/kafka-simple-cli/internal/kafka"
	"github.com/urfave/cli/v2"
	"go.uber.org/zap"
)

func close(producer sarama.SyncProducer, logger *zap.Logger) {
	if err := producer.Close(); err != nil {
		logger.With(zap.Error(err)).Error("failed to shut down data collector cleanly")
	}
}
func publishTopicCommandAction(context *cli.Context) error {
	logger, _ := zap.NewProduction()
	defer logger.Sync()
	producer, err := kafka.NewProducer(strings.Split(context.String("broker"), ","))
	if err != nil {
		return err
	}

	defer close(producer, logger)

	partition, offset, err := producer.SendMessage(&sarama.ProducerMessage{Topic: context.String("topic"), Value: sarama.StringEncoder(context.String("message"))})
	if err != nil {
		return fmt.Errorf("failed to sent message; %w", err)
	}

	logger.With(zap.Int32("partition", partition), zap.Int64("offset", offset)).Info("message sent")
	return nil
}

func PublishTopicCommand() *cli.Command {
	return &cli.Command{
		Name: "publish",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:     "broker",
				Value:    "broker:9092",
				Usage:    "kafka broker addres",
				Required: true,
			},
			&cli.StringFlag{
				Name:     "topic",
				Value:    "topic-name",
				Usage:    "topic-name",
				Required: true,
			},
			&cli.StringFlag{
				Name:     "message",
				Value:    "xd",
				Usage:    "kafka consumer group id",
				Required: true,
			},
		},
		Aliases: []string{"p"},
		Usage:   "publish message to specyfied topic ",
		Action:  publishTopicCommandAction,
	}
}
