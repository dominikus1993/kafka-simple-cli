package cmd

import (
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/Shopify/sarama"
	"github.com/dominikus1993/kafka-simple-cli/internal/kafka"
	kafkago "github.com/segmentio/kafka-go"
	"github.com/urfave/cli/v2"
	"go.uber.org/zap"
)

func close(producer *kafkago.Writer, logger *zap.Logger) {
	if err := producer.Close(); err != nil {
		logger.With(zap.Error(err)).Error("failed to shut down data collector cleanly")
	}
}

func getMessage(context *cli.Context, loggger *zap.Logger) (sarama.StringEncoder, error) {
	msg := context.String("message")
	if msg != "" {
		return sarama.StringEncoder(msg), nil
	}
	file := context.String("json")
	if file != "" {
		jsonFile, err := os.Open(file)
		if err != nil {
			fmt.Println(err)
		}
		defer jsonFile.Close()

		byteValue, err := io.ReadAll(jsonFile)
		if err != nil {
			return "", err
		}
		return sarama.StringEncoder(string(byteValue)), nil
	}

	return "", errors.New("no message or json provided")
}

func publishTopicCommandAction(context *cli.Context) error {
	logger, _ := zap.NewProduction()
	defer logger.Sync()
	producer, err := kafka.NewProducer(strings.Split(context.String("broker"), ","), context.String("topic"))
	if err != nil {
		return err
	}

	defer close(producer, logger)

	message := kafkago.Message{
		Value: []byte("exampleValue"),
	}
	err = producer.WriteMessages(context.Context, message)
	if err != nil {
		return fmt.Errorf("failed to sent message; %w", err)
	}

	logger.Info("message sent")
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
				Usage:    "kafka consumer group id",
				Required: false,
			},
			&cli.StringFlag{
				Name:     "json",
				Usage:    "./consume.json",
				Required: false,
			},
		},
		Aliases: []string{"p"},
		Usage:   "publish message to specyfied topic ",
		Action:  publishTopicCommandAction,
	}
}
