package cmd

import (
	"fmt"
	"log"

	"github.com/dominikus1993/kafka-simple-cli/internal/kafka"
	"github.com/urfave/cli/v2"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

func consumeTopicCommandAction(context *cli.Context) error {
	logger, _ := zap.NewProduction()
	defer logger.Sync()

	consumer, err := kafka.NewConsumer(context.String("broker"), context.String("topic"), context.String("groupid"))
	if err != nil {
		return err
	}
	wg, ctx := errgroup.WithContext(context.Context)
	ready := make(chan bool)
	kafkaconsumer := kafka.NewKafkaConsumer(ready, logger)
	if err != nil {
		return err
	}
	wg.Go(func() error {
		for {
			// `Consume` should be called inside an infinite loop, when a
			// server-side rebalance happens, the consumer session will need to be
			// recreated to get the new claims
			if err := consumer.Consume(ctx, []string{context.String("topic")}, kafkaconsumer); err != nil {
				log.Panicf("Error from consumer: %v", err)
			}
			// check if context was cancelled, signaling that the consumer should stop
			if ctx.Err() != nil {
				return ctx.Err()
			}
			kafkaconsumer.SetReady(make(chan bool))
		}
	})

	<-kafkaconsumer.Ready // Await till the consumer has been set up
	logger.Info("Sarama consumer up and running!...")

	if err = wg.Wait(); err != nil {
		return fmt.Errorf("error in some async task: %w", err)
	}
	if err = consumer.Close(); err != nil {
		return fmt.Errorf("error closing client: %w", err)
	}
	return nil
}

func ConsumeTopicCommand() *cli.Command {
	return &cli.Command{
		Name: "consume",
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
				Name:     "groupid",
				Value:    "xd",
				Usage:    "kafka consumer group id",
				Required: true,
			},
		},
		Aliases: []string{"cs"},
		Usage:   "consume topic ",
		Action:  consumeTopicCommandAction,
	}
}
