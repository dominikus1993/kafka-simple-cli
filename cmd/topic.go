package cmd

import (
	"fmt"

	"github.com/dominikus1993/kafka-simple-cli/internal/kafka"
	"github.com/k0kubun/pp/v3"
	"github.com/urfave/cli/v2"
	"go.uber.org/zap"
)

func showTopicCommandAction(context *cli.Context) error {
	logger, _ := zap.NewProduction()
	defer logger.Sync()
	admin, err := kafka.NewKafkaAdmin(context.String("broker"))
	if err != nil {
		return err
	}
	meta, err := admin.DescribeTopics([]string{context.String("topic")})
	if err != nil {
		return err
	}
	if len(meta) == 1 {
		scheme := pp.ColorScheme{
			Integer: pp.Green | pp.Bold,
			Float:   pp.Black | pp.BackgroundWhite | pp.Bold,
			String:  pp.Yellow,
		}

		// Register it for usage
		pp.SetColorScheme(scheme)
		pp.Println(meta[0])
		return nil
	}
	return fmt.Errorf("number of topic informations should be 1")
}

func ShowTopicCommand() *cli.Command {
	return &cli.Command{
		Name: "show",
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
		},
		Aliases: []string{"s"},
		Usage:   "show topic info",
		Action:  showTopicCommandAction,
	}
}
