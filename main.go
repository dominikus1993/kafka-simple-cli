package main

import (
	"os"

	"github.com/dominikus1993/kafka-simple-cli/cmd"
	"github.com/urfave/cli/v2"
	"go.uber.org/zap"
)

func main() {
	logger, _ := zap.NewProduction()
	defer logger.Sync()
	app := &cli.App{
		Name: "kafka-cli",
		Commands: []*cli.Command{
			cmd.ShowTopicCommand(),
		},
	}
	err := app.Run(os.Args)
	if err != nil {
		logger.With(zap.Error(err)).Error("error running app")
	}
}
