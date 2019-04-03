package main

import (
	"context"
	"fmt"
	"time"

	"cloud.google.com/go/pubsub"
)

func publish(cfg *config, batchName string, sleepTime time.Duration, number int) error {
	var counter int

	ctx := context.Background()

	client, err := pubsub.NewClient(ctx, cfg.projectID)
	if err != nil {
		return err
	}

	topic := client.Topic("queue1")

	for {
		mID := counter
		counter++

		msg := &pubsub.Message{
			Data:       []byte(fmt.Sprintf("Hello this is message %s-%d", batchName, mID)),
			Attributes: map[string]string{"batch": batchName, "id": fmt.Sprintf("%04d", mID)},
		}

		printMessage(msg)

		topic.Publish(ctx, msg)

		if counter == number {
			break
		}
		// time.Sleep(sleepTime)
	}

	topic.Stop()

	return err
}
