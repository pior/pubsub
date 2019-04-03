package main

import (
	"context"
	"fmt"
	"sync"

	"cloud.google.com/go/pubsub"
)

func monitor(cfg *config) error {
	var mu sync.Mutex
	received := 0
	maxMessage := 1000 * 1000 * 1000

	ctx := context.Background()

	client, err := pubsub.NewClient(ctx, cfg.projectID)
	if err != nil {
		return err
	}

	sub := client.Subscription("monitor")

	cctx, cancel := context.WithCancel(ctx)

	fmt.Printf("Start monitoring messages...\n")

	err = sub.Receive(cctx, func(ctx context.Context, msg *pubsub.Message) {
		msg.Ack()

		printMessage(msg)

		mu.Lock()
		defer mu.Unlock()
		received++
		if received == maxMessage {
			cancel()
		}
	})
	return err
}
