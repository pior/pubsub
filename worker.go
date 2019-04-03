package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"strings"
	"sync/atomic"
	"time"

	"cloud.google.com/go/pubsub"
)

type processorFunc func(*pubsub.Message) error

func simulatorProcessor() processorFunc {
	tmpErr := errors.New("tmp failed")
	permErr := errors.New("perm failed")

	return func(msg *pubsub.Message) error {
		if strings.HasSuffix(msg.ID, "00") {
			return permErr
		}

		processingTime := time.Millisecond * 1
		if rand.Float32() > 0.9 {
			processingTime = time.Second * 12
		}
		time.Sleep(processingTime)

		if rand.Float32() > 0.9 {
			return tmpErr
		}
		return nil
	}
}

type Task struct {
}

func worker(cfg *config, subscriptionName string, processor processorFunc) error {
	var receivedCounter, received int64
	var processedCounter int64
	var processing int64

	ctx := context.Background()

	client, err := pubsub.NewClient(ctx, cfg.projectID)
	if err != nil {
		return err
	}

	sub := client.Subscription(subscriptionName)
	sub.ReceiveSettings.MaxOutstandingMessages = 50
	sub.ReceiveSettings.MaxExtension = 20 * time.Second

	fmt.Printf("Started worker...\n")

	err = sub.Receive(ctx, func(ctx context.Context, msg *pubsub.Message) {
		received = atomic.AddInt64(&receivedCounter, 1)
		processing = received - atomic.LoadInt64(&processedCounter)

		log.Printf("[%4d] ---> %s processing=%d", received, msgID(msg), processing)

		msgErr := processor(msg)
		if msgErr != nil {
			log.Printf("[%4d] nack %s err=%q", received, msgID(msg), msgErr)
			msg.Nack()
		} else {
			log.Printf("[%4d]  ack %s", received, msgID(msg))
			msg.Ack()
		}

		atomic.AddInt64(&processedCounter, 1)
	})
	return err
}
