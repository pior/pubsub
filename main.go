package main

import (
	"fmt"
	"log"
	"os"
	"time"

	"cloud.google.com/go/pubsub"
)

type config struct {
	projectID string
}

func printMessage(msg *pubsub.Message) {
	fmt.Printf("Msg %s: Data: %q PublishTime: %s Attributes: %+v\n",
		msg.ID, string(msg.Data), msg.PublishTime, msg.Attributes)
}

func msgToString(msg *pubsub.Message) string {
	return fmt.Sprintf("id:%s data:%q time:%s attr:%+v\n", msg.ID, string(msg.Data), msg.PublishTime, msg.Attributes)
}

func msgID(msg *pubsub.Message) string {
	return fmt.Sprintf("%s-%s-%s", msg.Attributes["batch"], msg.Attributes["id"], msg.ID)
}

func main() {
	var err error

	cfg := &config{
		projectID: "piortest",
	}

	switch os.Args[1] {
	case "monitor":
		err = monitor(cfg)

	case "publish":
		err = publish(cfg, os.Args[2], time.Millisecond*10, 100)

	case "worker":
		err = worker(cfg, "sub1", simulatorProcessor())
	}

	if err != nil {
		log.Fatalf("Error: %s", err)
	}
}
