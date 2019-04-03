package main

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"cloud.google.com/go/pubsub"

	uuid "github.com/satori/go.uuid"
)

type JobType func() Job

type JobRegistry struct {
	jobTypes map[string]JobType
}

func (r *JobRegistry) Register(jobType JobType) {
	jobName := jobType().Name()
	if jobName == "" {
		panic(fmt.Sprintf("Job %t.Name() cannot be empty string", jobType))
	}
	r.jobTypes[jobType().Name()] = jobType
}

func (r *JobRegistry) Get(name string) (JobType, error) {
	jobType, ok := r.jobTypes[name]
	if !ok {
		return nil, fmt.Errorf("JobType %q does not exist", name)
	}
	return jobType, nil
}

type Job interface {
	Name() string
	Unmasrshal([]byte) error
	Marshal() ([]byte, error)
	Perform(context.Context) error
}

type JobRequest struct {
	RequestID   uuid.UUID
	RequestTime time.Time

	// TracingContext  opencensus stuff

	JobName string
	JobData []byte
}

// Runner enqueues jobs
type Runner struct {
	client    *pubsub.Client
	topicName string
}

// client, err := pubsub.NewClient(ctx, cfg.projectID)
// if err != nil {
// 	return nil, err
// }

// func NewRunner(pubsubClient *pubsub.Client, topicName string) (*Runner, error) {
// 	runner := &Runner{
// 		client: pubsubClient,
// 		topicName: topicName,
// 	}
// 	return runner, nil
// }

func (r *Runner) Enqueue(ctx context.Context, job Job) error {
	jobData, err := job.Marshal()
	if err != nil {
		return err
	}

	request := &JobRequest{
		RequestID:   uuid.NewV1(),
		RequestTime: time.Now(),

		JobName: job.Name(),
		JobData: jobData,
	}

	data, err := json.Marshal(request)
	if err != nil {
		return err
	}

	topic := r.client.Topic(r.topicName)

	msg := &pubsub.Message{Data: data}

	publishResult := topic.Publish(ctx, msg)
	_, err = publishResult.Get(ctx)
	if err != nil {
		return err
	}

	topic.Stop()
	return nil
}

type JobErrorHandler func(*JobRequest, error)
type JobSuccessHandler func(*JobRequest, *Job)

type Worker struct {
	subscription *pubsub.Subscription
	registry     *JobRegistry
	onError      JobErrorHandler
	onSuccess    JobSuccessHandler
}

func NewWorker(ctx context.Context, projectID, subscriptionName string) (*Worker, error) {
	client, err := pubsub.NewClient(ctx, projectID)
	if err != nil {
		return nil, err
	}

	sub := client.Subscription(subscriptionName)
	sub.ReceiveSettings.MaxOutstandingMessages = 50
	sub.ReceiveSettings.MaxExtension = 20 * time.Second

	w := &Worker{
		subscription: sub,
		onError: func(req *JobRequest, err error) {
			fmt.Println("Error: %s: %+v", err, req)
		},
	}
	return w, nil
}

func (w *Worker) Run(ctx context.Context) error {
	return w.subscription.Receive(ctx, w.processMessage)
}

func (w *Worker) processMessage(ctx context.Context, msg *pubsub.Message) {
	req := &JobRequest{}
	err := json.Unmarshal(msg.Data, req)
	if err != nil {
		w.processFailure(msg, err, nil, nil)
		return
	}

	jobType, err := w.registry.Get(req.JobName)
	if err != nil {
		w.processFailure(msg, err, req, nil)
		return
	}

	job := jobType()
	err = job.Unmasrshal(req.JobData)
	if err != nil {
		w.processFailure(msg, err, req, nil)
		return
	}

	err = job.Perform(ctx)
	if err != nil {
		w.processFailure(msg, err, req, &job)
		return
	}

	msg.Ack()
}

func (w *Worker) processFailure(msg *pubsub.Message, err error, req *JobRequest, job *Job) {
	w.onError(req, err)

	// TODO:
	// introduce a delay
	// maybe out of band to avoid blocking the pool of goroutines of Receive()

	msg.Nack()
}
