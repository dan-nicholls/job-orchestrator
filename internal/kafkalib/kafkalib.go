package kafkalib

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/segmentio/kafka-go"
	"log"
	"sync"
	"time"
)

type Message struct {
	Type string          `json:"type"`
	Data json.RawMessage `json:"data"`
}

type KafkaInput struct {
	Topic      string
	InputTypes []string
	Reader     *kafka.Reader
}

type KafkaOutput struct {
	Topic       string
	OutputTypes []string
	Writer      *kafka.Writer
}

type Job struct {
	Name    string
	Input   KafkaInput
	Output  KafkaOutput
	Handler func(Message, *KafkaOutput)
}

type JobManager struct {
	Jobs    []Job
	address string
	port    int
	mu      sync.Mutex
}

func NewJobManager(address string, port int) *JobManager {
	return &JobManager{
		Jobs:    make([]Job, 0),
		address: address,
		port:    port,
	}
}

// This parameter should be a generic
func (jm *JobManager) AddJob(job Job) error {
	jm.mu.Lock()
	defer jm.mu.Unlock()

	for _, j := range jm.Jobs {
		if job.Name == j.Name {
			return fmt.Errorf("job with name %s is already exists", j.Name)
		}
	}
	jm.Jobs = append(jm.Jobs, job)
	return nil
}

func (jm *JobManager) RemoveJob(jobName string) error {
	jm.mu.Lock()
	defer jm.mu.Unlock()

	for i, j := range jm.Jobs {
		if j.Name == jobName {
			jm.Jobs = append(jm.Jobs[:i], jm.Jobs[i+1:]...)
			return nil
		}
	}
	return fmt.Errorf("Job name %s not found in registered jobs", jobName)
}

func (j *Job) InitialiseJob(address string, port int) {
	if len(j.Input.InputTypes) > 0 {
		j.Input.InitKafkaReader(address, port, j.Input.Topic, j.Name)
	}
	if len(j.Output.OutputTypes) > 0 {
		j.Output.InitKafkaWriter(address, port, j.Output.Topic)
	}
}

func (j *Job) EndJob() {
	fmt.Printf("Ending %s Job\n", j.Name)
	if j.Input.Reader != nil {
		j.Input.Reader.Close()
	}

	if j.Output.Writer != nil {
		j.Output.Writer.Close()
	}
}

func (jm *JobManager) StartJob(jobName string) error {
	jm.mu.Lock()
	defer jm.mu.Unlock()
	for _, j := range jm.Jobs {
		// Initialise Job
		if j.Name == jobName {
			j.InitialiseJob(jm.address, jm.port)
			fmt.Printf("Starting job: %s\n", j.Name)
			//DO STUFF HERE
			go j.Start()
			return nil
		}
	}
	return fmt.Errorf("Job name %s cannot be started. Not found in registered jobs")
}

func (jm *JobManager) StopAllJobs() {
	for _, j := range jm.Jobs {
		j.EndJob()
	}
}

func (jw *KafkaOutput) InitKafkaWriter(hostAddress string, port int, topic string) {
	jw.Writer = &kafka.Writer{
    Addr:                   kafka.TCP(fmt.Sprintf("%s:%d", hostAddress, port)),
		Topic:                  topic,
		AllowAutoTopicCreation: true,
	}
	jw.Topic = topic
}

func (ki *KafkaInput) InitKafkaReader(hostAddress string, port int, topic, groupId string) {
	ki.Reader = kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{"broker:9092"},
		Topic:   topic,
		GroupID: groupId,
	})
	ki.Topic = topic
}

func (ko *KafkaOutput) ProduceMessage(data []byte, messageType string) error {
	if ko.Writer == nil {
		fmt.Errorf("Writer not initialised")
	}
	if !contains(ko.OutputTypes, messageType) {
		fmt.Errorf("%s message type not in defined OutputTypes")
	}

	message := Message{
		Type: messageType,
		Data: data,
	}

	messageBytes, err := json.Marshal(message)
	if err != nil {
		fmt.Errorf("Failed to marshal output message %v", err)
	}

	err = ko.Writer.WriteMessages(context.Background(),
		kafka.Message{
			Key:   []byte("Key-A"),
			Value: messageBytes,
		},
	)

	if err != nil {
		log.Fatal("failed to write messages:", err)
	}
	return nil
}

func (ko *KafkaOutput) ProduceMessagesPeriodically(message []byte, messageType string, interval time.Duration) {
	ticker := time.NewTicker(interval)

	go func() {
		for {
			select {
			case t := <-ticker.C:
				fmt.Println("Generating Message at ", t)
				ko.ProduceMessage(message, messageType)
			}
		}
	}()
}

func contains(arr []string, str string) bool {
	for _, item := range arr {
		if item == str {
			return true
		}
	}
	return false
}

func (j *Job) Start() {
	for {
		msg, err := j.Input.Reader.ReadMessage(context.Background())
		if err != nil {
			log.Fatal("Failed to read messages: ", err)
		}

		var message Message
		err = json.Unmarshal(msg.Value, &message)
		if err != nil {
			log.Printf("Failed to unmarshal message: %v", err)
			continue
		}

		if contains(j.Input.InputTypes, message.Type) {
			j.Handler(message, &j.Output)
		} else {
			fmt.Printf("Invalid Msg Type: %s\n", message.Type)
		}
	}
}
