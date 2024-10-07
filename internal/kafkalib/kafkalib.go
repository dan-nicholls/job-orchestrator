package kafkalib

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"time"

	"github.com/segmentio/kafka-go"
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

type Status string

const (
	Errored Status = "errored"
	Running Status = "running"
	Stopped Status = "stopped"
)

type Job struct {
	Name     string
	Input    KafkaInput
	Output   KafkaOutput
	Template *JobTemplate
	Status   Status
	Logger   *log.Logger
	Logs     *bytes.Buffer
}

func (j *Job) InitialiseJob(address string, port int) {
	j.Logs = new(bytes.Buffer)
	j.Logger = log.New(
		io.MultiWriter(os.Stdout, j.Logs),
		fmt.Sprintf("[%s] ", j.Name),
		log.LstdFlags,
	)
	if len(j.Input.InputTypes) > 0 {
		j.Input.InitKafkaReader(address, port, j.Input.Topic, j.Name)
	} else {
		j.Logger.Println("No Input Types available")
	}
	if len(j.Output.OutputTypes) > 0 {
		j.Output.InitKafkaWriter(address, port, j.Output.Topic)
	} else {
		j.Logger.Println("No Ouput Types available")
	}
}

func (j *Job) EndJob() {
	j.Logger.Printf("Ending %s Job\n", j.Name)
	if j.Input.Reader != nil {
		j.Input.Reader.Close()
	}

	if j.Output.Writer != nil {
		j.Output.Writer.Close()
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

func (ko *KafkaOutput) ProduceMessagesPeriodically(
	message []byte,
	messageType string,
	interval time.Duration,
) {
	ticker := time.NewTicker(interval)

	go func() {
		for {
			select {
			case t := <-ticker.C:
				fmt.Printf("Generating %s Message at %s\n", messageType, t)
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
			j.Logger.Printf("Failed to read messages: %v\n", err)
			continue
		}

		var message Message
		err = json.Unmarshal(msg.Value, &message)
		if err != nil {
			j.Logger.Printf("Failed to unmarshal message: %v\n", err)
			continue
		}

		if contains(j.Input.InputTypes, message.Type) {
			j.Template.Handler(message, &j.Output, j.Logger)
			// } else {
			// 	fmt.Printf("Invalid Msg Type: %s\n", message.Type)
		}
	}
}
