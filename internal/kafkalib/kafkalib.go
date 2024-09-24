package kafkalib

import (
  "fmt"
	"github.com/segmentio/kafka-go"
	"context"
	"log"
  "time"
  "encoding/json"
)

type Message struct {
  Type string `json:"type"`
  Data json.RawMessage `json:"data"`
}

type JobReader struct {
  InputTopic string
  Reader *kafka.Reader
}

type JobWriter struct {
  OuputTopic string
  Writer *kafka.Writer
}

type Job struct {
  Name string
  Reader JobReader
  Writer JobWriter
  InputTypes []string
  OutputTypes []string
  Handler func(Message, *JobWriter)
}

func (jw *JobWriter) InitKafkaWriter(hostAddress string, port int, topic string) {
  jw.Writer = &kafka.Writer{
    Addr:                   kafka.TCP("broker:9092"),
		Topic:                  topic,
		AllowAutoTopicCreation: true,
  }
  jw.OuputTopic = topic
}

func (jr *JobReader) InitKafkaReader(hostAddress string, port int, topic, groupId string) {
  jr.Reader = kafka.NewReader(kafka.ReaderConfig{
    Brokers: []string{"broker:9092"},
    Topic: topic,
    GroupID: groupId,
  })
  jr.InputTopic = topic
}

func (jw *JobWriter) ProduceMessage(message []byte) {
	err := jw.Writer.WriteMessages(context.Background(),
		kafka.Message{
			Key:   []byte("Key-A"),
			Value: message,
		},
	)

	if err != nil {
		log.Fatal("failed to write messages:", err)
	}
}

func (jw *JobWriter) ProduceMessagesPeriodically(message []byte, interval time.Duration) {
  ticker := time.NewTicker(interval)

  go func() {
    for {
      select {
      case t := <- ticker.C:
          fmt.Println("Generating Message at ", t)
          jw.ProduceMessage(message)
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

func (j *Job) ReadMessages() {
  for {
    msg, err := j.Reader.Reader.ReadMessage(context.Background())
    if err != nil {
      log.Fatal("Failed to read messages: ", err)
    }

    var message Message
    err = json.Unmarshal(msg.Value, &message)
    if err != nil {
      log.Printf("Failed to unmarshal message: %v", err)
      continue
    }

    if contains(j.InputTypes, message.Type) {
        j.Handler(message, &j.Writer)
    } else {
      fmt.Printf("Invalid Msg Type: %s\n", message.Type)
    }
  }
}
