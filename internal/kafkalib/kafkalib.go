package kafkalib

import (
  "fmt"
	"github.com/segmentio/kafka-go"
	"context"
	"log"
  "time"
)

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
  Handler func([]byte)
}

func (jw *JobWriter) InitKafkaWriter(hostAddress string, port int, topic string) {
  jw.Writer = &kafka.Writer{
    Addr:                   kafka.TCP(fmt.Sprintf("%s:%i", hostAddress, port)),
		Topic:                  topic,
		AllowAutoTopicCreation: true,
  }
  jw.OuputTopic = topic
}

func (jr *JobReader) InitKafkaReader(hostAddress string, port int, topic, groupId string) {
  jr.Reader = kafka.NewReader(kafka.ReaderConfig{
    Brokers: []string{fmt.Sprintf("%s:%d", hostAddress, port)},
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

func (j *Job) ReadMessages() {
  for {
    msg, err := j.Reader.Reader.ReadMessage(context.Background())
    if err != nil {
      log.Fatal("Failed to read messages: ", err)
    }

    j.Handler(msg.Value)
  }
}
