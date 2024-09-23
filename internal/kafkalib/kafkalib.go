package kafkalib

import (
  "fmt"
	"github.com/segmentio/kafka-go"
	"context"
	"log"
  "time"
)

func ProduceMessage(client *kafka.Writer, message []byte) {
	err := client.WriteMessages(context.Background(),
		kafka.Message{
			Key:   []byte("Key-A"),
			Value: message,
		},
	)

	if err != nil {
		log.Fatal("failed to write messages:", err)
	}
}

func GetNewKafkaWriter(topic string) *kafka.Writer {
	return &kafka.Writer{
		Addr:                   kafka.TCP("broker:9092"),
		Topic:                  topic,
		AllowAutoTopicCreation: true,
	}
}

func ProduceMessages(client *kafka.Writer, message []byte, interval time.Duration) {
  ticker := time.NewTicker(interval)

  go func() {
    for {
      select {
      case t := <- ticker.C:
          fmt.Println("Generating Message at ", t)
          ProduceMessage(client, message)
      }
    } 
  }()
}
