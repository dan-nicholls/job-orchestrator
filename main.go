package main

import (
	"context"
	"fmt"
	"github.com/dannicholls/joborchestrator/internal/inputgenerator"
	"github.com/segmentio/kafka-go"
	"log"
  "time"
  "os"
  "os/signal"
  "syscall"
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

func main() {
	fmt.Println("Hello World!")

	exampleMessage, err := inputgenerator.GetInputDataExample()
	if err != nil {
		fmt.Println("Error getting example data bytes: ", err)
		return
	}

  writer := GetNewKafkaWriter("testTopic")
  defer writer.Close()

  go ProduceMessages(writer, exampleMessage, 1 * time.Second)

  sigChan := make(chan os.Signal, 1)
  signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
  <-sigChan

  fmt.Println("Shutting down...")
}
