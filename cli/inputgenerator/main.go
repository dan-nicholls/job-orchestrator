package main

import (
	"github.com/dannicholls/joborchestrator/internal/kafkalib"
	"fmt"
  "time"
  "os"
  "os/signal"
  "syscall"
)


func main() {
	fmt.Println("Starting Kafka Message Generator")

	exampleMessage, err := GetInputDataExample()
	if err != nil {
		fmt.Println("Error getting example data bytes: ", err)
		return
	}

  writer := kafkalib.GetNewKafkaWriter("testTopic")
  defer writer.Close()

  go kafkalib.ProduceMessages(writer, exampleMessage, 1 * time.Second)

  sigChan := make(chan os.Signal, 1)
  signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
  <-sigChan

  fmt.Println("Shutting down...")
}
