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

	exampleMessage, err := GetMessageJsonExample()
	if err != nil {
		fmt.Println("Error getting example data bytes: ", err)
		return
	}

  exampleJob := kafkalib.Job {
    Name: "ExampleJobWriter",
    Handler: nil,
  }

  exampleJob.Writer.InitKafkaWriter("broker", 9092, "inputTopic")
  defer exampleJob.Writer.Writer.Close()

  go exampleJob.Writer.ProduceMessagesPeriodically(exampleMessage, 1 * time.Second)

  sigChan := make(chan os.Signal, 1)
  signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
  <-sigChan

  fmt.Println("Shutting down...")
}
