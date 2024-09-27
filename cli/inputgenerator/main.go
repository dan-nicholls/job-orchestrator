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

  jm := kafkalib.NewJobManager("broker", 9092)

  exampleJob := kafkalib.Job {
    Name: "ExampleJobWriter",
    Handler: nil,
    Output: kafkalib.KafkaOutput{
      OutputTypes: []string{"testOutput"},
      Topic: "inputTopic",
    },
  }

  jm.AddJob(exampleJob)

  jm.Jobs[0].InitialiseJob("broker", 9092)
  fmt.Printf("%+v",jm.Jobs[0])
  jm.Jobs[0].Output.ProduceMessagesPeriodically(exampleMessage, "testOutput", 1 * time.Second)

  sigChan := make(chan os.Signal, 1)
  signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
  <-sigChan

  fmt.Println("Shutting down...")
}
