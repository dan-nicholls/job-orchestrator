package main

import (
	"fmt"
	"github.com/dannicholls/joborchestrator/internal/kafkalib"
  "os"
  "os/signal"
  "syscall"
)

func main() {
	fmt.Println("Welcome to Job-Orchestrator")

  jm := kafkalib.NewJobManager("broker", 9092)

	exampleJob := kafkalib.Job{
		Name: "ExampleJob1",
		Handler: func(message kafkalib.Message, jw *kafkalib.KafkaOutput) {
			fmt.Printf("Processing message: %s\n", message)
      jw.ProduceMessage([]byte("testData"), "testOuputType")
		},
    Input: kafkalib.KafkaInput{
      InputTypes: []string{"testType"},
      Topic: "inputTopic",
    },
    Output: kafkalib.KafkaOutput{
      OutputTypes: []string{"testOuputType"},
      Topic: "inputTopic",
    },
	}

	LoggingJob := kafkalib.Job{
		Name: "LoggingJob",
		Handler: func(message kafkalib.Message, jw *kafkalib.KafkaOutput) {
			fmt.Printf("LOGGER - Processing message: %s\n", message.Type)
		},
    Input: kafkalib.KafkaInput{
      InputTypes: []string{"testType"},
      Topic: "inputTopic",
    },
	}

  jm.AddJob(exampleJob)
  jm.AddJob(LoggingJob)


  go jm.StartJob("LoggingJob")
  go jm.StartJob("ExampleJob")
  defer jm.StopAllJobs()

  // Enter loop until interupt signal
  sigChan := make(chan os.Signal, 1)
  signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
  sig := <-sigChan
  fmt.Printf("Received signal: %s. Shutting down...\n", sig)
}
