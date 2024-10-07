package main

import (
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/dannicholls/joborchestrator/internal/kafkalib"
)

func main() {
	fmt.Println("Welcome to Job-Orchestrator")

	jm := kafkalib.NewJobManager("broker", 9092)

	exampleJobTemplate := kafkalib.JobTemplate{
		Name: "ExampleJobTemplate",
		Handler: func(message kafkalib.Message, jw *kafkalib.KafkaOutput) {
			fmt.Printf("EXAMPLE - Processing message: %s\n", message)
			data := map[string]string{
				"testData": "data",
			}

			jsonData, err := json.Marshal(data)
			if err != nil {
				fmt.Printf("Error Marshalling data: %v\n", err)
				return
			}

			jw.ProduceMessage(jsonData, "testOuputType")
		},
		InputTypes:  []string{"testType"},
		OutputTypes: []string{"testOuputType"},
	}

	loggingJobTemplate := kafkalib.JobTemplate{
		Name: "LoggingJobTemplate",
		Handler: func(message kafkalib.Message, jw *kafkalib.KafkaOutput) {
			fmt.Printf("LOGGER - Processing message: %s\n", message.Type)
		},
		InputTypes: []string{"testOuputType"},
	}

	jm.AddTemplate(exampleJobTemplate)
	jm.AddTemplate(loggingJobTemplate)
	fmt.Printf("%+v\n", jm)

	_, err := jm.CreateJob("ExampleJob", "ExampleJobTemplate", "inputTopic", "inputTopic")
	if err != nil {
		fmt.Println("error: ", err)
	}
	_, err = jm.CreateJob("LoggingJob", "LoggingJobTemplate", "inputTopic", "")
	if err != nil {
		fmt.Println("error: ", err)
	}
	fmt.Printf("%+v\n", jm)

	go jm.StartJob("LoggingJob")
	go jm.StartJob("ExampleJob")
	defer jm.StopAllJobs()

	// Enter loop until interupt signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigChan
	fmt.Printf("Received signal: %s. Shutting down...\n", sig)
}
