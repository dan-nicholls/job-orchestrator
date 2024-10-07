package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/dannicholls/joborchestrator/internal/kafkalib"
)

func main() {
	fmt.Println("Starting Kafka Message Generator")

	exampleMessage, err := GetMessageJsonExample()
	if err != nil {
		fmt.Println("Error getting example data bytes: ", err)
		return
	}

	jm := kafkalib.NewJobManager("broker", 9092)

	exampleJobTemplate := kafkalib.JobTemplate{
		Name:        "exampleJobWriterTemplate",
		Handler:     nil,
		OutputTypes: []string{"testType"},
	}

	jm.AddTemplate(exampleJobTemplate)
	tempJob, err := jm.CreateJob("exampleJob", "exampleJobWriterTemplate", "", "inputTopic")
	if err != nil {
		fmt.Println("Error: ", err)
	}
	fmt.Printf("Job: %+v\n", tempJob)

	job := jm.Jobs["exampleJob"]
	fmt.Printf("%+v\n", job)
	job.InitialiseJob("broker", 9092)
	fmt.Printf("%+v\n", jm)
	job.Output.ProduceMessagesPeriodically(exampleMessage, "testType", 1*time.Second)

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	fmt.Println("Shutting down...")
}
