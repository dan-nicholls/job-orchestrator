package main

import (
	"fmt"
	"github.com/dannicholls/joborchestrator/internal/kafkalib"
)

func ExampleJobHandler(message kafkalib.Message, jw *kafkalib.JobWriter) {
	fmt.Printf("Processing message: %s\n", message)
	jw.ProduceMessage([]byte("Output Message!"))
}

func LoggingHandler(message kafkalib.Message, jw *kafkalib.JobWriter) {
	fmt.Printf("LOGGER - Processing message: %s\n", message.Type)
}

func main() {

	fmt.Println("Hello World!")

	exampleJob := kafkalib.Job{
		Name:       "ExampleJob1",
		Handler:    ExampleJobHandler,
		InputTypes: []string{"testType"},
	}

	LoggingJob := kafkalib.Job{
		Name:       "LoggingJob",
		Handler:    LoggingHandler,
		InputTypes: []string{"testType"},
	}

	exampleJob.Reader.InitKafkaReader("broker", 9092, "inputTopic", LoggingJob.Name)
	exampleJob.Writer.InitKafkaWriter("broker", 9092, "inputTopic")
	defer exampleJob.Reader.Reader.Close()
	defer exampleJob.Writer.Writer.Close()

	LoggingJob.Reader.InitKafkaReader("broker", 9092, "inputTopic", LoggingJob.Name)
	defer LoggingJob.Reader.Reader.Close()

	go exampleJob.ReadMessages()
	go LoggingJob.ReadMessages()

	select {}
}
