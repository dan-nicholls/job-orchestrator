package main

import (
	"fmt"
  "github.com/dannicholls/joborchestrator/internal/kafkalib"
)

func ExampleHandler(message []byte) {
  fmt.Printf("Processing message: %s\n", message)
}

func main() {
  fmt.Println("Hello World!")

  job := kafkalib.Job{
    Name: "ExampleJob",
    Handler: ExampleHandler,
  }
  
  job.Reader.InitKafkaReader("broker", 9092, "testTopic", job.Name)

  go job.ReadMessages()
  
  select {}
}
