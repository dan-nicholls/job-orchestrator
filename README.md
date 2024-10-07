# job-orchestrator

Job-Orchestrator is a Go-based application designed to manage and orchestrate
jobs using Kafka as the messaging backbone. It was originally designed as a
simplification to the `Apache Flink` application, for simple use-cases via a
single message bus. It allows you to define job templates, create jobs from
these templates, and manage their lifecycle.

## Features

- **Job Templates**: Define reusable job templates with specific input and
  output types.
- **Kafka Integration**: Seamlessly integrates with Kafka for message handling.
- **Job Management**: Create, start, and stop jobs dynamically.

## Geting Started

### Installation

1. **Clone the repository:**

   ```bash
   git clone https://github.com/dan-nicholls/joborchestrator.git
   cd joborchestrator
   ```

1. **Start required services**

   ```bash
   docker compose up kafka kafka-ui -d
   ```

1. **Start main application**

   ```bash
   docker compose up go-app
   ```

1. **Start the message generator**
   ```bash
   docker compose up go-gen
   ```

## Todo

- [ ] Add Logging per Job
- [ ] Add a TUI app that handles jobs
- [ ] CRUD for starting jobs
- [ ] Endpoint for fetching logs (Stream)

## Acknowledgments

- [segmentio/kafka-go](https://github.com/segmentio/kafka-go) for Kafka
  integration ofering both high and low level APIs.
