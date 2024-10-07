package kafkalib

import (
	"fmt"
	"sync"
)

type JobTemplate struct {
	Name        string
	InputTypes  []string
	OutputTypes []string
	Handler     func(Message, *KafkaOutput)
}

type JobManager struct {
	templates map[string]JobTemplate
	Jobs      map[string]Job
	address   string
	port      int
	mu        sync.Mutex
}

func NewJobManager(address string, port int) *JobManager {
	return &JobManager{
		templates: make(map[string]JobTemplate),
		Jobs:      make(map[string]Job),
		address:   address,
		port:      port,
	}
}

// Add template
func (jm *JobManager) AddTemplate(jt JobTemplate) error {
	jm.mu.Lock()
	defer jm.mu.Unlock()

	_, exists := jm.templates[jt.Name]

	if exists {
		return fmt.Errorf("Job Template with name %s is already registered", jt.Name)
	}

	jm.templates[jt.Name] = jt
	return nil
}

// Get all templates
func (jm *JobManager) GetAllTemplates() (map[string]JobTemplate, error) {
	jm.mu.Lock()
	defer jm.mu.Unlock()

	if jm.templates == nil {
		return nil, fmt.Errorf("Job Templates map has not yet been initialised")
	}
	return jm.templates, nil
}

// Get template by Name
func (jm *JobManager) GetTemplate(templateName string) (JobTemplate, error) {
	jm.mu.Lock()
	defer jm.mu.Unlock()

	if len(templateName) <= 0 {
		return JobTemplate{}, fmt.Errorf("Unable to fetch template, invalid template name")
	}

	job, exists := jm.templates[templateName]
	if !exists {
		return JobTemplate{}, fmt.Errorf("No job template with name %s is registered", templateName)
	}
	return job, nil
}

// Remove template
func (jm *JobManager) RemoveTemplate(templateName string) error {
	jm.mu.Lock()
	defer jm.mu.Unlock()

	_, exists := jm.templates[templateName]
	if !exists {
		return fmt.Errorf("No job template with name %s is registered", templateName)
	}
	delete(jm.templates, templateName)
	return nil
}

func (jm *JobManager) CreateJob(name, templateName, inputTopic, outputTopic string) (*Job, error) {
	fmt.Printf("Creating Job %s (%s)\n", name, templateName)
	jm.mu.Lock()
	defer jm.mu.Unlock()

	if len(name) == 0 {
		return nil, fmt.Errorf("Invalid Job name")
	}
	jt, exists := jm.templates[templateName]
	if !exists {
		return nil, fmt.Errorf("Job template %s, does not exist.")
	}

	job := Job{
		Name: name,
		Input: KafkaInput{
			InputTypes: jt.InputTypes,
			Topic:      inputTopic,
		},
		Output: KafkaOutput{
			OutputTypes: jt.OutputTypes,
			Topic:       outputTopic,
		},
		Template: &jt,
		Status:   Stopped,
	}
	job.InitialiseJob(jm.address, jm.port)

	jm.Jobs[name] = job
	return &job, nil
}

func (jm *JobManager) GetJob(jobName string) (*Job, error) {
	if len(jobName) <= 0 {
		return nil, fmt.Errorf("Unable to fetch job, invalid job name")
	}

	job, exists := jm.Jobs[jobName]
	if !exists {
		return nil, fmt.Errorf("No job with name %s is registered", jobName)
	}
	return &job, nil
}

func (jm *JobManager) StartJob(jobName string) error {
	jm.mu.Lock()
	defer jm.mu.Unlock()

	j, err := jm.GetJob(jobName)
	if err != nil {
		return err
	}

	j.InitialiseJob(jm.address, jm.port)
	fmt.Printf("Starting job: %s\n", j.Name)
	go j.Start()
	return nil
}

func (jm *JobManager) StopJob(jobName string) error {
	// TODO: Update for template support
	jm.mu.Lock()
	defer jm.mu.Unlock()

	j, err := jm.GetJob(jobName)
	if err != nil {
		return err
	}

	fmt.Printf("Stopping job: %s\n", j.Name)
	go j.EndJob()
	return nil
}

func (jm *JobManager) StopAllJobs() {
	// TODO: Update for template support
	for _, j := range jm.Jobs {
		j.EndJob()
	}
}
