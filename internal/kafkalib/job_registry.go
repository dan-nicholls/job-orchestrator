package kafkalib

import "fmt"

type JobRegistry struct {
	jobs map[string]Job
}

// Register a new job
func (jr *JobRegistry) RegisterJob(job Job) error {
	_, exists := jr.jobs[job.Name]

	if exists {
		return fmt.Errorf("Job with name %s is already registered", job.Name)
	}

	jr.jobs[job.Name] = job
	return nil
}

// Get all registered jobs
func (jr *JobRegistry) GetAllJobs() (map[string]Job, error) {
	if jr.jobs == nil {
		return nil, fmt.Errorf("Jobs map has not yet been initialised")
	}
	return jr.jobs, nil
}

// Get a registered job
func (jr *JobRegistry) GetJob(jobName string) (Job, error) {
	if len(jobName) <= 0 {
		fmt.Errorf("Unable to fetch job, invalid job name")
	}

	job, exists := jr.jobs[jobName]
	if !exists {
		return Job{}, fmt.Errorf("No job with name %s is registered", jobName)
	}
	return job, nil
}

// Delete a regsitered job
func (jr *JobRegistry) RemoveJob(jobName string) error {
	_, exists := jr.jobs[jobName]
	if !exists {
		return fmt.Errorf("No job with name %s is registered", jobName)
	}
	delete(jr.jobs, jobName)
	return nil
}
