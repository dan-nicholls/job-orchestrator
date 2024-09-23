package main

import (
	"embed"
	"io"
	"os"
)

//go:embed example.json
var exampleJson embed.FS

func readFileAsBytes(filePath string) ([]byte, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	fileBytes, err := io.ReadAll(file)
	if err != nil {
		return nil, err
	}

	return fileBytes, nil
}

func GetInputDataExample() ([]byte, error) {
	content, err := exampleJson.ReadFile("example.json")
	if err != nil {
		return nil, err
	}
	return content, nil
}
