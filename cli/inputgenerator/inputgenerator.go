package main

import (
	"embed"
	"io"
	"os"
)

//go:embed example_raw.json
var exampleRawJson embed.FS

//go:embed example_message.json
var exampleMessageJson embed.FS

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

func GetRawJsonExample() ([]byte, error) {
	content, err := exampleRawJson.ReadFile("example_raw.json")
	if err != nil {
		return nil, err
	}
	return content, nil
}

func GetMessageJsonExample() ([]byte, error) {
	content, err := exampleMessageJson.ReadFile("example_message.json")
	if err != nil {
		return nil, err
	}
	return content, nil
}
