package main

import (
	"context"
	"fmt"
	"log"

	straw "github.com/beremaran/straw-sdk-go"
)

func main() {
	client := straw.NewClient("http://localhost:8080", "")
	response, err := client.Do(context.Background(), straw.Request{Method: "GET", URL: "https://example.com"})
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println(response.Status, response.RequestID)
}
