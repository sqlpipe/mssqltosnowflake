package main

import (
	"fmt"

	"net/http"
)

func (app *application) showConcurrencyHandler(w http.ResponseWriter, r *http.Request) {
	counter := 0

	// iterate over transfermap and count the number of objects with a status of "running"
	for _, v := range app.transferMap {
		if v.Status == "running" {
			// increment a counter
			counter++
		}
	}

	// write the counter to the response
	fmt.Fprintf(w, "%d", counter)
}
