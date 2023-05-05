package main

import (
	"expvar"
	"net/http"

	"github.com/julienschmidt/httprouter"
)

func (app *application) routes() http.Handler {
	router := httprouter.New()

	router.NotFound = http.HandlerFunc(app.notFoundResponse)
	router.MethodNotAllowed = http.HandlerFunc(app.methodNotAllowedResponse)

	router.HandlerFunc(http.MethodGet, "/v1/healthcheck", app.healthcheckHandler)

	router.HandlerFunc(http.MethodPost, "/v1/transfers", app.createTransferHandler)
	router.HandlerFunc(http.MethodGet, "/v1/transfers/", app.showTransferHandler)
	router.HandlerFunc(http.MethodGet, "/v1/concurrency", app.showConcurrencyHandler)

	router.Handler(http.MethodGet, "/debug/vars", expvar.Handler())

	return app.recoverPanic(router)
}
