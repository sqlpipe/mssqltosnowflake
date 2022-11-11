package main

import (
	"expvar"
	"flag"
	"fmt"
	"os"
	"runtime"
	"sync"
	"time"

	"github.com/sqlpipe/mssqltosnowflake/internal/jsonlog"
	"github.com/sqlpipe/mssqltosnowflake/internal/vcs" // New import

	_ "github.com/lib/pq"
)

var (
	version = vcs.Version()
)

type cfg struct {
	port int
}

type application struct {
	config cfg
	logger *jsonlog.Logger
	wg     sync.WaitGroup
}

func main() {
	var cfg cfg

	flag.IntVar(&cfg.port, "port", 9000, "API server port")
	displayVersion := flag.Bool("version", false, "Display version and exit")

	flag.Parse()

	if *displayVersion {
		fmt.Printf("Version:\t%s\n", version)
		os.Exit(0)
	}

	logger := jsonlog.New(os.Stdout, jsonlog.LevelInfo)

	expvar.NewString("version").Set(version)

	expvar.Publish("goroutines", expvar.Func(func() interface{} {
		return runtime.NumGoroutine()
	}))

	expvar.Publish("timestamp", expvar.Func(func() interface{} {
		return time.Now().Unix()
	}))

	app := &application{
		config: cfg,
		logger: logger,
	}

	err := app.serve()
	if err != nil {
		logger.PrintFatal(err, nil)
	}
}
