package main

import (
	"expvar"
	"flag"
	"fmt"
	"os"
	"runtime"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/sqlpipe/mssqltosnowflake/internal/jsonlog"
	"github.com/sqlpipe/mssqltosnowflake/internal/vcs" // New import
	"github.com/sqlpipe/mssqltosnowflake/pkg"

	_ "github.com/lib/pq"
)

var (
	version = vcs.Version()
)

type cfg struct {
	port    int
	token   string
	limiter struct {
		enabled bool
		rps     float64
		burst   int
	}
	cors struct {
		trustedOrigins []string
	}
}

type application struct {
	config cfg
	logger *jsonlog.Logger
	wg     sync.WaitGroup
}

func main() {
	var cfg cfg

	flag.IntVar(&cfg.port, "port", 9000, "API server port")

	flag.BoolVar(&cfg.limiter.enabled, "limiter-enabled", true, "Enable rate limiter")
	flag.Float64Var(&cfg.limiter.rps, "limiter-rps", 2, "Rate limiter maximum requests per second")
	flag.IntVar(&cfg.limiter.burst, "limiter-burst", 4, "Rate limiter maximum burst")
	flag.StringVar(&cfg.token, "token", "", "Auth token")

	flag.Func("cors-trusted-origins", "Trusted CORS origins (space separated)", func(val string) error {
		cfg.cors.trustedOrigins = strings.Fields(val)
		return nil
	})

	displayVersion := flag.Bool("version", false, "Display version and exit")

	flag.Parse()

	if *displayVersion {
		fmt.Printf("Version:\t%s\n", version)
		os.Exit(0)
	}

	if utf8.RuneCountInString(cfg.token) != 32 {
		fmt.Println("Invalid or missing auth-token value (must be exactly 32 characters). Generating random characters.")
		randomCharacters, err := pkg.RandomCharacters(32)
		if err != nil {
			fmt.Println("Unable to generate random characters")
			os.Exit(0)
		}
		fmt.Printf("Your auth token is: %v\n", randomCharacters)
		cfg.token = randomCharacters
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
