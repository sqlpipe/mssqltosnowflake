package main

import (
	"context"
	"errors"
	"expvar"
	"flag"
	"fmt"
	"log"
	"os"
	"runtime"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/sqlpipe/mssqltosnowflake/internal/data"
	"github.com/sqlpipe/mssqltosnowflake/internal/jsonlog"
	"github.com/sqlpipe/mssqltosnowflake/internal/vcs"

	_ "github.com/lib/pq"
)

var (
	version = vcs.Version()
)

type cfg struct {
	port       int
	cloudWatch struct {
		logGroupName  string
		logStreamName string
	}
}

type application struct {
	config           cfg
	transferMap      map[string]data.Transfer
	logger           *jsonlog.Logger
	wg               sync.WaitGroup
	uploader         *manager.Uploader
	cloudWatchClient *cloudwatchlogs.Client
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
	awsCfg, err := config.LoadDefaultConfig(
		context.Background(),
		config.WithRegion("us-west-2"),
	)
	if err != nil {
		log.Fatalf("Could not load aws default config <- %v", err)
	}

	ip, err := getLocalIPAddress()
	if err != nil {
		log.Fatalf("Error getting local IP address: %v", err)
	}

	cfg.cloudWatch.logGroupName = "sqlpipe-logs"
	cfg.cloudWatch.logStreamName = ip

	logger := jsonlog.New(os.Stdout, jsonlog.LevelInfo)

	expvar.NewString("version").Set(version)

	expvar.Publish("goroutines", expvar.Func(func() interface{} {
		return runtime.NumGoroutine()
	}))

	expvar.Publish("timestamp", expvar.Func(func() interface{} {
		return time.Now().Unix()
	}))

	app := &application{
		config:           cfg,
		logger:           logger,
		transferMap:      make(map[string]data.Transfer),
		cloudWatchClient: cloudwatchlogs.NewFromConfig(awsCfg),
	}

	s3Client := s3.NewFromConfig(awsCfg)
	app.uploader = manager.NewUploader(s3Client)

	_, err = app.cloudWatchClient.CreateLogGroup(context.Background(), &cloudwatchlogs.CreateLogGroupInput{
		LogGroupName: aws.String(cfg.cloudWatch.logGroupName),
	})

	if err != nil {
		var resourceAlreadyExistsException *types.ResourceAlreadyExistsException
		if !errors.As(err, &resourceAlreadyExistsException) {
			log.Fatalf("CreateLogGroup error: %v", err)
		}
	}

	_, err = app.cloudWatchClient.CreateLogStream(context.Background(), &cloudwatchlogs.CreateLogStreamInput{
		LogGroupName:  aws.String(cfg.cloudWatch.logGroupName),
		LogStreamName: aws.String(cfg.cloudWatch.logStreamName),
	})

	if err != nil {
		var resourceAlreadyExistsException *types.ResourceAlreadyExistsException
		if !errors.As(err, &resourceAlreadyExistsException) {
			log.Fatalf("CreateLogStream error: %v", err)
		}
	}

	app.putLogEvents(fmt.Sprintf("Starting sqlpipe at IP %v", ip))

	err = app.serve()
	if err != nil {
		logger.PrintFatal(err, nil)
	}
}
