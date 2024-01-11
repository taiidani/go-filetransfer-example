package main

import (
	"context"
	"flag"
	"fmt"
	"go-filetransfer-example/internal"
	"log"
	"os"
	"os/signal"
	"sync"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

var (
	// The amount of concurrent file uploaders to run
	concurrency int
	// The location of the JSON file to track uploads within
	recordPath string
	// The directory to upload
	basePath string
	// The name of the S3 bucket to upload to
	s3Bucket string
	// The prefix within the bucket to upload objects to
	s3BucketPrefix string
)

func main() {
	flag.IntVar(&concurrency, "concurrency", 3, "The amount of concurrent file uploaders to run")
	flag.StringVar(&recordPath, "record-path", "./uploadRecord.json", "The location of the file to save upload records to")
	flag.Parse()
	if len(flag.Args()) < 2 {
		log.Println("Error: Missing arguments. Required format: 'go run main.go <Directory To Upload> <Bucket Name> [Bucket Prefix]'.")
		flag.PrintDefaults()
		os.Exit(1)
	}
	basePath = flag.Args()[0]
	s3Bucket = flag.Args()[1]
	if len(flag.Args()) == 3 {
		s3BucketPrefix = flag.Args()[2]
	}

	// Will gracefully cancel all in-progress operations upon an interrupt
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	reader, storage, uploader, err := setup(ctx)
	if err != nil {
		log.Printf("Setup Error: %s", err)
		os.Exit(2)
	}

	if err := run(ctx, reader, storage, uploader); err != nil {
		log.Printf("Run Error: %s", err)
		os.Exit(3)
	}
}

func setup(ctx context.Context) (*internal.Reader, *internal.Storage, *internal.S3Uploader, error) {
	// Load the storage backend
	storage := internal.NewFileStorage(recordPath)
	if err := storage.Load(); err != nil {
		return nil, nil, nil, fmt.Errorf("could not load storage entries: %w", err)
	}

	// Set up the Uploader
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("could not load AWS configuration: %w", err)
	}
	client := s3.NewFromConfig(cfg)
	uploader := internal.NewS3Uploader(client, storage, s3Bucket, s3BucketPrefix)

	// Set up the filesystem reader
	scanner := internal.NewFileReader(storage)

	return scanner, storage, uploader, nil
}

func run(ctx context.Context, reader *internal.Reader, storage *internal.Storage, uploader *internal.S3Uploader) error {
	// Always save the storage at the end
	defer func() {
		if err := storage.Save(); err != nil {
			log.Printf("could not save storage logs: %s", err)
		}
	}()

	// Start the scanner thread, which will enumerate all files and pass them
	// into the 'files' channel.
	paths := make(chan string)
	go func(paths chan<- string) {
		defer close(paths)

		if err := reader.ReadDir(ctx, basePath, paths); err != nil {
			log.Printf("Failed to enumerate all files: %s", err)
		}
	}(paths)

	// Start multiple concurrent uploader threads, which pull files off the
	// 'files' channel and upload them to S3.
	wg := sync.WaitGroup{}
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := uploader.UploaderThread(ctx, paths); err != nil {
				log.Printf("Uploader thread failure: %s", err)
			}
		}()
	}

	wg.Wait()
	fmt.Println("Sync Complete")
	return nil
}
