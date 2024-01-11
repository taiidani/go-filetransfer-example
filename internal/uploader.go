package internal

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type UploadRecord struct {
	// The path within the bucket that the file was uploaded to
	Key string
	// When the upload began
	Start time.Time
	// When the upload completed
	End time.Time
}

type S3Uploader struct {
	// The name of the bucket to upload to
	Bucket string

	// The base path for keys to be stored in the bucket
	BucketPrefix string

	// Instance of the AWS S3 API client
	Client *s3.Client

	// The storage mechanism for tracking uploaded files
	Storage *Storage
}

func NewS3Uploader(client *s3.Client, storage *Storage, bucket, bucketPrefix string) *S3Uploader {
	return &S3Uploader{
		Client:       client,
		Storage:      storage,
		Bucket:       bucket,
		BucketPrefix: bucketPrefix,
	}
}

func (s *S3Uploader) UploaderThread(ctx context.Context, files <-chan string) error {
	for path := range files {
		select {
		case <-ctx.Done():
			return fmt.Errorf("Aborted uploader thread: %w", ctx.Err())
		default:
			f, err := os.Open(path)
			if err != nil {
				return fmt.Errorf("failed to open file %q: %w", path, err)
			}

			record, err := s.uploadFile(ctx, f, filepath.Base(path))
			if err != nil {
				return fmt.Errorf("failed to upload file %q: %w", path, err)
			}

			slog.Info("Upload successful", "bucket", s.Bucket, "key", record.Key)
			s.Storage.SetItem(path, record)
		}
	}

	return nil
}

func (s *S3Uploader) uploadFile(ctx context.Context, f io.ReadCloser, path string) (UploadRecord, error) {
	defer f.Close()

	// Begin setting up the upload log
	ret := UploadRecord{Start: time.Now()}

	// Upload the object to S3
	ret.Key = strings.Join([]string{s.BucketPrefix, path}, "/")
	_, err := s.Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(s.Bucket),
		Key:    aws.String(ret.Key),
		Body:   f,
	})
	if err != nil {
		return ret, fmt.Errorf("could not upload archive %q to S3: %s", ret.Key, err)
	}

	ret.End = time.Now()
	return ret, nil
}
