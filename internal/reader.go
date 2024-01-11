package internal

import (
	"context"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
)

type Reader struct {
	// The path within the filesystem to read from
	Path string
	// The storage mechanism for tracking uploaded files
	Storage *Storage
}

type ReaderEntry struct {
	// The absolute path to the file on disk
	Path string
}

func NewFileReader(storage *Storage) *Reader {
	return &Reader{Storage: storage}
}

func (r *Reader) ReadDir(ctx context.Context, dir string, out chan<- string) error {
	files, err := os.ReadDir(dir)
	if err != nil {
		return fmt.Errorf("failed to read %s: %w", dir, err)
	}

	for _, file := range files {
		select {
		case <-ctx.Done():
			return fmt.Errorf("Aborting sync: %w", ctx.Err())
		default:
			path := filepath.Join(dir, file.Name())
			if r.isExcluded(file) {
				slog.Info("Skipped upload", "path", path)
				continue
			}

			out <- path
		}
	}

	slog.Info("All files have been queued for uploading", "path", dir)
	return nil
}

func (r *Reader) isExcluded(file fs.DirEntry) bool {
	if file.IsDir() {
		return true
	}

	// Has the file already been uploaded?
	if r.Storage.GetItem(file.Name()) != nil {
		return true
	}

	return false
}
