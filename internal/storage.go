package internal

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"
)

type Storage struct {
	// The path to the JSON file recording all file transfers
	Path string

	// The in-memory store of all successful transfers
	items map[string]UploadRecord

	// The in-memory store of all successful transfers
	itemsMutex sync.Mutex
}

func NewFileStorage(path string) *Storage {
	return &Storage{
		Path:       path,
		items:      map[string]UploadRecord{},
		itemsMutex: sync.Mutex{},
	}
}

func (s *Storage) SetItem(path string, record UploadRecord) {
	s.itemsMutex.Lock()
	defer s.itemsMutex.Unlock()

	s.items[path] = record
}

func (s *Storage) GetItem(path string) *UploadRecord {
	s.itemsMutex.Lock()
	defer s.itemsMutex.Unlock()

	if ret, ok := s.items[path]; ok {
		return &ret
	}

	return nil
}

func (s *Storage) Save() error {
	s.itemsMutex.Lock()
	defer s.itemsMutex.Unlock()

	f, err := os.Create(s.Path)
	if err != nil {
		return fmt.Errorf("unable to open %q file for writing: %w", s.Path, err)
	}
	defer f.Close()

	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	err = enc.Encode(&s.items)
	if err != nil {
		return fmt.Errorf("unable to serialize file upload log to %q file for writing: %w", s.Path, err)
	}

	return nil
}

func (s *Storage) Load() error {
	s.itemsMutex.Lock()
	defer s.itemsMutex.Unlock()

	f, err := os.Open(s.Path)
	if err != nil && os.IsNotExist(err) {
		return nil
	} else if err != nil {
		return fmt.Errorf("unable to open or create %q file for reading: %w", s.Path, err)
	}
	defer f.Close()

	err = json.NewDecoder(f).Decode(&s.items)
	if err != nil {
		return fmt.Errorf("unable to unserialize file upload log from %q file: %w", s.Path, err)
	}

	return nil
}
