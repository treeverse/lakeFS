package kv

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"time"
)

// DumpFormat represents the overall structure of a KV dump
type DumpFormat struct {
	Version   string     `json:"version"`
	Timestamp string     `json:"timestamp"`
	Entries   []RawEntry `json:"entries"`
}

// RawEntry represents a raw key-value entry with partition
type RawEntry struct {
	Partition string `json:"partition"`
	Key       string `json:"key"`
	Value     string `json:"value"` // base64 encoded
}

// Dumper handles dumping of KV store data
type Dumper struct {
	store Store
}

// NewDumper creates a new dumper
func NewDumper(store Store) *Dumper {
	return &Dumper{store: store}
}

// DumpPartition dumps all data from a specific partition
func (d *Dumper) DumpPartition(ctx context.Context, partition string) ([]RawEntry, error) {
	partitionKey := []byte(partition)
	var entries []RawEntry

	iter, err := d.store.Scan(ctx, partitionKey, ScanOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to create iterator: %w", err)
	}
	defer iter.Close()

	for iter.Next() {
		entry := iter.Entry()
		if entry == nil {
			continue
		}

		// Encode value as base64
		valueBase64 := base64.StdEncoding.EncodeToString(entry.Value)

		rawEntry := RawEntry{
			Partition: partition,
			Key:       string(entry.Key),
			Value:     valueBase64,
		}
		entries = append(entries, rawEntry)
	}

	if iter.Err() != nil {
		return nil, fmt.Errorf("iterator error: %w", iter.Err())
	}

	return entries, nil
}

// DumpPartitions dumps data from multiple partitions
func (d *Dumper) DumpPartitions(ctx context.Context, partitions []string) ([]RawEntry, error) {
	var allEntries []RawEntry

	for _, partition := range partitions {
		entries, err := d.DumpPartition(ctx, partition)
		if err != nil {
			return nil, fmt.Errorf("failed to dump partition %s: %w", partition, err)
		}
		allEntries = append(allEntries, entries...)
	}

	return allEntries, nil
}

// Loader handles loading of KV store data
type Loader struct {
	store Store
}

// NewLoader creates a new loader
func NewLoader(store Store) *Loader {
	return &Loader{store: store}
}

// LoadEntries loads raw entries into the KV store
func (l *Loader) LoadEntries(ctx context.Context, entries []RawEntry, strategy LoadStrategy) error {
	for _, entry := range entries {
		partitionKey := []byte(entry.Partition)
		key := []byte(entry.Key)

		// Check if key exists when using override or skip strategy
		if strategy == LoadStrategySkip {
			_, err := l.store.Get(ctx, partitionKey, key)
			if err != nil {
				if errors.Is(err, ErrNotFound) {
					continue // Skip non-existing keys
				}
				return fmt.Errorf("failed to check if key exists: %w", err)
			}
		}

		// Decode base64 value
		valueBytes, err := base64.StdEncoding.DecodeString(entry.Value)
		if err != nil {
			return fmt.Errorf("failed to decode base64 value for key %s: %w", entry.Key, err)
		}

		// Store the entry
		if err := l.store.Set(ctx, partitionKey, key, valueBytes); err != nil {
			return fmt.Errorf("failed to store key %s: %w", entry.Key, err)
		}
	}

	return nil
}

// LoadStrategy defines how to handle existing data during load
type LoadStrategy string

const (
	LoadStrategyOverwrite LoadStrategy = "overwrite"
	LoadStrategySkip      LoadStrategy = "skip"
)

// SectionMapping defines which partitions and key prefixes belong to each section
var SectionMapping = map[string][]string{
	"auth":     {"auth", "basicAuth", "aclauth"}, // auth section maps to all auth-related partitions
	"pulls":    {"pulls"},                        // pull requests section
	"metadata": {"kv-internal-metadata"},         // internal metadata section
}

// CreateDump creates a complete dump with the specified sections
func CreateDump(ctx context.Context, store Store, sections []string) (*DumpFormat, error) {
	// Default to auth section if no sections specified
	if len(sections) == 0 {
		sections = []string{"auth"}
	}

	// Map sections to partitions
	partitionSet := make(map[string]bool)
	for _, section := range sections {
		partitions, exists := SectionMapping[section]
		if !exists {
			return nil, fmt.Errorf("%w section: %s", ErrUnsupported, section)
		}
		for _, partition := range partitions {
			partitionSet[partition] = true
		}
	}

	// Convert set to slice
	var partitions []string
	for partition := range partitionSet {
		partitions = append(partitions, partition)
	}

	dumper := NewDumper(store)
	entries, err := dumper.DumpPartitions(ctx, partitions)
	if err != nil {
		return nil, fmt.Errorf("failed to dump sections: %w", err)
	}

	dump := &DumpFormat{
		Version:   "1.0", // Current format version
		Timestamp: time.Now().UTC().Format(time.RFC3339),
		Entries:   entries,
	}

	return dump, nil
}

// LoadDump loads data from a dump into the KV store
func LoadDump(ctx context.Context, store Store, dump *DumpFormat, sectionsToLoad []string, strategy LoadStrategy) error {
	if dump.Version != "1.0" {
		return fmt.Errorf("%w dump version: %s (supported: 1.0)", ErrUnsupported, dump.Version)
	}

	var entriesToLoad []RawEntry

	// If specific sections are requested, filter entries by mapping sections to partitions
	if len(sectionsToLoad) > 0 {
		partitionSet := make(map[string]bool)
		for _, section := range sectionsToLoad {
			partitions, exists := SectionMapping[section]
			if !exists {
				return fmt.Errorf("%w section: %s", ErrUnsupported, section)
			}
			for _, partition := range partitions {
				partitionSet[partition] = true
			}
		}

		for _, entry := range dump.Entries {
			if partitionSet[entry.Partition] {
				entriesToLoad = append(entriesToLoad, entry)
			}
		}
	} else {
		// Load all entries
		entriesToLoad = dump.Entries
	}

	loader := NewLoader(store)
	return loader.LoadEntries(ctx, entriesToLoad, strategy)
}
