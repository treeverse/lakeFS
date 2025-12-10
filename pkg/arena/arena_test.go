package arena_test

import (
	"fmt"
	"testing"

	"github.com/treeverse/lakefs/pkg/arena"
)

func TestArena(t *testing.T) {
	a := arena.New[string]()

	hello := a.Add("hello")
	goodbye := a.Add("goodbye")

	gotHello := a.Get(hello)
	if gotHello == nil {
		t.Errorf("Could not retrieve index %v", hello)
	} else {
		if *gotHello != "hello" {
			t.Errorf("Got %s != hello at index %v", *gotHello, hello)
		}
	}

	// Add enough objects to realloc a few times.
	for i := range 1000 {
		_ = a.Add(fmt.Sprintf("%06d", i))
	}

	gotGoodbye := a.Get(goodbye)
	if gotGoodbye == nil {
		t.Errorf("Could not retrieve index %v", goodbye)
	} else {
		if *gotGoodbye != "goodbye" {
			t.Errorf("Got %s != goodbye at index %v", *gotGoodbye, goodbye)
		}
	}

	// Verify we can still get hello
	gotHello = a.Get(hello)
	if gotHello == nil {
		t.Errorf("Could not retrieve index %v", hello)
	} else {
		if *gotHello != "hello" {
			t.Errorf("Got %s != hello at index %v", *gotHello, hello)
		}
	}

	b := arena.New[string]()
	// This is invalid; if b had any elements it _might_ also return some other value.
	gotBadHello := b.Get(hello)
	if gotBadHello != nil {
		t.Errorf("Expected to get nothing but got %s at %v in an empty Arena.", *gotBadHello, hello)
	}
}

func TestArenaMap(t *testing.T) {
	m := arena.NewMap[string, string]()

	// Test basic Put and Get
	putPtr1 := m.Put("foo", "value1")
	if putPtr1 == nil {
		t.Errorf("Put returned nil pointer for foo")
	} else if *putPtr1 != "value1" {
		t.Errorf("Put returned pointer to %s != value1 for foo", *putPtr1)
	}

	m.Put("bar", "value2")

	gotValue1 := m.Get("foo")
	if gotValue1 == nil {
		t.Errorf("Could not retrieve value for foo")
	} else {
		if *gotValue1 != "value1" {
			t.Errorf("Got %s != value1 for foo", *gotValue1)
		}
	}

	gotValue2 := m.Get("bar")
	if gotValue2 == nil {
		t.Errorf("Could not retrieve value for bar")
	} else {
		if *gotValue2 != "value2" {
			t.Errorf("Got %s != value2 for bar", *gotValue2)
		}
	}

	// Test updating an existing key
	m.Put("foo", "updated1")
	gotUpdated := m.Get("foo")
	if gotUpdated == nil {
		t.Errorf("Could not retrieve updated value for foo")
	} else {
		if *gotUpdated != "updated1" {
			t.Errorf("Got %s != updated1 for foo after update", *gotUpdated)
		}
	}

	// Test non-existent key
	gotMissing := m.Get("nonexistent")
	if gotMissing != nil {
		t.Errorf("Expected nil for non-existent key, but got %s", *gotMissing)
	}

	// Add many entries to test that arena reallocation doesn't break map
	for i := range 1000 {
		m.Put(fmt.Sprintf("key%d", i), fmt.Sprintf("value%d", i))
	}

	// Verify original keys still work
	gotValue1Again := m.Get("foo")
	if gotValue1Again == nil {
		t.Errorf("Could not retrieve foo after many additions")
	} else {
		if *gotValue1Again != "updated1" {
			t.Errorf("Got %s != updated1 for foo after many additions", *gotValue1Again)
		}
	}

	gotValue2Again := m.Get("bar")
	if gotValue2Again == nil {
		t.Errorf("Could not retrieve bar after many additions")
	} else {
		if *gotValue2Again != "value2" {
			t.Errorf("Got %s != value2 for bar after many additions", *gotValue2Again)
		}
	}
}
