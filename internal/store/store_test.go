package store

import "testing"

func TestPutGetDelete(t *testing.T) {
	s := New()

	if _, ok := s.Get("missing"); ok {
		t.Fatalf("Get on empty store: want not found")
	}

	s.Put("hello", "world")
	if v, ok := s.Get("hello"); !ok || v != "world" {
		t.Fatalf("Get after Put = (%q, %v), want (world, true)", v, ok)
	}

	s.Put("hello", "there")
	if v, _ := s.Get("hello"); v != "there" {
		t.Fatalf("Get after overwrite = %q, want there", v)
	}

	if existed := s.Delete("hello"); !existed {
		t.Fatalf("Delete existing key: want existed=true")
	}
	if _, ok := s.Get("hello"); ok {
		t.Fatalf("Get after Delete: want not found")
	}
	if existed := s.Delete("hello"); existed {
		t.Fatalf("Delete missing key: want existed=false")
	}
}
