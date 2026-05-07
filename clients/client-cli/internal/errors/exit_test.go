package clierr

import (
	"errors"
	"testing"
)

func TestCodeOf(t *testing.T) {
	cases := []struct {
		err      error
		expected int
	}{
		{nil, CodeOK},
		{errors.New("bare"), CodeServer},
		{User(errors.New("u")), CodeUser},
		{Server(errors.New("s")), CodeServer},
		{Auth(errors.New("a")), CodeAuth},
		{Empty("noop"), CodeEmpty},
		{Userf("hi %d", 1), CodeUser},
	}
	for _, c := range cases {
		if got := CodeOf(c.err); got != c.expected {
			t.Errorf("err=%v got %d want %d", c.err, got, c.expected)
		}
	}
}

func TestNilWrappers(t *testing.T) {
	if User(nil) != nil {
		t.Error("User(nil) should be nil")
	}
	if Server(nil) != nil {
		t.Error("Server(nil) should be nil")
	}
	if Auth(nil) != nil {
		t.Error("Auth(nil) should be nil")
	}
}
