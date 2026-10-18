package source

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func serve(t *testing.T, h http.HandlerFunc) string {
	t.Helper()
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	return srv.URL
}

func newDownloader(maxBytes int64) *Downloader {
	return New(Config{MaxBytes: maxBytes, HeaderTimeout: 200 * time.Millisecond})
}

func TestOpen_StreamsTheFile(t *testing.T) {
	url := serve(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/pdf")
		_, _ = w.Write([]byte("%PDF-1.4 content"))
	})

	dl, err := newDownloader(1024).Open(context.Background(), url)
	if err != nil {
		t.Fatal(err)
	}
	defer dl.Body.Close()
	data, err := io.ReadAll(dl.Body)
	if err != nil || string(data) != "%PDF-1.4 content" || dl.ContentType != "application/pdf" {
		t.Errorf("data %q, type %q, err %v", data, dl.ContentType, err)
	}
}

func TestOpen_Statuses(t *testing.T) {
	tests := []struct {
		status int
		want   error
	}{
		{http.StatusNotFound, ErrNotFound},
		{http.StatusGone, ErrNotFound},
		{http.StatusForbidden, ErrForbidden},
		{http.StatusUnauthorized, ErrForbidden},
		{http.StatusInternalServerError, ErrUnavailable},
		{http.StatusServiceUnavailable, ErrUnavailable},
		{http.StatusTooManyRequests, ErrUnavailable},
	}
	for _, tt := range tests {
		t.Run(http.StatusText(tt.status), func(t *testing.T) {
			url := serve(t, func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(tt.status) })
			if _, err := newDownloader(1024).Open(context.Background(), url); !errors.Is(err, tt.want) {
				t.Errorf("err = %v, want %v", err, tt.want)
			}
		})
	}
}

func TestOpen_TooLargeByContentLength(t *testing.T) {
	url := serve(t, func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte(strings.Repeat("x", 100))) })
	if _, err := newDownloader(99).Open(context.Background(), url); !errors.Is(err, ErrTooLarge) {
		t.Errorf("err = %v, want ErrTooLarge", err)
	}
}

func TestOpen_TooLargeWhileStreaming(t *testing.T) {
	// No Content-Length: the size is only known while reading.
	url := serve(t, func(w http.ResponseWriter, _ *http.Request) {
		for range 10 {
			_, _ = w.Write([]byte(strings.Repeat("x", 50)))
			w.(http.Flusher).Flush()
		}
	})
	dl, err := newDownloader(300).Open(context.Background(), url)
	if err != nil {
		t.Fatal(err)
	}
	defer dl.Body.Close()
	if _, err := io.ReadAll(dl.Body); !errors.Is(err, ErrTooLarge) {
		t.Errorf("err = %v, want ErrTooLarge", err)
	}
}

func TestOpen_ExactlyTheLimitIsFine(t *testing.T) {
	url := serve(t, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(strings.Repeat("x", 100)))
		w.(http.Flusher).Flush()
	})
	dl, err := newDownloader(100).Open(context.Background(), url)
	if err != nil {
		t.Fatal(err)
	}
	defer dl.Body.Close()
	if data, err := io.ReadAll(dl.Body); err != nil || len(data) != 100 {
		t.Errorf("read %d bytes, err %v", len(data), err)
	}
}

func TestOpen_SlowSourceTimesOut(t *testing.T) {
	url := serve(t, func(w http.ResponseWriter, _ *http.Request) { time.Sleep(time.Second) })
	if _, err := newDownloader(100).Open(context.Background(), url); !errors.Is(err, ErrUnavailable) {
		t.Errorf("err = %v, want ErrUnavailable", err)
	}
}

func TestOpen_UnreachableSource(t *testing.T) {
	if _, err := newDownloader(100).Open(context.Background(), "http://127.0.0.1:1/x.pdf"); !errors.Is(err, ErrUnavailable) {
		t.Errorf("err = %v, want ErrUnavailable", err)
	}
}
