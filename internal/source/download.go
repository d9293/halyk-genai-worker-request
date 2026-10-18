// Package source downloads a task document from its Attstore link as a
// stream, so a file of any size never has to fit in memory.
package source

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"
)

var (
	// ErrNotFound: the link points to nothing (404, 410). Trying again will not help.
	ErrNotFound = errors.New("source not found")
	// ErrForbidden: the worker may not read it (401, 403). Trying again will not help.
	ErrForbidden = errors.New("source forbidden")
	// ErrTooLarge: the file is bigger than the allowed maximum.
	ErrTooLarge = errors.New("source too large")
	// ErrUnavailable: the source did not answer properly (timeout, 5xx, 429); may work later.
	ErrUnavailable = errors.New("source unavailable")
)

type Config struct {
	// MaxBytes is the biggest file accepted; reading stops with ErrTooLarge beyond it.
	MaxBytes int64
	// HeaderTimeout bounds the wait for the source to start answering. The
	// body itself may take as long as the caller's context allows.
	HeaderTimeout time.Duration
}

type Downloader struct {
	cfg  Config
	http *http.Client
}

func New(cfg Config) *Downloader {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.ResponseHeaderTimeout = cfg.HeaderTimeout
	return &Downloader{cfg: cfg, http: &http.Client{Transport: transport}}
}

// Download is an open file. The caller must Close it.
type Download struct {
	// Body yields at most MaxBytes, then fails with ErrTooLarge.
	Body io.ReadCloser
	// ContentType as the source reported it; may be empty.
	ContentType string
}

// Open starts downloading url.
func (d *Downloader) Open(ctx context.Context, url string) (*Download, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("%w: invalid link: %w", ErrNotFound, err)
	}
	resp, err := d.http.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err() // shutting down or out of time, not the source's fault
		}
		return nil, fmt.Errorf("%w: %w", ErrUnavailable, err) // unreachable, timeout, connection reset
	}

	switch {
	case resp.StatusCode == http.StatusOK:
	case resp.StatusCode == http.StatusNotFound, resp.StatusCode == http.StatusGone:
		resp.Body.Close()
		return nil, fmt.Errorf("%w: HTTP %d", ErrNotFound, resp.StatusCode)
	case resp.StatusCode == http.StatusUnauthorized, resp.StatusCode == http.StatusForbidden:
		resp.Body.Close()
		return nil, fmt.Errorf("%w: HTTP %d", ErrForbidden, resp.StatusCode)
	case resp.StatusCode == http.StatusTooManyRequests, resp.StatusCode >= 500:
		resp.Body.Close()
		return nil, fmt.Errorf("%w: HTTP %d", ErrUnavailable, resp.StatusCode)
	default:
		resp.Body.Close()
		return nil, fmt.Errorf("%w: unexpected HTTP %d", ErrNotFound, resp.StatusCode)
	}

	if resp.ContentLength > d.cfg.MaxBytes {
		resp.Body.Close()
		return nil, fmt.Errorf("%w: %d bytes, the limit is %d", ErrTooLarge, resp.ContentLength, d.cfg.MaxBytes)
	}
	return &Download{
		Body:        &limitedBody{body: resp.Body, left: d.cfg.MaxBytes},
		ContentType: resp.Header.Get("Content-Type"),
	}, nil
}

// limitedBody fails with ErrTooLarge once more than the limit was read, even
// when the source did not announce its size. A failure while reading is
// reported as ErrUnavailable.
type limitedBody struct {
	body io.ReadCloser
	left int64
}

func (l *limitedBody) Read(p []byte) (int, error) {
	if l.left < 0 {
		return 0, ErrTooLarge
	}
	if int64(len(p)) > l.left+1 {
		p = p[:l.left+1] // read one byte past the limit to notice an oversized file
	}
	n, err := l.body.Read(p)
	l.left -= int64(n)
	if l.left < 0 {
		return n, ErrTooLarge
	}
	if err != nil && !errors.Is(err, io.EOF) {
		return n, fmt.Errorf("%w: %w", ErrUnavailable, err)
	}
	return n, err
}

func (l *limitedBody) Close() error { return l.body.Close() }
