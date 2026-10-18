// Package s3 stores document copies in S3-compatible storage (MinIO locally).
// Uploads are streamed in parts, so memory use does not grow with file size.
package s3

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// ErrUnavailable: storage did not accept the upload; it may work later.
var ErrUnavailable = errors.New("storage unavailable")

type Config struct {
	// Endpoint like http://localhost:9000; https uses TLS.
	Endpoint        string
	Region          string
	Bucket          string
	AccessKeyID     string
	SecretAccessKey string
	// PartSize is the multipart chunk size; each upload in progress holds about one part in memory.
	PartSize uint64
}

type Store struct {
	client   *minio.Client
	bucket   string
	partSize uint64
}

// New connects to the storage and checks the bucket exists, so a wrong
// endpoint, key or bucket stops the worker at startup.
func New(ctx context.Context, cfg Config) (*Store, error) {
	endpoint, err := url.Parse(cfg.Endpoint)
	if err != nil || endpoint.Host == "" {
		return nil, fmt.Errorf("invalid storage endpoint %q", cfg.Endpoint)
	}
	client, err := minio.New(endpoint.Host, &minio.Options{
		Creds:  credentials.NewStaticV4(cfg.AccessKeyID, cfg.SecretAccessKey, ""),
		Secure: endpoint.Scheme == "https",
		Region: cfg.Region,
	})
	if err != nil {
		return nil, fmt.Errorf("storage client: %w", err)
	}

	exists, err := client.BucketExists(ctx, cfg.Bucket)
	if err != nil {
		return nil, fmt.Errorf("check bucket %s at %s: %w", cfg.Bucket, cfg.Endpoint, err)
	}
	if !exists {
		return nil, fmt.Errorf("bucket %s does not exist at %s", cfg.Bucket, cfg.Endpoint)
	}
	return &Store{client: client, bucket: cfg.Bucket, partSize: cfg.PartSize}, nil
}

// Put streams r into the object key and returns its s3:// link. The size need
// not be known in advance. On failure nothing is left behind: an unfinished
// multipart upload is aborted.
func (s *Store) Put(ctx context.Context, key string, r io.Reader, contentType string) (string, error) {
	_, err := s.client.PutObject(ctx, s.bucket, key, r, -1, minio.PutObjectOptions{
		ContentType: contentType,
		PartSize:    s.partSize,
	})
	if err != nil {
		if ctx.Err() != nil {
			return "", ctx.Err()
		}
		return "", fmt.Errorf("%w: upload %s: %w", ErrUnavailable, key, err)
	}
	return "s3://" + s.bucket + "/" + key, nil
}

// Bucket is the bucket objects are stored in.
func (s *Store) Bucket() string { return s.bucket }

// Client exposes the underlying client, for tests.
func (s *Store) Client() *minio.Client { return s.client }
