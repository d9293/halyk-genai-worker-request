package s3

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
)

// These tests use a real S3 (the local MinIO). Skipped unless S3_TEST_ENDPOINT is set:
//
//	S3_TEST_ENDPOINT=http://localhost:9000 S3_TEST_BUCKET=bpm-bucket \
//	S3_TEST_ACCESS_KEY_ID=... S3_TEST_SECRET_ACCESS_KEY=... go test ./internal/storage/s3/
//
// Objects are written under test/worker-request/. The worker's keys may lack
// delete rights (the worker never deletes); then a test object stays behind
// and the test says so.
func newStore(t *testing.T) *Store {
	t.Helper()
	endpoint := os.Getenv("S3_TEST_ENDPOINT")
	if endpoint == "" {
		t.Skip("S3_TEST_ENDPOINT not set")
	}
	store, err := New(context.Background(), Config{
		Endpoint:        endpoint,
		Region:          "us-east-1",
		Bucket:          os.Getenv("S3_TEST_BUCKET"),
		AccessKeyID:     os.Getenv("S3_TEST_ACCESS_KEY_ID"),
		SecretAccessKey: os.Getenv("S3_TEST_SECRET_ACCESS_KEY"),
		PartSize:        5 << 20, // the S3 minimum, so the 12 MiB test uses several parts
	})
	if err != nil {
		t.Fatal(err)
	}
	return store
}

func TestPut_StreamsInPartsAndStoresExactBytes(t *testing.T) {
	store := newStore(t)
	ctx := context.Background()
	key := "test/worker-request/" + time.Now().UTC().Format("20060102T150405.000") + ".bin"
	t.Cleanup(func() {
		if err := store.Client().RemoveObject(ctx, store.Bucket(), key, minio.RemoveObjectOptions{}); err != nil {
			t.Logf("could not remove test object %s (the keys may lack delete rights): %v", key, err)
		}
	})

	data := make([]byte, 12<<20) // 12 MiB: three parts
	_, _ = rand.Read(data)
	// An io.Reader without a known size, like a download stream.
	uri, err := store.Put(ctx, key, io.MultiReader(bytes.NewReader(data)), "application/pdf")
	if err != nil {
		t.Fatal(err)
	}
	if uri != "s3://"+store.Bucket()+"/"+key {
		t.Errorf("uri = %s", uri)
	}

	obj, err := store.Client().GetObject(ctx, store.Bucket(), key, minio.GetObjectOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer obj.Close()
	stored, err := io.ReadAll(obj)
	if err != nil {
		t.Fatal(err)
	}
	if sha256.Sum256(stored) != sha256.Sum256(data) {
		t.Errorf("stored %d bytes differ from the %d uploaded", len(stored), len(data))
	}
	info, _ := obj.Stat()
	if info.ContentType != "application/pdf" {
		t.Errorf("content type = %q", info.ContentType)
	}
}

func TestPut_FailingSourceLeavesNothing(t *testing.T) {
	store := newStore(t)
	ctx := context.Background()
	key := "test/worker-request/broken-" + time.Now().UTC().Format("20060102T150405.000") + ".bin"

	// The download breaks after 7 MiB, in the middle of the second part.
	broken := io.MultiReader(bytes.NewReader(make([]byte, 7<<20)), errReader{})
	if _, err := store.Put(ctx, key, broken, "application/pdf"); err == nil {
		t.Fatal("expected an error")
	}

	if _, err := store.Client().StatObject(ctx, store.Bucket(), key, minio.StatObjectOptions{}); err == nil {
		t.Error("a broken upload left an object behind")
	}
	for upload := range store.Client().ListIncompleteUploads(ctx, store.Bucket(), key, false) {
		if upload.Key == key {
			t.Errorf("an unfinished multipart upload was left behind: %s", upload.UploadID)
		}
	}
}

// A bucket that is missing, or that the keys may not use (MinIO then answers
// Access Denied rather than "does not exist"), stops the worker at startup.
func TestNew_UnusableBucketFailsAtStartup(t *testing.T) {
	newStore(t) // skip unless configured
	_, err := New(context.Background(), Config{
		Endpoint: os.Getenv("S3_TEST_ENDPOINT"), Region: "us-east-1", Bucket: "no-such-bucket-for-worker-tests",
		AccessKeyID: os.Getenv("S3_TEST_ACCESS_KEY_ID"), SecretAccessKey: os.Getenv("S3_TEST_SECRET_ACCESS_KEY"),
		PartSize: 5 << 20,
	})
	if err == nil || !strings.Contains(err.Error(), "no-such-bucket-for-worker-tests") {
		t.Errorf("err = %v, want a startup error naming the bucket", err)
	}
}

type errReader struct{}

func (errReader) Read([]byte) (int, error) { return 0, errors.New("download interrupted") }
