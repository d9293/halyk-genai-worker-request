package document

import (
	"context"
	"errors"

	"github.com/d9293/halyk-genai-worker-request/internal/client/integrator"
	"github.com/d9293/halyk-genai-worker-request/internal/source"
	"github.com/d9293/halyk-genai-worker-request/internal/storage/s3"
)

// Failure codes reported to Integrator.Api.
const (
	CodeSourceNotFound     = "SOURCE_NOT_FOUND"
	CodeSourceForbidden    = "SOURCE_FORBIDDEN"
	CodeSourceTooLarge     = "SOURCE_TOO_LARGE"
	CodeSourceUnavailable  = "SOURCE_UNAVAILABLE"
	CodeStorageUnavailable = "STORAGE_UNAVAILABLE"
	CodeCopyTimeout        = "COPY_TIMEOUT"
	CodeCopyFailed         = "COPY_FAILED"
)

// classify turns a copy error into a failure for Integrator.Api, and whether
// trying again later may help. Messages are short and never contain the link
// itself, which may be signed.
func classify(err error) (integrator.Failure, bool) {
	switch {
	case errors.Is(err, source.ErrNotFound):
		return integrator.Failure{Code: CodeSourceNotFound, Message: "The document does not exist at its link"}, false
	case errors.Is(err, source.ErrForbidden):
		return integrator.Failure{Code: CodeSourceForbidden, Message: "Access to the document was refused"}, false
	case errors.Is(err, source.ErrTooLarge):
		return integrator.Failure{Code: CodeSourceTooLarge, Message: "The document is larger than allowed"}, false
	case errors.Is(err, source.ErrUnavailable):
		return integrator.Failure{Code: CodeSourceUnavailable, Message: "The document source did not answer properly"}, true
	case errors.Is(err, s3.ErrUnavailable):
		return integrator.Failure{Code: CodeStorageUnavailable, Message: "Storage did not accept the copy"}, true
	case errors.Is(err, context.DeadlineExceeded):
		return integrator.Failure{Code: CodeCopyTimeout, Message: "The copy took too long"}, true
	}
	return integrator.Failure{Code: CodeCopyFailed, Message: "The copy failed"}, true
}
