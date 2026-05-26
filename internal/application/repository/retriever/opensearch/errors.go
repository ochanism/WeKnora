// Package opensearch implements the RetrieveEngineRepository interface
// for OpenSearch k-NN native vector search. See design notes at
// projects/weknora/issues/1208/design-pr2.md.
package opensearch

import "errors"

// Sentinel errors returned by Repository. Service-layer factory
// (engine_factory.go in PR 3) wraps these to typed AppError (2200/2201).
// Repository never imports internal/errors — the boundary is intentional
// (directional dependency).
var (
	// ErrIndexNotFound — alias / underlying index missing. Search and
	// delete-by-query operations return this when the per-dim alias has
	// not been created yet (no Save has been issued for that dim).
	ErrIndexNotFound = errors.New("opensearch: index not found")

	// ErrDimensionMismatch — embedding dimension violates the per-dim
	// invariant (e.g. dim <= 0, dim > 16000, or embeddings within a
	// single batch disagree).
	ErrDimensionMismatch = errors.New("opensearch: embedding dimension mismatch")

	// ErrAuth — cluster returned 401 / 403. Distinguished from ErrTransport
	// so the service layer can map to a clean 4xx instead of 503.
	ErrAuth = errors.New("opensearch: authentication failed")

	// ErrTransport — network / 5xx / opaque cluster error. Classified as
	// transient: ensureReady does NOT persist this in initErr, so the next
	// caller will retry.
	ErrTransport = errors.New("opensearch: transport error")

	// ErrVersionUnsupported — cluster is not OpenSearch, is OS 1.x, or is
	// OS 2.0~2.3 (pre-Lucene-HNSW-GA). probeVersion enforces.
	ErrVersionUnsupported = errors.New("opensearch: cluster version unsupported")

	// ErrConfigInvalid — IndexConfig / storeID / sanitizeIndexName guard
	// failed, or the k-NN plugin is missing on one or more cluster nodes.
	ErrConfigInvalid = errors.New("opensearch: invalid index config")

	// ErrFeatureNotEnabled — stubs.go returns this from CopyIndices /
	// BatchUpdateChunk* / swapToVersion. PR 3 ships real implementations
	// and removes these stubs (or recategorizes the sentinel).
	ErrFeatureNotEnabled = errors.New("opensearch: feature not enabled in this build")

	// ErrBatchTooLarge — Save / Delete batch exceeded the driver's sync
	// cap. Distinct from ErrFeatureNotEnabled so the service layer can
	// chunk + retry rather than treat as "wait for PR 3."
	ErrBatchTooLarge = errors.New("opensearch: batch size exceeds driver cap")

	// ErrCircuitBreaker — OpenSearch k-NN circuit breaker returned 429
	// (knn_circuit_breaker_exception). Classified as transient so the
	// caller can retry after the operator scales the cluster.
	ErrCircuitBreaker = errors.New("opensearch: knn circuit breaker open")
)

// isTransientErr classifies sentinel errors. Transient errors do not get
// persisted in ensureReady's initErr cache — they can be retried by the
// next caller after the underlying cause clears.
func isTransientErr(err error) bool {
	return errors.Is(err, ErrTransport) || errors.Is(err, ErrCircuitBreaker)
}
