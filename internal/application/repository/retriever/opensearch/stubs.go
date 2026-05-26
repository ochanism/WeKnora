package opensearch

import (
	"context"

	"github.com/Tencent/WeKnora/internal/types"
)

// Methods in this file exist solely to satisfy the
// interfaces.RetrieveEngineRepository contract. They return
// ErrFeatureNotEnabled (or a conservative sentinel value) so that any
// accidental invocation surfaces loudly. PR 3 ships real
// implementations and removes / replaces these stubs.

// CopyIndices: PR 3 ships the async _reindex path with task polling
// for >10K-doc batches. Until then this is unreachable in production
// (no registry registration path activates the OpenSearch driver),
// but the stub fails closed if reached.
func (r *Repository) CopyIndices(
	_ context.Context,
	_ string, // sourceKnowledgeBaseID
	_ map[string]string, // sourceToTargetKBIDMap
	_ map[string]string, // sourceToTargetChunkIDMap
	_ string, // targetKnowledgeBaseID
	_ int, // dimension
	_ string, // knowledgeType
) error {
	return ErrFeatureNotEnabled
}

// BatchUpdateChunkEnabledStatus: PR 3 ships the _update_by_query path.
func (r *Repository) BatchUpdateChunkEnabledStatus(
	_ context.Context, _ map[string]bool,
) error {
	return ErrFeatureNotEnabled
}

// BatchUpdateChunkTagID: PR 3 ships the _update_by_query path.
func (r *Repository) BatchUpdateChunkTagID(
	_ context.Context, _ map[string]string,
) error {
	return ErrFeatureNotEnabled
}

// EstimateStorageSize: PR 3 ships the real impl that reads cluster
// `_stats` for the per-dim alias. PR 2 returns a conservative
// lower-bound estimate using the HNSW memory formula so the Phase 2
// KB delete guard fails-closed (treats non-empty KBs as "may free
// non-trivial storage, force confirmation") rather than failing open.
//
// Formula: N * (1024 content bytes + 4*dimGuess float + 128 HNSW M=16 overhead)
// dimGuess = 768 (common embedding size; conservative — actual impl in
// PR 3 reads real dim from cluster).
func (r *Repository) EstimateStorageSize(
	_ context.Context,
	indexInfoList []*types.IndexInfo,
	_ map[string]any,
) int64 {
	if len(indexInfoList) == 0 {
		return 0
	}
	const (
		contentBytes  = 1024 // average chunk content
		embDimGuess   = 768  // common embedding size
		hnswOverhead  = 128  // M=16 → 8*M = 128 bytes/vector
	)
	return int64(len(indexInfoList)) * int64(contentBytes+4*embDimGuess+hnswOverhead)
}

// swapToVersion is a stub for the future rolling-reindex swap path.
// Calling it is illegal in PR 2 — alias is fixed at "_v1" and only
// PR 3+ ships the swap orchestration. Surface exists so the future
// API contract is reviewable now.
//
// Unexported because it is not part of the public
// RetrieveEngineRepository interface — exposing it would require an
// interface widening in internal/types/interfaces.
func (r *Repository) swapToVersion(_ context.Context, _ int, _ int) error {
	return ErrFeatureNotEnabled
}
