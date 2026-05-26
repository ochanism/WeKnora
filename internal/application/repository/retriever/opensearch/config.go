package opensearch

import "github.com/Tencent/WeKnora/internal/types"

// internalCfg is the driver-internal, immutable view of IndexConfig.
// Defaults match the OpenSearch entry that ships in Phase 3 PR 3
// (GetVectorStoreTypes) so the env-path (no IndexConfig) and DB-store-
// path (with IndexConfig) produce identical mappings.
type internalCfg struct {
	shards             int
	replicas           int
	knnEngine          string // "lucene" | "faiss"
	hnswM              int
	hnswEFConstruction int
	efSearch           int
}

// buildInternalCfg projects IndexConfig to the driver-internal view,
// substituting defaults for unset fields. Validation of value ranges
// (D15 caps: m <= 100, ef_construction etc.) is a service-layer concern
// shipped in PR 3 (validateOpenSearchIndexConfig); this function applies
// defaults only and never rejects.
//
// OpenSearch-specific overrides (knn_engine, hnsw_m, hnsw_ef_construction,
// hnsw_ef_search) are intentionally NOT read from IndexConfig in this PR:
// IndexConfig is a Phase 1 schema shared across all drivers, and adding
// OpenSearch-specific fields would surface them in the Phase 1
// VectorStoreFieldInfo schema visible to every driver's create form.
// PR 3 extends IndexConfig with those fields together with the activation
// switch.
func buildInternalCfg(c *types.IndexConfig) (internalCfg, error) {
	cfg := internalCfg{
		shards:             4,   // Phase 1 ES default kept (design v2 § 14-F)
		replicas:           1,   // assumes >= 2 node cluster
		knnEngine:          "lucene", // OS default; Faiss preferred only at >= 10M docs
		hnswM:              16,  // OS official default
		hnswEFConstruction: 100, // OS official default (was 256 in design v1 — corrected)
		efSearch:           100, // OS default
	}
	if c == nil {
		return cfg, nil
	}
	if c.NumberOfShards > 0 {
		cfg.shards = c.NumberOfShards
	}
	if c.NumberOfReplicas > 0 {
		cfg.replicas = c.NumberOfReplicas
	}
	return cfg, nil
}
