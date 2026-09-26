import os

QDRANT_URL = os.environ.get("QDRANT_URL", "http://localhost:6333")
COLLECTION = os.environ.get("QDRANT_COLLECTION", "behavior_profiles")
KNN_K = int(os.environ.get("KNN_K", "10"))
# HNSW search breadth; lower is faster and slightly less exact.
HNSW_EF = int(os.environ.get("HNSW_EF", "32"))
# Must match detect.FeatureDims in the Go engine.
FEATURE_DIMS = 5
