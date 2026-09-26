"""Load labeled behavior vectors (from `seed-export`) into Qdrant.

    python -m app.seed data/seed.jsonl
"""

import argparse
import json
import sys
import time

from qdrant_client import QdrantClient, models

from . import config


def wait_for(client: QdrantClient, timeout: float) -> None:
    deadline = time.monotonic() + timeout
    while True:
        try:
            client.get_collections()
            return
        except Exception:
            if time.monotonic() > deadline:
                raise
            time.sleep(1)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("file", help="JSON lines of {features, label}")
    parser.add_argument("--url", default=config.QDRANT_URL)
    parser.add_argument("--collection", default=config.COLLECTION)
    parser.add_argument("--batch", type=int, default=1000)
    args = parser.parse_args()

    client = QdrantClient(url=args.url)
    wait_for(client, timeout=60)
    if client.collection_exists(args.collection):
        client.delete_collection(args.collection)
    client.create_collection(
        args.collection,
        vectors_config=models.VectorParams(size=config.FEATURE_DIMS, distance=models.Distance.COSINE),
        # The default threshold (in KB) is far above this collection's size
        # (~1MB), which leaves every segment on brute-force search.
        optimizers_config=models.OptimizersConfigDiff(indexing_threshold=10, default_segment_number=2),
    )

    points: list[models.PointStruct] = []
    counts: dict[str, int] = {}
    with open(args.file) as f:
        for i, line in enumerate(f):
            row = json.loads(line)
            counts[row["label"]] = counts.get(row["label"], 0) + 1
            payload = {"label": row["label"]}
            if "persona" in row:
                payload["persona"] = row["persona"]
            points.append(models.PointStruct(id=i, vector=row["features"], payload=payload))
            if len(points) >= args.batch:
                client.upsert(args.collection, points, wait=True)
                points = []
    if points:
        client.upsert(args.collection, points, wait=True)

    # Wait until the HNSW index covers every point, so scoring is fast from the start.
    deadline = time.monotonic() + 120
    while True:
        info = client.get_collection(args.collection)
        if info.indexed_vectors_count >= info.points_count and info.status == models.CollectionStatus.GREEN:
            break
        if time.monotonic() > deadline:
            print("warning: index not complete after 120s", file=sys.stderr)
            break
        time.sleep(1)

    print(f"seeded {args.collection}: {counts}, indexed {info.indexed_vectors_count}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
