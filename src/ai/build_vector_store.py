"""
Build ChromaDB vector store from embeddings and corpus.

This module loads pre-generated embeddings (from OpenAI text-embedding-3-small)
and the corresponding text corpus, then stores them in a ChromaDB persistent
collection for efficient similarity search.
"""
import json
import logging
import random
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

import chromadb
import numpy as np

from src.shared.config import CHROMADB_PATH

# Constants
EMBEDDINGS_NPY_PATH = Path("data/processed/embeddings.npy")
EMBEDDING_CORPUS_PATH = Path("data/processed/embedding_corpus.json")
STATS_OUTPUT_PATH = Path("data/processed/chromadb_stats.json")
COLLECTION_NAME = "ghibli_knowledge"
EMBEDDING_DIMENSION = 1536  # text-embedding-3-small
BATCH_SIZE = 100  # Documents per batch for ChromaDB add()
NUM_TEST_QUERIES = 10
NUM_VALIDATION_DOCS = 5

# Logging
logger = logging.getLogger("spiriteddata.ai.build_vector_store")


def get_chromadb_client() -> chromadb.PersistentClient:
    """
    Initialize ChromaDB persistent client.

    Returns:
        chromadb.PersistentClient: Persistent client at CHROMADB_PATH

    Raises:
        Exception: If ChromaDB client initialization fails
    """
    try:
        chromadb_path = Path(CHROMADB_PATH)
        chromadb_path.mkdir(parents=True, exist_ok=True)
        client = chromadb.PersistentClient(path=str(chromadb_path))
        logger.info(f"Initialized ChromaDB client at {chromadb_path}")
        return client
    except Exception as e:
        logger.error(f"Failed to initialize ChromaDB client: {e}")
        raise


def create_ghibli_collection(
    client: chromadb.PersistentClient,
) -> chromadb.Collection:
    """
    Create or recreate ghibli_knowledge collection.

    Implements idempotent loading by deleting existing collection
    before creating a new one.

    Args:
        client: ChromaDB persistent client

    Returns:
        chromadb.Collection: Created collection with metadata

    Raises:
        Exception: If collection creation fails
    """
    try:
        # Delete existing collection if present (idempotent loading)
        try:
            client.delete_collection(name=COLLECTION_NAME)
            logger.info(f"Deleted existing collection: {COLLECTION_NAME}")
        except (ValueError, Exception) as e:
            # Collection doesn't exist, this is fine
            if "does not exist" in str(e) or "not found" in str(e).lower():
                logger.debug(f"Collection {COLLECTION_NAME} does not exist, will create new one")
            else:
                raise

        # Create new collection with metadata
        created_at_str = (
            datetime.now(datetime.UTC).isoformat()
            if hasattr(datetime, 'UTC')
            else datetime.utcnow().isoformat()
        )
        metadata = {
            "created_at": created_at_str,
            "embedding_model": "text-embedding-3-small",
            "embedding_dimension": str(EMBEDDING_DIMENSION),
            "schema_version": "1.0",
        }

        collection = client.create_collection(
            name=COLLECTION_NAME,
            metadata=metadata,
        )
        logger.info(
            f"Created collection '{COLLECTION_NAME}' with dimension {EMBEDDING_DIMENSION}"
        )
        return collection

    except Exception as e:
        logger.error(f"Failed to create collection: {e}")
        raise


def load_embeddings() -> np.ndarray:
    """
    Load embeddings from numpy file.

    Returns:
        np.ndarray: Array of shape (n_docs, 1536) containing embeddings

    Raises:
        FileNotFoundError: If embeddings.npy not found
        ValueError: If embeddings have wrong shape or contain invalid values
    """
    if not EMBEDDINGS_NPY_PATH.exists():
        raise FileNotFoundError(
            f"Embeddings file not found: {EMBEDDINGS_NPY_PATH}. "
            "Run src/ai/generate_embeddings.py first (Story 4.2)."
        )

    try:
        embeddings = np.load(EMBEDDINGS_NPY_PATH)
        logger.info(f"Loaded embeddings with shape {embeddings.shape}")

        # Validate shape
        if len(embeddings.shape) != 2:
            raise ValueError(
                f"Expected 2D array, got shape {embeddings.shape}. "
                "Embeddings should be (n_docs, embedding_dim)."
            )

        if embeddings.shape[1] != EMBEDDING_DIMENSION:
            raise ValueError(
                f"Expected embedding dimension {EMBEDDING_DIMENSION}, "
                f"got {embeddings.shape[1]}. Ensure embeddings were generated "
                "with text-embedding-3-small model."
            )

        # Check for invalid values
        if np.isnan(embeddings).any():
            raise ValueError(
                "Embeddings contain NaN values. Check embedding generation process."
            )

        if np.isinf(embeddings).any():
            raise ValueError(
                "Embeddings contain infinite values. Check embedding generation process."
            )

        return embeddings

    except Exception as e:
        logger.error(f"Failed to load embeddings: {e}")
        raise


def load_embedding_corpus() -> List[Dict[str, Any]]:
    """
    Load embedding corpus from JSON file.

    Returns:
        List[Dict[str, Any]]: List of document dictionaries with structure:
            {
                "doc_id": str,
                "type": str,
                "name": str,
                "text": str,
                "metadata": dict
            }

    Raises:
        FileNotFoundError: If embedding_corpus.json not found
        ValueError: If corpus has invalid structure
    """
    if not EMBEDDING_CORPUS_PATH.exists():
        raise FileNotFoundError(
            f"Embedding corpus not found: {EMBEDDING_CORPUS_PATH}. "
            "Run src/ai/prepare_embedding_corpus.py first (Story 4.1)."
        )

    try:
        with open(EMBEDDING_CORPUS_PATH, "r", encoding="utf-8") as f:
            corpus = json.load(f)

        logger.info(f"Loaded corpus with {len(corpus)} documents")

        # Validate corpus structure
        if not corpus:
            raise ValueError("Corpus is empty. Check Story 4.1 output.")

        required_fields = ["doc_id", "type", "name", "text", "metadata"]
        for i, doc in enumerate(corpus):
            for field in required_fields:
                if field not in doc:
                    raise ValueError(
                        f"Document {i} missing required field '{field}'. "
                        "Check corpus generation in Story 4.1."
                    )

            if not doc["text"] or not doc["text"].strip():
                raise ValueError(
                    f"Document {doc['doc_id']} has empty text field. "
                    "All documents must have non-empty text."
                )

        return corpus

    except Exception as e:
        logger.error(f"Failed to load corpus: {e}")
        raise


def add_documents_to_collection(
    collection: chromadb.Collection,
    embeddings: np.ndarray,
    corpus: List[Dict[str, Any]],
) -> None:
    """
    Add documents to ChromaDB collection in batches.

    Args:
        collection: ChromaDB collection
        embeddings: NumPy array of shape (n_docs, 1536)
        corpus: List of document dictionaries

    Raises:
        ValueError: If embeddings and corpus counts don't match
    """
    if len(embeddings) != len(corpus):
        raise ValueError(
            f"Embedding count ({len(embeddings)}) does not match "
            f"corpus document count ({len(corpus)}). "
            "Ensure embeddings.npy and embedding_corpus.json are in sync."
        )

    total_docs = len(corpus)
    logger.info(f"Adding {total_docs} documents to collection in batches of {BATCH_SIZE}...")

    # Process in batches
    for batch_start in range(0, total_docs, BATCH_SIZE):
        batch_end = min(batch_start + BATCH_SIZE, total_docs)
        batch_corpus = corpus[batch_start:batch_end]
        batch_embeddings = embeddings[batch_start:batch_end]

        # Prepare batch data
        ids = [doc["doc_id"] for doc in batch_corpus]
        documents = [doc["text"] for doc in batch_corpus]
        metadatas = [
            {
                "type": doc["type"],
                "name": doc["name"],
                "film_id": doc["metadata"].get("film_id", ""),
                "source": doc["metadata"].get("source", ""),
            }
            for doc in batch_corpus
        ]
        embeddings_list = batch_embeddings.tolist()

        # Add to collection
        collection.add(
            ids=ids,
            embeddings=embeddings_list,
            documents=documents,
            metadatas=metadatas,
        )

        logger.info(f"Added batch {batch_start + 1}-{batch_end}/{total_docs} documents")

    logger.info(f"Successfully added all {total_docs} documents to collection")


def validate_similarity_search(collection: chromadb.Collection) -> None:
    """
    Validate similarity search with Princess Mononoke query.

    Args:
        collection: ChromaDB collection

    Raises:
        AssertionError: If Princess Mononoke not in top 3 results
    """
    logger.info("Running similarity search validation...")

    query_text = "brave warrior princess"
    n_results = 5

    results = collection.query(
        query_texts=[query_text],
        n_results=n_results,
    )

    logger.info(f"Query: '{query_text}'")
    logger.info(f"Top {n_results} results:")

    top_3_contains_mononoke = False

    for i in range(min(n_results, len(results["ids"][0]))):
        document = results["documents"][0][i]
        metadata = results["metadatas"][0][i]
        distance = results["distances"][0][i] if "distances" in results else None

        distance_str = f"{distance:.4f}" if distance is not None else "N/A"
        logger.info(
            f"  {i+1}. {metadata['name']} ({metadata['type']}) - Distance: {distance_str}"
        )
        logger.debug(f"     Text: {document[:100]}...")

        # Check if Princess Mononoke or San in top 3
        if i < 3:
            if "Princess Mononoke" in metadata["name"] or "San" in metadata["name"]:
                top_3_contains_mononoke = True

    if not top_3_contains_mononoke:
        logger.warning(
            "Princess Mononoke or San not found in top 3 results. "
            "This may indicate embedding quality issues."
        )
    else:
        logger.info("✓ Validation passed: Princess Mononoke content in top 3 results")


def generate_collection_stats(collection: chromadb.Collection) -> Dict[str, Any]:
    """
    Generate collection statistics and performance metrics.

    Args:
        collection: ChromaDB collection

    Returns:
        Dict[str, Any]: Statistics dictionary with document counts, size, and performance
    """
    logger.info("Generating collection statistics...")

    # Get all documents to analyze
    all_docs = collection.get()

    # Count documents by type
    type_counts: Dict[str, int] = {}
    for metadata in all_docs["metadatas"]:
        doc_type = metadata.get("type", "unknown")
        type_counts[doc_type] = type_counts.get(doc_type, 0) + 1

    # Calculate collection size on disk
    chromadb_path = Path(CHROMADB_PATH)
    total_size = sum(f.stat().st_size for f in chromadb_path.rglob("*") if f.is_file())
    size_mb = total_size / (1024 * 1024)

    # Measure query performance with test queries
    test_queries = [
        "brave warrior princess",
        "magical spirit world",
        "environmental themes",
        "flying castle",
        "forest spirit",
        "princess transformation",
        "coal dust creatures",
        "bathhouse adventure",
        "moving castle magic",
        "nature vs technology",
    ]

    query_times = []
    for query in test_queries[:NUM_TEST_QUERIES]:
        start_time = time.time()
        collection.query(query_texts=[query], n_results=5)
        query_time = time.time() - start_time
        query_times.append(query_time)

    avg_query_time_ms = (sum(query_times) / len(query_times)) * 1000

    generated_at_str = (
        datetime.now(datetime.UTC).isoformat()
        if hasattr(datetime, 'UTC')
        else datetime.utcnow().isoformat()
    )
    stats = {
        "total_documents": len(all_docs["ids"]),
        "documents_by_type": type_counts,
        "collection_size_mb": round(size_mb, 2),
        "average_query_time_ms": round(avg_query_time_ms, 2),
        "num_test_queries": NUM_TEST_QUERIES,
        "generated_at": generated_at_str,
    }

    logger.info(f"Total documents: {stats['total_documents']}")
    logger.info(f"Documents by type: {stats['documents_by_type']}")
    logger.info(f"Collection size: {stats['collection_size_mb']} MB")
    logger.info(f"Average query time: {stats['average_query_time_ms']} ms")

    # Save stats to file
    with open(STATS_OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(stats, f, indent=2)

    logger.info(f"Saved statistics to {STATS_OUTPUT_PATH}")

    return stats


def validate_documents(
    collection: chromadb.Collection, corpus: List[Dict[str, Any]]
) -> None:
    """
    Validate document retrieval by comparing with original corpus.

    Args:
        collection: ChromaDB collection
        corpus: Original corpus documents

    Raises:
        AssertionError: If retrieved documents don't match original corpus
    """
    logger.info(f"Validating {NUM_VALIDATION_DOCS} random documents...")

    # Select random documents
    random_docs = random.sample(corpus, min(NUM_VALIDATION_DOCS, len(corpus)))

    for doc in random_docs:
        doc_id = doc["doc_id"]
        original_text = doc["text"]

        # Retrieve from collection
        result = collection.get(ids=[doc_id])

        if not result["ids"]:
            raise AssertionError(f"Document {doc_id} not found in collection")

        retrieved_text = result["documents"][0]

        # Compare text
        if retrieved_text != original_text:
            logger.error(f"Text mismatch for document {doc_id}")
            logger.error(f"Expected: {original_text[:100]}...")
            logger.error(f"Retrieved: {retrieved_text[:100]}...")
            raise AssertionError(f"Text mismatch for document {doc_id}")

        logger.debug(f"✓ Document {doc_id} validated successfully")

    logger.info(f"✓ All {NUM_VALIDATION_DOCS} documents validated successfully")


def main() -> None:
    """
    Main orchestration function for building ChromaDB vector store.

    Executes the following steps:
    1. Initialize ChromaDB client
    2. Create/recreate collection
    3. Load embeddings and corpus
    4. Add documents in batches
    5. Run similarity search validation
    6. Generate statistics
    7. Validate random documents
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    logger.info("=" * 60)
    logger.info("Building ChromaDB Vector Store (Story 4.3)")
    logger.info("=" * 60)

    try:
        # Step 1: Initialize ChromaDB client
        client = get_chromadb_client()

        # Step 2: Create/recreate collection
        collection = create_ghibli_collection(client)

        # Step 3: Load embeddings and corpus
        embeddings = load_embeddings()
        corpus = load_embedding_corpus()

        # Step 4: Add documents in batches
        add_documents_to_collection(collection, embeddings, corpus)

        # Step 5: Run similarity search validation
        validate_similarity_search(collection)

        # Step 6: Generate statistics
        stats = generate_collection_stats(collection)

        # Step 7: Validate random documents
        validate_documents(collection, corpus)

        logger.info("=" * 60)
        logger.info(
            f"✓ Vector store built successfully with {stats['total_documents']} documents"
        )
        logger.info("=" * 60)

    except Exception as e:
        logger.error(f"Failed to build vector store: {e}")
        raise


if __name__ == "__main__":
    main()
