"""
Vector embedding generation using OpenAI text-embedding-3-small model.

This module loads the embedding corpus prepared in Story 4.1, generates embeddings
via OpenAI API with batch processing and retry logic, and saves embeddings as
numpy arrays for downstream RAG retrieval tasks.
"""

import argparse
import json
import logging
import os
import time
from pathlib import Path
from typing import Any, Dict, List, Tuple

import numpy as np
import tiktoken
from dotenv import load_dotenv
from openai import APIConnectionError, APIError, OpenAI, RateLimitError

# Constants
EMBEDDING_MODEL = "text-embedding-3-small"
EMBEDDING_DIMENSIONS = 1536
BATCH_SIZE = 100
MAX_RETRIES = 5
COST_PER_MILLION_TOKENS = 0.02

# Logger
logger = logging.getLogger(__name__)


def load_embedding_corpus(corpus_path: str) -> List[Dict[str, Any]]:
    """
    Load embedding corpus from JSON file.

    Args:
        corpus_path: Path to embedding_corpus.json file

    Returns:
        List of document dictionaries with doc_id, type, name, text, metadata

    Raises:
        FileNotFoundError: If corpus file does not exist
        ValueError: If corpus JSON is malformed or missing required fields

    Example:
        >>> documents = load_embedding_corpus("data/processed/embedding_corpus.json")
        >>> len(documents)
        5411
        >>> documents[0]["type"]
        'film'
    """
    corpus_path_obj = Path(corpus_path)

    if not corpus_path_obj.exists():
        raise FileNotFoundError(f"Embedding corpus not found at {corpus_path}")

    logger.info(f"Loading embedding corpus from {corpus_path}...")

    with open(corpus_path_obj, "r", encoding="utf-8") as f:
        documents = json.load(f)

    # Validate corpus structure
    required_fields = {"doc_id", "type", "name", "text", "metadata"}
    for i, doc in enumerate(documents):
        missing_fields = required_fields - set(doc.keys())
        if missing_fields:
            raise ValueError(
                f"Document {i} missing required fields: {missing_fields}"
            )

    # Log corpus statistics
    type_counts: Dict[str, int] = {}
    for doc in documents:
        doc_type = doc["type"]
        type_counts[doc_type] = type_counts.get(doc_type, 0) + 1

    logger.info(f"Loaded {len(documents)} documents")
    logger.info(f"Document distribution: {type_counts}")

    return documents  # type: ignore[no-any-return]


def estimate_tokens(text: str, model: str = EMBEDDING_MODEL) -> int:
    """
    Estimate token count for text using tiktoken.

    Args:
        text: Input text to tokenize
        model: OpenAI model name for encoding (default: text-embedding-3-small)

    Returns:
        Estimated token count

    Example:
        >>> estimate_tokens("Spirited Away is a 2001 film.")
        8
    """
    enc = tiktoken.encoding_for_model(model)
    return len(enc.encode(text))


def estimate_embedding_cost(documents: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Estimate embedding generation cost before processing.

    Args:
        documents: List of document dictionaries

    Returns:
        Dictionary with total_documents, total_tokens, estimated_cost_usd,
        budget_usd, under_budget

    Example:
        >>> docs = [{"text": "Test doc"}]
        >>> cost_info = estimate_embedding_cost(docs)
        >>> cost_info["under_budget"]
        True
    """
    logger.info("Estimating embedding generation cost...")

    total_tokens = sum(estimate_tokens(doc["text"]) for doc in documents)
    estimated_cost = (total_tokens / 1_000_000) * COST_PER_MILLION_TOKENS

    cost_info = {
        "total_documents": len(documents),
        "total_tokens": total_tokens,
        "estimated_cost_usd": estimated_cost,
        "budget_usd": 2.0,
        "under_budget": estimated_cost < 2.0,
    }

    logger.info(f"Total documents: {cost_info['total_documents']}")
    logger.info(f"Estimated tokens: {cost_info['total_tokens']:,}")
    logger.info(f"Estimated cost: ${cost_info['estimated_cost_usd']:.4f}")
    logger.info(f"Budget: ${cost_info['budget_usd']:.2f}")

    if not cost_info["under_budget"]:
        logger.warning(
            f"⚠️  Estimated cost ${estimated_cost:.4f} exceeds budget $2.00!"
        )

    return cost_info


def create_batches(
    documents: List[Dict[str, Any]], batch_size: int = BATCH_SIZE
) -> List[List[Dict[str, Any]]]:
    """
    Split documents into batches for API processing.

    Args:
        documents: List of document dictionaries
        batch_size: Number of documents per batch (default: 100)

    Returns:
        List of document batches

    Example:
        >>> docs = [{"text": f"doc {i}"} for i in range(250)]
        >>> batches = create_batches(docs, batch_size=100)
        >>> len(batches)
        3
        >>> len(batches[0])
        100
    """
    batches = []
    for i in range(0, len(documents), batch_size):
        batch = documents[i : i + batch_size]
        batches.append(batch)

    logger.info(f"Created {len(batches)} batches (batch_size={batch_size})")

    return batches


def generate_embeddings_batch(
    client: OpenAI, documents: List[Dict[str, Any]]
) -> Tuple[List[np.ndarray], int]:
    """
    Generate embeddings for a batch of documents with retry logic.

    Args:
        client: OpenAI client instance
        documents: List of document dictionaries

    Returns:
        Tuple of (list of embeddings as numpy arrays, total tokens used)

    Raises:
        RateLimitError: If rate limit exceeded after max retries
        APIError: If API error persists after max retries
        APIConnectionError: If connection error persists after max retries

    Example:
        >>> client = OpenAI(api_key="sk-...")
        >>> docs = [{"text": "Test"}]
        >>> embeddings, tokens = generate_embeddings_batch(client, docs)
        >>> len(embeddings)
        1
        >>> embeddings[0].shape
        (1536,)
    """
    texts = [doc["text"] for doc in documents]
    backoff_delay = 1

    for attempt in range(MAX_RETRIES):
        try:
            response = client.embeddings.create(model=EMBEDDING_MODEL, input=texts)

            embeddings = [
                np.array(item.embedding, dtype=np.float32) for item in response.data
            ]

            tokens_used = response.usage.total_tokens

            logger.debug(
                f"Generated {len(embeddings)} embeddings "
                f"({tokens_used} tokens, attempt {attempt + 1})"
            )

            return embeddings, tokens_used

        except RateLimitError as e:
            if attempt < MAX_RETRIES - 1:
                logger.warning(
                    f"Rate limit hit, retrying in {backoff_delay}s "
                    f"(attempt {attempt + 1}/{MAX_RETRIES}): {e}"
                )
                time.sleep(backoff_delay)
                backoff_delay *= 2  # Exponential backoff
            else:
                logger.error(f"Rate limit exceeded after {MAX_RETRIES} attempts")
                raise

        except APIError as e:
            if attempt < MAX_RETRIES - 1:
                logger.warning(
                    f"API error, retrying in {backoff_delay}s "
                    f"(attempt {attempt + 1}/{MAX_RETRIES}): {e}"
                )
                time.sleep(backoff_delay)
                backoff_delay *= 2
            else:
                logger.error(f"API error after {MAX_RETRIES} attempts: {e}")
                raise

        except APIConnectionError as e:
            if attempt < MAX_RETRIES - 1:
                logger.warning(
                    f"Connection error, retrying in {backoff_delay}s "
                    f"(attempt {attempt + 1}/{MAX_RETRIES}): {e}"
                )
                time.sleep(backoff_delay)
                backoff_delay *= 2
            else:
                logger.error(f"Connection error after {MAX_RETRIES} attempts: {e}")
                raise

        except Exception as e:
            logger.error(f"Unexpected error generating embeddings: {e}")
            raise

    raise RuntimeError("Failed to generate embeddings after all retries")


def validate_embeddings(
    embeddings: np.ndarray, doc_ids: List[str], documents: List[Dict[str, Any]]
) -> Dict[str, Any]:
    """
    Validate generated embeddings.

    Args:
        embeddings: Numpy array of embeddings (N x 1536)
        doc_ids: List of document IDs
        documents: Original document list

    Returns:
        Validation report dictionary

    Raises:
        AssertionError: If validation checks fail

    Example:
        >>> embeddings = np.random.rand(100, 1536).astype(np.float32)
        >>> doc_ids = [f"doc_{i}" for i in range(100)]
        >>> docs = [{"doc_id": doc_ids[i], "type": "film"} for i in range(100)]
        >>> report = validate_embeddings(embeddings, doc_ids, docs)
        >>> report["shape_valid"]
        True
    """
    logger.info("Validating embeddings...")

    # Shape check
    expected_shape = (len(documents), EMBEDDING_DIMENSIONS)
    assert (
        embeddings.shape == expected_shape
    ), f"Expected shape {expected_shape}, got {embeddings.shape}"
    logger.info(f"✓ Shape valid: {embeddings.shape}")

    # Count check
    assert len(embeddings) == len(
        doc_ids
    ), f"Embedding count {len(embeddings)} != document count {len(doc_ids)}"
    logger.info(f"✓ Count matches: {len(embeddings)} embeddings")

    # NaN check
    assert not np.isnan(embeddings).any(), "Embeddings contain NaN values"
    logger.info("✓ No NaN values detected")

    # Spot-check similarity between related documents
    # Find two film documents
    film_indices = [i for i, doc in enumerate(documents) if doc["type"] == "film"]

    if len(film_indices) >= 2:
        from sklearn.metrics.pairwise import cosine_similarity

        idx1, idx2 = film_indices[0], film_indices[1]
        sim = cosine_similarity([embeddings[idx1]], [embeddings[idx2]])[0][0]
        logger.info(
            f"✓ Spot-check similarity between films "
            f"'{documents[idx1]['name']}' and '{documents[idx2]['name']}': "
            f"{sim:.4f}"
        )

    return {
        "shape_valid": True,
        "count_valid": True,
        "nan_check_passed": True,
        "document_count": len(embeddings),
        "embedding_dimensions": embeddings.shape[1],
    }


def log_processing_summary(stats: Dict[str, Any]) -> None:
    """
    Log processing summary with cost and performance metrics.

    Args:
        stats: Dictionary with processing statistics

    Example:
        >>> stats = {
        ...     "total_documents": 5411,
        ...     "total_tokens": 541000,
        ...     "actual_cost_usd": 0.01,
        ...     "processing_time_seconds": 120.5,
        ...     "budget_usd": 2.0
        ... }
        >>> log_processing_summary(stats)
    """
    logger.info("=" * 60)
    logger.info("EMBEDDING GENERATION SUMMARY")
    logger.info("=" * 60)
    logger.info(f"Documents processed: {stats['total_documents']}")
    logger.info(f"Total tokens used: {stats['total_tokens']:,}")
    logger.info(f"Actual cost: ${stats['actual_cost_usd']:.4f}")
    logger.info(f"Processing time: {stats['processing_time_seconds']:.2f}s")
    logger.info(
        f"Avg time per document: "
        f"{stats['processing_time_seconds'] / stats['total_documents']:.3f}s"
    )
    logger.info(f"Budget: ${stats['budget_usd']:.2f}")

    if stats["actual_cost_usd"] > stats["budget_usd"]:
        logger.warning(
            f"⚠️  COST EXCEEDED BUDGET: "
            f"${stats['actual_cost_usd']:.4f} > ${stats['budget_usd']:.2f}"
        )
    else:
        logger.info(
            f"✓ Cost under budget "
            f"(${stats['actual_cost_usd']:.4f} / ${stats['budget_usd']:.2f})"
        )

    logger.info("=" * 60)


def generate_all_embeddings(
    corpus_path: str, output_dir: str, batch_size: int = BATCH_SIZE, dry_run: bool = False
) -> Tuple[np.ndarray, List[str], Dict[str, Any]]:
    """
    Generate embeddings for all documents in corpus.

    Args:
        corpus_path: Path to embedding corpus JSON
        output_dir: Directory to save embeddings and IDs
        batch_size: Batch size for API processing (default: 100)
        dry_run: If True, estimate cost without calling API

    Returns:
        Tuple of (embeddings array, document IDs list, processing stats)

    Example:
        >>> embeddings, doc_ids, stats = generate_all_embeddings(
        ...     corpus_path="data/processed/embedding_corpus.json",
        ...     output_dir="data/processed/",
        ...     batch_size=100
        ... )
        >>> embeddings.shape
        (5411, 1536)
    """
    # Load corpus
    documents = load_embedding_corpus(corpus_path)

    # Estimate cost
    cost_estimate = estimate_embedding_cost(documents)

    if dry_run:
        logger.info("DRY RUN mode - stopping before API calls")
        return np.array([]), [], cost_estimate

    # Initialize OpenAI client
    load_dotenv()
    api_key = os.getenv("OPENAI_API_KEY")

    if not api_key:
        raise ValueError("OPENAI_API_KEY not found in environment variables")

    client = OpenAI(api_key=api_key)

    # Create batches
    batches = create_batches(documents, batch_size=batch_size)

    # Process batches
    logger.info(f"Processing {len(batches)} batches...")
    start_time = time.time()

    all_embeddings = []
    total_tokens_used = 0

    for i, batch in enumerate(batches):
        logger.info(f"Processing batch {i + 1}/{len(batches)} ({len(batch)} documents)...")

        batch_embeddings, batch_tokens = generate_embeddings_batch(client, batch)
        all_embeddings.extend(batch_embeddings)
        total_tokens_used += batch_tokens

        logger.info(
            f"Batch {i + 1}/{len(batches)} complete "
            f"({batch_tokens} tokens, {len(all_embeddings)} total embeddings)"
        )

    end_time = time.time()
    processing_time = end_time - start_time

    # Combine embeddings
    embeddings_array = np.vstack(all_embeddings)

    # Extract document IDs
    doc_ids = [doc["doc_id"] for doc in documents]

    # Save embeddings
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    embeddings_file = output_path / "embeddings.npy"
    doc_ids_file = output_path / "embedding_ids.json"

    np.save(embeddings_file, embeddings_array)
    logger.info(f"Saved embeddings to {embeddings_file}")

    with open(doc_ids_file, "w", encoding="utf-8") as f:
        json.dump(doc_ids, f, indent=2)
    logger.info(f"Saved document IDs to {doc_ids_file}")

    # Calculate actual cost
    actual_cost = (total_tokens_used / 1_000_000) * COST_PER_MILLION_TOKENS

    # Processing stats
    stats = {
        "total_documents": len(documents),
        "total_tokens": total_tokens_used,
        "actual_cost_usd": actual_cost,
        "processing_time_seconds": processing_time,
        "budget_usd": 2.0,
    }

    # Validate embeddings
    validate_embeddings(embeddings_array, doc_ids, documents)

    # Log summary
    log_processing_summary(stats)

    return embeddings_array, doc_ids, stats


def main() -> None:
    """Main entry point for embedding generation script."""
    parser = argparse.ArgumentParser(
        description="Generate vector embeddings using OpenAI text-embedding-3-small"
    )
    parser.add_argument(
        "--corpus",
        type=str,
        default="data/processed/embedding_corpus.json",
        help="Path to embedding corpus JSON file",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default="data/processed/",
        help="Directory to save embeddings",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=BATCH_SIZE,
        help=f"Batch size for API processing (default: {BATCH_SIZE})",
    )
    parser.add_argument(
        "--verbose", action="store_true", help="Enable debug-level logging"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Estimate cost without calling OpenAI API",
    )

    args = parser.parse_args()

    # Configure logging
    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    # Generate embeddings
    try:
        generate_all_embeddings(
            corpus_path=args.corpus,
            output_dir=args.output_dir,
            batch_size=args.batch_size,
            dry_run=args.dry_run,
        )

        logger.info("✓ Embedding generation complete!")

    except Exception as e:
        logger.error(f"✗ Embedding generation failed: {e}")
        raise


if __name__ == "__main__":
    main()
