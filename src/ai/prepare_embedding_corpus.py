"""
Text corpus preparation for embedding generation.

This module extracts text from DuckDB tables (films, characters, locations, species)
and memorable dialogue quotes from emotion analysis results, then structures them
into embeddable documents for OpenAI's text-embedding-3-small model.
"""

import argparse
import json
import logging
from collections import Counter
from pathlib import Path
from typing import Any, Dict, List

import duckdb
import tiktoken

from src.shared.database import get_duckdb_connection

# Constants
MAX_TOKENS_PER_CHUNK = 300
FILM_DESCRIPTION_MAX_TOKENS = 500
QUOTES_PER_FILM = 50
TOKEN_OVERLAP = 50
EMBEDDING_MODEL = "text-embedding-3-small"

# Logger
logger = logging.getLogger(__name__)


def estimate_tokens(text: str, model: str = EMBEDDING_MODEL) -> int:
    """
    Estimate token count for text using tiktoken.

    Args:
        text: Text to count tokens for
        model: OpenAI model name for encoding (default: text-embedding-3-small)

    Returns:
        Integer token count

    Example:
        >>> estimate_tokens("Hello world")
        2
    """
    enc = tiktoken.encoding_for_model(model)
    return len(enc.encode(text))


def chunk_text(
    text: str, max_tokens: int = MAX_TOKENS_PER_CHUNK, overlap: int = TOKEN_OVERLAP
) -> List[str]:
    """
    Split text into chunks with token overlap for context continuity.

    Args:
        text: Text to chunk
        max_tokens: Maximum tokens per chunk (default: 300)
        overlap: Token overlap between chunks (default: 50)

    Returns:
        List of text chunks

    Example:
        >>> chunks = chunk_text("Long text...", max_tokens=100)
        >>> all(estimate_tokens(c) <= 100 for c in chunks)
        True
    """
    enc = tiktoken.encoding_for_model(EMBEDDING_MODEL)
    tokens = enc.encode(text)

    if len(tokens) <= max_tokens:
        return [text]

    chunks = []
    start = 0

    while start < len(tokens):
        end = start + max_tokens
        chunk_tokens = tokens[start:end]
        chunk_text = enc.decode(chunk_tokens)
        chunks.append(chunk_text)
        start = end - overlap  # Overlap for context

    return chunks


def extract_film_documents(conn: duckdb.DuckDBPyConnection) -> List[Dict[str, Any]]:
    """
    Extract film documents from DuckDB.

    Queries main_staging.stg_films table and creates embeddable documents from
    film descriptions. Generates synthetic descriptions for films missing them.

    Args:
        conn: Active DuckDB connection

    Returns:
        List of film documents with structure:
        {doc_id, type, name, text, metadata}

    Example:
        >>> docs = extract_film_documents(conn)
        >>> docs[0]["type"]
        'film'
    """
    query = """
        SELECT id, title, description, director, release_year, rt_score, running_time
        FROM main_staging.stg_films
        WHERE title IS NOT NULL
    """

    try:
        results = conn.execute(query).fetchall()
        logger.info(f"Found {len(results)} films in database")

        documents = []
        for row in results:
            film_id, title, description, director, release_year, rt_score, running_time = row

            # Generate description if missing
            if not description or description.strip() == "":
                description = (
                    f"{title} is a {release_year} film directed by {director}."
                )
                logger.debug(f"Generated synthetic description for: {title}")

            doc = {
                "doc_id": f"film_{film_id}",
                "type": "film",
                "name": title,
                "text": description,
                "metadata": {
                    "director": director,
                    "release_year": int(release_year) if release_year else None,
                    "rt_score": int(rt_score) if rt_score else None,
                    "running_time": int(running_time) if running_time else None,
                },
            }
            documents.append(doc)

        logger.info(f"Extracted {len(documents)} film documents")
        return documents

    except Exception as e:
        logger.error(f"Failed to extract film documents: {e}")
        return []


def extract_character_documents(conn: duckdb.DuckDBPyConnection) -> List[Dict[str, Any]]:
    """
    Extract character documents from DuckDB with chunking support.

    Queries main_staging.stg_people table and generates descriptions from
    structured character data. Splits long bios into chunks with overlap.

    Args:
        conn: Active DuckDB connection

    Returns:
        List of character documents (may include chunks)

    Example:
        >>> docs = extract_character_documents(conn)
        >>> all(d["type"] == "character" for d in docs)
        True
    """
    query = """
        SELECT id, name, gender, age, eye_color, hair_color, species_id
        FROM main_staging.stg_people
        WHERE name IS NOT NULL
    """

    try:
        results = conn.execute(query).fetchall()
        logger.info(f"Found {len(results)} characters in database")

        documents = []
        chunk_count = 0

        for row in results:
            char_id, name, gender, age, eye_color, hair_color, species = row

            # Generate bio from structured fields
            bio_parts = [f"{name} is a character from Studio Ghibli films."]

            if gender:
                bio_parts.append(f"Gender: {gender}.")
            if age:
                bio_parts.append(f"Age: {age}.")
            if species:
                bio_parts.append(f"Species: {species}.")
            if eye_color:
                bio_parts.append(f"Eye color: {eye_color}.")
            if hair_color:
                bio_parts.append(f"Hair color: {hair_color}.")

            bio_text = " ".join(bio_parts)

            # Check if chunking is needed
            token_count = estimate_tokens(bio_text)

            if token_count > MAX_TOKENS_PER_CHUNK:
                # Split into chunks
                chunks = chunk_text(bio_text, MAX_TOKENS_PER_CHUNK, TOKEN_OVERLAP)
                logger.debug(f"Chunked {name} into {len(chunks)} parts")

                for chunk_num, chunk in enumerate(chunks, 1):
                    doc = {
                        "doc_id": f"character_{char_id}_chunk{chunk_num}",
                        "type": "character",
                        "name": f"{name} (part {chunk_num})",
                        "text": chunk,
                        "metadata": {
                            "gender": gender,
                            "age": age,
                            "species": species,
                            "chunk_num": chunk_num,
                            "total_chunks": len(chunks),
                        },
                    }
                    documents.append(doc)
                    chunk_count += 1
            else:
                # Single document
                doc = {
                    "doc_id": f"character_{char_id}",
                    "type": "character",
                    "name": name,
                    "text": bio_text,
                    "metadata": {
                        "gender": gender,
                        "age": age,
                        "species": species,
                    },
                }
                documents.append(doc)

        logger.info(
            f"Extracted {len(documents)} character documents ({chunk_count} chunks)"
        )
        return documents

    except Exception as e:
        logger.error(f"Failed to extract character documents: {e}")
        return []


def extract_location_documents(conn: duckdb.DuckDBPyConnection) -> List[Dict[str, Any]]:
    """
    Extract location documents from DuckDB.

    Generates descriptions from structured location data (climate, terrain, etc.).

    Args:
        conn: Active DuckDB connection

    Returns:
        List of location documents

    Example:
        >>> docs = extract_location_documents(conn)
        >>> docs[0]["type"]
        'location'
    """
    query = """
        SELECT id, name, climate, terrain, surface_water_pct
        FROM main_staging.stg_locations
        WHERE name IS NOT NULL
    """

    try:
        results = conn.execute(query).fetchall()
        logger.info(f"Found {len(results)} locations in database")

        documents = []
        for row in results:
            loc_id, name, climate, terrain, surface_water_pct = row

            # Generate description from structured fields
            desc_parts = [f"{name} is a location in Studio Ghibli films."]

            if climate:
                desc_parts.append(f"Climate: {climate}.")
            if terrain:
                desc_parts.append(f"Terrain: {terrain}.")
            if surface_water_pct:
                desc_parts.append(f"Surface water: {surface_water_pct}%.")

            description = " ".join(desc_parts)

            doc = {
                "doc_id": f"location_{loc_id}",
                "type": "location",
                "name": name,
                "text": description,
                "metadata": {
                    "climate": climate,
                    "terrain": terrain,
                    "surface_water_pct": surface_water_pct,
                },
            }
            documents.append(doc)

        logger.info(f"Extracted {len(documents)} location documents")
        return documents

    except Exception as e:
        logger.error(f"Failed to extract location documents: {e}")
        return []


def extract_species_documents(conn: duckdb.DuckDBPyConnection) -> List[Dict[str, Any]]:
    """
    Extract species documents from DuckDB.

    Generates descriptions from structured species data.

    Args:
        conn: Active DuckDB connection

    Returns:
        List of species documents

    Example:
        >>> docs = extract_species_documents(conn)
        >>> docs[0]["type"]
        'species'
    """
    query = """
        SELECT id, name, classification, eye_colors, hair_colors
        FROM main_staging.stg_species
        WHERE name IS NOT NULL
    """

    try:
        results = conn.execute(query).fetchall()
        logger.info(f"Found {len(results)} species in database")

        documents = []
        for row in results:
            spec_id, name, classification, eye_colors, hair_colors = row

            # Generate description from structured fields
            desc_parts = [f"{name} is a species in Studio Ghibli films."]

            if classification:
                desc_parts.append(f"Classification: {classification}.")
            if eye_colors:
                desc_parts.append(f"Common eye colors: {eye_colors}.")
            if hair_colors:
                desc_parts.append(f"Common hair colors: {hair_colors}.")

            description = " ".join(desc_parts)

            doc = {
                "doc_id": f"species_{spec_id}",
                "type": "species",
                "name": name,
                "text": description,
                "metadata": {
                    "classification": classification,
                    "eye_colors": eye_colors,
                    "hair_colors": hair_colors,
                },
            }
            documents.append(doc)

        logger.info(f"Extracted {len(documents)} species documents")
        return documents

    except Exception as e:
        logger.error(f"Failed to extract species documents: {e}")
        return []


def extract_memorable_quotes(
    conn: duckdb.DuckDBPyConnection, max_quotes_per_film: int = QUOTES_PER_FILM
) -> List[Dict[str, Any]]:
    """
    Extract memorable dialogue quotes from emotion analysis peaks.

    Identifies top emotional moments per film and extracts corresponding
    dialogue from parsed subtitle files.

    Args:
        conn: Active DuckDB connection
        max_quotes_per_film: Maximum quotes to extract per film (default: 50)

    Returns:
        List of quote documents with emotion metadata

    Example:
        >>> docs = extract_memorable_quotes(conn)
        >>> docs[0]["type"]
        'quote'
    """
    # Query top emotional peaks per film
    # Calculate emotional intensity as sum of joy, fear, anger, love, sadness
    query = f"""
        WITH ranked_emotions AS (
            SELECT
                film_id,
                film_slug,
                language_code,
                minute_offset,
                dialogue_count,
                emotion_joy,
                emotion_fear,
                emotion_anger,
                emotion_love,
                emotion_sadness,
                (emotion_joy + emotion_fear + emotion_anger +
                 emotion_love + emotion_sadness) as emotional_intensity,
                ROW_NUMBER() OVER (
                    PARTITION BY film_id, language_code
                    ORDER BY (emotion_joy + emotion_fear + emotion_anger +
                              emotion_love + emotion_sadness) DESC
                ) as rank
            FROM raw.film_emotions
            WHERE dialogue_count > 0
        )
        SELECT
            film_id,
            film_slug,
            language_code,
            minute_offset,
            emotion_joy,
            emotion_fear,
            emotion_anger,
            emotion_love,
            emotion_sadness,
            emotional_intensity
        FROM ranked_emotions
        WHERE rank <= {max_quotes_per_film}
        ORDER BY film_id, language_code, emotional_intensity DESC
    """

    try:
        results = conn.execute(query).fetchall()
        logger.info(f"Found {len(results)} emotional peaks across all films")

        documents = []
        film_count = set()

        for row in results:
            (
                film_id,
                film_slug,
                language_code,
                minute_offset,
                emotion_joy,
                emotion_fear,
                emotion_anger,
                emotion_love,
                emotion_sadness,
                emotional_intensity,
            ) = row

            # Load corresponding subtitle file
            # Note: film_slug already contains language code (e.g., "spirited_away_en")
            subtitle_path = Path(
                f"data/processed/subtitles/{film_slug}_parsed.json"
            )

            if not subtitle_path.exists():
                logger.warning(f"Subtitle file not found: {subtitle_path}")
                continue

            try:
                with open(subtitle_path) as f:
                    subtitle_data = json.load(f)

                # Extract dialogue from the minute bucket
                minute_start = minute_offset * 60  # Convert to seconds
                minute_end = minute_start + 60

                relevant_dialogues = [
                    sub["dialogue_text"]
                    for sub in subtitle_data.get("subtitles", [])
                    if minute_start <= sub["start_time"] < minute_end
                ]

                if not relevant_dialogues:
                    logger.debug(
                        f"No dialogue found for {film_slug} at minute {minute_offset}"
                    )
                    continue

                # Concatenate dialogue (max 500 chars)
                dialogue_text = " ".join(relevant_dialogues)
                if len(dialogue_text) > 500:
                    dialogue_text = dialogue_text[:497] + "..."

                # Get film title from metadata
                film_title = subtitle_data.get("metadata", {}).get(
                    "film_name", film_slug.replace("_", " ").title()
                )

                doc = {
                    "doc_id": f"quote_{film_id}_{language_code}_{minute_offset}",
                    "type": "quote",
                    "name": f"{film_title} ({minute_offset}:00 - {language_code})",
                    "text": dialogue_text,
                    "metadata": {
                        "film_id": film_id,
                        "film_slug": film_slug,
                        "film_title": film_title,
                        "language": language_code,
                        "minute_offset": minute_offset,
                        "emotion_joy": float(emotion_joy) if emotion_joy else 0.0,
                        "emotion_fear": float(emotion_fear) if emotion_fear else 0.0,
                        "emotion_anger": float(emotion_anger) if emotion_anger else 0.0,
                        "emotion_love": float(emotion_love) if emotion_love else 0.0,
                        "emotion_sadness": float(emotion_sadness)
                        if emotion_sadness
                        else 0.0,
                        "emotional_intensity": float(emotional_intensity),
                    },
                }
                documents.append(doc)
                film_count.add(film_id)

            except Exception as e:
                logger.error(f"Failed to process quote from {subtitle_path}: {e}")
                continue

        logger.info(
            f"Extracted {len(documents)} memorable quotes from {len(film_count)} films"
        )
        return documents

    except Exception as e:
        logger.error(f"Failed to query emotion peaks: {e}")
        return []


def validate_corpus(documents: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Validate corpus and generate statistics.

    Checks document structure, calculates distribution metrics, and
    validates expected ranges.

    Args:
        documents: List of all extracted documents

    Returns:
        Dictionary with validation statistics

    Example:
        >>> validation = validate_corpus(docs)
        >>> validation["total_documents"]
        150
    """
    total = len(documents)

    # Distribution by type
    type_distribution = Counter([doc["type"] for doc in documents])

    # Average text length per type
    avg_length_by_type = {}
    avg_tokens_by_type = {}

    for doc_type in type_distribution.keys():
        docs_of_type = [doc for doc in documents if doc["type"] == doc_type]
        text_lengths = [len(doc["text"]) for doc in docs_of_type]
        token_counts = [estimate_tokens(doc["text"]) for doc in docs_of_type]

        avg_length_by_type[doc_type] = (
            sum(text_lengths) / len(text_lengths) if text_lengths else 0
        )
        avg_tokens_by_type[doc_type] = (
            sum(token_counts) / len(token_counts) if token_counts else 0
        )

    # Check for empty text
    empty_count = len([d for d in documents if not d.get("text", "").strip()])

    # Check expected range (100-200 documents per story requirements)
    in_expected_range = 100 <= total <= 2000  # Adjusted for quotes

    validation = {
        "total_documents": total,
        "type_distribution": dict(type_distribution),
        "avg_length_by_type": avg_length_by_type,
        "avg_tokens_by_type": avg_tokens_by_type,
        "empty_text_count": empty_count,
        "in_expected_range": in_expected_range,
    }

    return validation


def save_corpus_to_json(documents: List[Dict[str, Any]], output_path: str) -> None:
    """
    Save corpus to JSON file.

    Creates output directory if needed and writes documents with formatting.

    Args:
        documents: List of all documents to save
        output_path: Path to output JSON file

    Raises:
        IOError: If file write fails

    Example:
        >>> save_corpus_to_json(docs, "data/processed/corpus.json")
    """
    output_file = Path(output_path)

    # Create directory if needed
    output_file.parent.mkdir(parents=True, exist_ok=True)

    try:
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(documents, f, indent=2, ensure_ascii=False)

        logger.info(f"Saved {len(documents)} documents to {output_path}")

    except Exception as e:
        logger.error(f"Failed to save corpus to {output_path}: {e}")
        raise


def main(
    output_path: str = "data/processed/embedding_corpus.json",
    max_quotes_per_film: int = QUOTES_PER_FILM,
    verbose: bool = False,
) -> None:
    """
    Main orchestration function for corpus preparation.

    Extracts all document types, validates corpus, and saves to JSON.

    Args:
        output_path: Path to save corpus JSON (default: data/processed/embedding_corpus.json)
        max_quotes_per_film: Maximum quotes per film (default: 50)
        verbose: Enable debug logging (default: False)
    """
    # Configure logging
    log_level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    logger.info("Starting embedding corpus preparation...")

    # Connect to DuckDB in read-only mode (only reading, not writing to DB)
    conn = get_duckdb_connection(read_only=True)

    try:
        # Extract all document types
        logger.info("Extracting film documents...")
        films = extract_film_documents(conn)

        logger.info("Extracting character documents...")
        characters = extract_character_documents(conn)

        logger.info("Extracting location documents...")
        locations = extract_location_documents(conn)

        logger.info("Extracting species documents...")
        species = extract_species_documents(conn)

        logger.info("Extracting memorable quotes...")
        quotes = extract_memorable_quotes(conn, max_quotes_per_film)

        # Combine all documents
        all_documents = films + characters + locations + species + quotes

        # Validate corpus
        logger.info("Validating corpus...")
        validation = validate_corpus(all_documents)

        # Print validation summary
        print("\n" + "=" * 60)
        print("CORPUS VALIDATION SUMMARY")
        print("=" * 60)
        print(f"Total Documents: {validation['total_documents']}")
        print("\nDistribution by Type:")
        for doc_type, count in validation["type_distribution"].items():
            print(f"  {doc_type:15} : {count:4} documents")

        print("\nAverage Text Length by Type:")
        for doc_type, avg_len in validation["avg_length_by_type"].items():
            print(f"  {doc_type:15} : {avg_len:7.1f} characters")

        print("\nAverage Token Count by Type:")
        for doc_type, avg_tokens in validation["avg_tokens_by_type"].items():
            print(f"  {doc_type:15} : {avg_tokens:7.1f} tokens")

        print(f"\nEmpty Documents: {validation['empty_text_count']}")
        print(
            f"In Expected Range: {'✓ Yes' if validation['in_expected_range'] else '✗ No'}"
        )
        print("=" * 60 + "\n")

        # Save corpus
        logger.info(f"Saving corpus to {output_path}...")
        save_corpus_to_json(all_documents, output_path)

        logger.info("Corpus preparation completed successfully!")

    except Exception as e:
        logger.error(f"Corpus preparation failed: {e}")
        raise

    finally:
        conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Prepare text corpus for embedding generation"
    )
    parser.add_argument(
        "--output",
        type=str,
        default="data/processed/embedding_corpus.json",
        help="Output path for corpus JSON (default: data/processed/embedding_corpus.json)",
    )
    parser.add_argument(
        "--max-quotes-per-film",
        type=int,
        default=QUOTES_PER_FILM,
        help=f"Maximum quotes per film (default: {QUOTES_PER_FILM})",
    )
    parser.add_argument(
        "--verbose", "-v", action="store_true", help="Enable debug logging"
    )

    args = parser.parse_args()

    main(
        output_path=args.output,
        max_quotes_per_film=args.max_quotes_per_film,
        verbose=args.verbose,
    )
