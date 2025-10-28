"""
Custom exceptions for the SpiritedData project.

This module defines project-specific exceptions for better error handling
and debugging across different components of the data pipeline.
"""


class SpiritedDataError(Exception):
    """Base exception for all SpiritedData errors."""

    pass


class DataIngestionError(SpiritedDataError):
    """Raised when data fetching or parsing fails."""

    pass


class DataValidationError(SpiritedDataError):
    """Raised when data validation fails."""

    pass


class DatabaseError(SpiritedDataError):
    """Raised when database operations fail."""

    pass


class RAGError(SpiritedDataError):
    """Raised when RAG system fails (embeddings, retrieval, LLM)."""

    pass


class RateLimitError(SpiritedDataError):
    """Raised when API rate limit exceeded."""

    pass
