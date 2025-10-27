"""Configuration module for loading environment variables."""
import os
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Database configuration
DUCKDB_PATH = os.getenv("DUCKDB_PATH", "data/ghibli.duckdb")

# Vector store configuration
CHROMADB_PATH = os.getenv("CHROMADB_PATH", "data/vectors")

# Logging configuration
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

# API Keys
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
TMDB_API_KEY = os.getenv("TMDB_API_KEY", "")
