"""Setup configuration for ghibli_pipeline package."""
from setuptools import setup, find_packages

setup(
    name="ghibli_pipeline",
    version="0.1.0",
    description="Studio Ghibli Knowledge Graph & RAG System",
    author="Your Name",
    python_requires=">=3.9",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    install_requires=[
        "pandas>=2.2.0",
        "duckdb>=0.10.0",
        "dbt-core>=1.7.0",
        "dbt-duckdb>=1.7.0",
        "streamlit>=1.31.0",
        "langchain>=0.1.0",
        "openai>=1.12.0",
    ],
    extras_require={
        "dev": [
            "pytest>=7.4.3",
            "pytest-cov>=4.1.0",
            "black>=24.0.0",
            "flake8>=7.0.0",
            "mypy>=1.8.0",
        ]
    },
)
