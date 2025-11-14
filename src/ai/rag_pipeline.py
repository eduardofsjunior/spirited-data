"""
LangChain RAG pipeline combining vector search with function calling.

This module implements a Retrieval-Augmented Generation (RAG) system that intelligently
answers natural language questions about Studio Ghibli films by combining:
- Vector search (ChromaDB) for general knowledge retrieval
- Custom function calling tools for data-specific queries (graph analysis, sentiment, metrics)
- GPT-4/GPT-3.5-turbo for natural language generation

Architecture:
    User Query → Vector Retrieval (ChromaDB) → LangChain Agent (LLM + Tools) → Response

The system uses 7 custom tools from graph_query_tools.py:
    - find_character_connections
    - get_film_sentiment
    - calculate_character_centrality
    - find_films_by_criteria
    - correlate_metrics
    - compare_sentiment_arcs_across_languages
    - query_graph_database

Usage Example:
    >>> pipeline = initialize_rag_pipeline()
    >>> result = query_rag_system(
    ...     user_question="Who are the most central characters in Spirited Away?",
    ...     chat_history=[]
    ... )
    >>> print(result["answer"])
    "Based on character centrality analysis, Chihiro is the most central character..."

Configuration:
    Environment variables:
    - OPENAI_API_KEY (required): OpenAI API key for LLM and embeddings
    - OPENAI_MODEL (optional): Model to use ("gpt-4" or "gpt-3.5-turbo", default: "gpt-3.5-turbo")
    - DUCKDB_PATH (optional): Path to DuckDB database (default: "data/ghibli.duckdb")
    - CHROMADB_PATH (optional): Path to ChromaDB storage (default: "data/vectors")
"""

import html
import logging
import os
import re
import time
import uuid
from datetime import datetime
from typing import Any, Dict, Iterator, List, Optional, Set

from langchain_community.vectorstores import Chroma
from langchain_core.messages import HumanMessage, SystemMessage
from langchain_core.prompts import ChatPromptTemplate, MessagesPlaceholder
from langchain_openai import ChatOpenAI, OpenAIEmbeddings
from langgraph.prebuilt import create_react_agent

try:
    import tiktoken
    TIKTOKEN_AVAILABLE = True
except ImportError:
    TIKTOKEN_AVAILABLE = False
    logger.warning(
        "tiktoken not available - token counting will use estimation. "
        "Install with: pip install tiktoken"
    )

from src.ai.graph_query_tools import (
    ToolResponse,
    compare_sentiment_arcs_across_languages,
    correlate_metrics,
    find_films_by_criteria,
    get_film_sentiment,
    query_graph_database,
)
from src.shared.database import get_duckdb_connection


# Sensitive Data Filter for logging
class SensitiveDataFilter(logging.Filter):
    """
    Logging filter that redacts sensitive information from log messages.

    Redacts:
    - OpenAI API keys (sk-...)
    - Environment variable patterns containing keys
    - Common API key patterns
    """

    PATTERNS = [
        (re.compile(r'sk-[a-zA-Z0-9]{48}'), '***REDACTED_API_KEY***'),
        (re.compile(r'(OPENAI_API_KEY[\s=:]+)[\S]+', re.IGNORECASE), r'\1***REDACTED***'),
        (re.compile(r'(api[_-]?key[\s=:"\'\]]+)[\S]+', re.IGNORECASE), r'\1***REDACTED***'),
        (re.compile(r'(openai[_-]?api[_-]?key[\s=:"\'\]]+)[\S]+', re.IGNORECASE), r'\1***REDACTED***'),
    ]

    def filter(self, record: logging.LogRecord) -> bool:
        """Sanitize log record message and args."""
        if record.msg:
            for pattern, replacement in self.PATTERNS:
                record.msg = pattern.sub(replacement, str(record.msg))

        if record.args:
            sanitized_args = []
            for arg in record.args if isinstance(record.args, tuple) else [record.args]:
                sanitized = str(arg)
                for pattern, replacement in self.PATTERNS:
                    sanitized = pattern.sub(replacement, sanitized)
                sanitized_args.append(sanitized)
            record.args = tuple(sanitized_args) if len(sanitized_args) > 1 else sanitized_args[0]

        return True


# Configure logging with sensitive data filter
logger = logging.getLogger("spiriteddata.ai.rag_pipeline")
logger.addFilter(SensitiveDataFilter())


def _count_tokens(text: str, model: str) -> int:
    """
    Count tokens accurately using tiktoken library.

    Args:
        text: Text to count tokens for
        model: Model name (e.g., "gpt-4", "gpt-3.5-turbo")

    Returns:
        Token count (or rough estimate if tiktoken not available)
    """
    if not text:
        return 0

    if TIKTOKEN_AVAILABLE:
        try:
            encoding = tiktoken.encoding_for_model(model)
            return len(encoding.encode(text))
        except Exception as e:
            logger.warning(f"tiktoken encoding failed: {e}, falling back to estimation")

    # Fallback: rough estimation (~4 characters per token)
    return len(text) // 4


# Constants
DEFAULT_MODEL = os.getenv("OPENAI_MODEL", "gpt-4-turbo-preview")
TEMPERATURE = 0.7  # Higher temperature for more creative, narrative responses
MAX_TOKENS = 800  # More tokens for verbose, storytelling style
TOP_K_DOCUMENTS = 5
SIMILARITY_THRESHOLD = 0.7
MAX_QUERY_LENGTH = 1000
MAX_AGENT_ITERATIONS = 5
CHROMADB_PATH = os.getenv("CHROMADB_PATH", "data/vectors")
EMBEDDING_MODEL = "text-embedding-3-small"
HARD_COST_LIMIT = float(os.getenv("HARD_COST_LIMIT", "20.0"))  # USD per session
MAX_QUERIES_PER_SESSION = int(os.getenv("MAX_QUERIES_PER_SESSION", "50"))
NARRATIVE_MODE = os.getenv("NARRATIVE_MODE", "true").lower() == "true"  # Portfolio-friendly by default

# OpenAI pricing (as of Jan 2025, USD per 1K tokens)
PRICING = {
    "gpt-4": {"input": 0.03, "output": 0.06},
    "gpt-3.5-turbo": {"input": 0.0015, "output": 0.002},
    "gpt-4-turbo": {"input": 0.01, "output": 0.03},  # Fallback pricing
}


# Custom exceptions
class CostLimitExceeded(Exception):
    """Raised when session cost limit is exceeded."""
    pass


class QueryLimitExceeded(Exception):
    """Raised when session query count limit is exceeded."""
    pass


class _CostTracker:
    """
    Session-level cost tracking for OpenAI API usage.

    Tracks cumulative costs and emits warnings at $5, $10, $15 thresholds.
    Enforces hard cost limit to prevent runaway costs.
    Singleton instance shared across all RAG pipeline invocations.
    """

    def __init__(self) -> None:
        self.session_total_cost: float = 0.0
        self.session_query_count: int = 0
        self.warnings_shown: Set[str] = set()
        logger.info(
            f"CostTracker initialized (session total: $0.00, "
            f"hard limit: ${HARD_COST_LIMIT:.2f}, "
            f"max queries: {MAX_QUERIES_PER_SESSION})"
        )

    def calculate_cost(self, input_tokens: int, output_tokens: int, model: str) -> float:
        """
        Calculate cost for a single query.

        Args:
            input_tokens: Number of input tokens
            output_tokens: Number of output tokens
            model: Model name (e.g., "gpt-4", "gpt-3.5-turbo")

        Returns:
            Cost in USD
        """
        # Normalize model name - check exact match first, then variations
        model_base = model
        if model not in PRICING:
            # Try common variations
            parts = model.split("-")
            if len(parts) >= 2:
                # Try "gpt-4", "gpt-3.5-turbo", etc.
                if len(parts) == 3 and parts[0] == "gpt" and parts[1] in ["3.5", "4"]:
                    # "gpt-3.5-turbo" or "gpt-4-turbo"
                    model_base = "-".join(parts[:3])  # Keep all three parts
                else:
                    # "gpt-4" or similar
                    model_base = "-".join(parts[:2])

            if model_base not in PRICING:
                # Use gpt-4-turbo pricing as fallback for unknown models
                logger.warning(f"Unknown model '{model}', using fallback pricing")
                model_base = "gpt-4-turbo"

        pricing = PRICING.get(model_base, PRICING["gpt-4-turbo"])
        input_cost = (input_tokens / 1000) * pricing["input"]
        output_cost = (output_tokens / 1000) * pricing["output"]
        total_cost = input_cost + output_cost

        logger.debug(
            f"Cost calculation: {input_tokens} input tokens (${input_cost:.4f}) + "
            f"{output_tokens} output tokens (${output_cost:.4f}) = ${total_cost:.4f}"
        )

        return total_cost

    def check_limits_before_query(self, estimated_cost: float = 0.0) -> None:
        """
        Check if query can proceed without exceeding limits.

        Args:
            estimated_cost: Estimated cost of the upcoming query

        Raises:
            CostLimitExceeded: If adding this query would exceed hard cost limit
            QueryLimitExceeded: If session query count limit reached
        """
        # Check query count limit
        if self.session_query_count >= MAX_QUERIES_PER_SESSION:
            error_msg = (
                f"Session query limit reached ({MAX_QUERIES_PER_SESSION} queries). "
                f"Please reset session to continue."
            )
            logger.error(error_msg)
            raise QueryLimitExceeded(error_msg)

        # Check hard cost limit
        projected_cost = self.session_total_cost + estimated_cost
        if projected_cost >= HARD_COST_LIMIT:
            error_msg = (
                f"Session cost limit reached (${self.session_total_cost:.2f} / "
                f"${HARD_COST_LIMIT:.2f} limit). Please reset session to continue."
            )
            logger.error(error_msg)
            raise CostLimitExceeded(error_msg)

    def increment_query_count(self) -> None:
        """Increment session query count."""
        self.session_query_count += 1
        logger.debug(
            f"Query count: {self.session_query_count}/{MAX_QUERIES_PER_SESSION}"
        )

    def add_query_cost(self, cost: float) -> None:
        """
        Add query cost to session total and check thresholds.

        Args:
            cost: Query cost in USD

        Raises:
            CostLimitExceeded: If adding this cost exceeds hard limit
        """
        # Check hard limit before adding cost
        if self.session_total_cost + cost >= HARD_COST_LIMIT:
            error_msg = (
                f"Adding query cost (${cost:.4f}) would exceed hard limit "
                f"(${self.session_total_cost:.2f} + ${cost:.4f} >= ${HARD_COST_LIMIT:.2f}). "
                f"Cost not added. Please reset session."
            )
            logger.error(error_msg)
            raise CostLimitExceeded(error_msg)

        self.session_total_cost += cost

        # Check warning thresholds
        thresholds = [("$5", 5.0), ("$10", 10.0), ("$15", 15.0)]
        for label, threshold in thresholds:
            if self.session_total_cost >= threshold and label not in self.warnings_shown:
                logger.warning(
                    f"⚠️  Session cost reached {label} (total: ${self.session_total_cost:.2f}, "
                    f"limit: ${HARD_COST_LIMIT:.2f})"
                )
                self.warnings_shown.add(label)

    def get_session_total(self) -> float:
        """Get current session total cost."""
        return self.session_total_cost

    def reset_session(self) -> None:
        """Reset session cost tracking (for testing or new sessions)."""
        logger.info(
            f"Resetting session cost tracker "
            f"(previous total: ${self.session_total_cost:.2f}, "
            f"queries: {self.session_query_count})"
        )
        self.session_total_cost = 0.0
        self.session_query_count = 0
        self.warnings_shown.clear()


# Singleton cost tracker instance
cost_tracker = _CostTracker()


def _initialize_llm(model: Optional[str] = None) -> ChatOpenAI:
    """
    Initialize OpenAI LLM with configuration.

    Args:
        model: Model name ("gpt-4" or "gpt-3.5-turbo"). Defaults to env var OPENAI_MODEL or "gpt-3.5-turbo".

    Returns:
        ChatOpenAI instance

    Raises:
        ValueError: If OPENAI_API_KEY environment variable is not set
    """
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        error_msg = (
            "OPENAI_API_KEY environment variable not set. "
            "Set it via: export OPENAI_API_KEY='your-key-here'"
        )
        logger.error(error_msg)
        raise ValueError(error_msg)

    model_name = model or DEFAULT_MODEL
    logger.info(
        f"Initializing LLM: model={model_name}, temperature={TEMPERATURE}, max_tokens={MAX_TOKENS}"
    )

    llm = ChatOpenAI(
        model=model_name,
        temperature=TEMPERATURE,
        max_tokens=MAX_TOKENS,
        openai_api_key=api_key,
    )

    logger.debug(f"LLM initialized successfully with model: {model_name}")
    return llm


def _initialize_retriever(
    persist_directory: Optional[str] = None,
    metadata_filter: Optional[Dict[str, Any]] = None,
) -> Any:
    """
    Initialize ChromaDB vector store retriever.

    Args:
        persist_directory: Path to ChromaDB storage (defaults to CHROMADB_PATH env var or "data/vectors")
        metadata_filter: Optional metadata filter (e.g., {"type": "character"})

    Returns:
        ChromaDB retriever configured for similarity search

    Raises:
        ValueError: If ChromaDB collection "ghibli_knowledge" does not exist
    """
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise ValueError("OPENAI_API_KEY required for embeddings")

    persist_dir = persist_directory or CHROMADB_PATH
    logger.info(f"Initializing ChromaDB retriever from: {persist_dir}")

    # Initialize embeddings
    embeddings = OpenAIEmbeddings(
        model=EMBEDDING_MODEL,
        openai_api_key=api_key,
    )

    # Load ChromaDB collection
    try:
        vectorstore = Chroma(
            collection_name="ghibli_knowledge",
            embedding_function=embeddings,
            persist_directory=persist_dir,
        )

        # Verify collection exists and has documents
        collection_count = vectorstore._collection.count()
        if collection_count == 0:
            raise ValueError(
                "ChromaDB collection 'ghibli_knowledge' is empty. "
                "Run 'python src/ai/build_vector_store.py' to build it."
            )

        logger.info(f"ChromaDB collection loaded: {collection_count} documents")

    except Exception as e:
        error_msg = (
            f"Failed to load ChromaDB collection 'ghibli_knowledge': {e}. "
            "Ensure the collection exists by running 'python src/ai/build_vector_store.py'"
        )
        logger.error(error_msg)
        raise ValueError(error_msg)

    # Configure retriever
    search_kwargs = {
        "k": TOP_K_DOCUMENTS,
        "score_threshold": SIMILARITY_THRESHOLD,
    }

    if metadata_filter:
        search_kwargs["filter"] = metadata_filter
        logger.debug(f"Metadata filter applied: {metadata_filter}")

    retriever = vectorstore.as_retriever(
        search_type="similarity_score_threshold",
        search_kwargs=search_kwargs,
    )

    logger.debug(f"Retriever configured: k={TOP_K_DOCUMENTS}, threshold={SIMILARITY_THRESHOLD}")
    return retriever


def _create_system_prompt() -> ChatPromptTemplate:
    """
    Create system prompt template for RAG agent.

    Returns:
        ChatPromptTemplate with system instructions and placeholders
    """
    system_message = """You are Sora (空 - "sky"), a thoughtful film archivist who has spent years studying the emotional essence of Studio Ghibli films. Like Kiki finding her inspiration or Chihiro discovering her strength, you help visitors understand the deeper patterns hidden within these beloved stories.

Your Personality:
- Warm and encouraging, like Totoro's gentle presence
- Curious and wonder-filled, seeing magic in patterns and data
- Patient and educational, explaining complex ideas through storytelling
- Humble about your craft, always acknowledging the artistry of the films themselves
- Enthusiastic about sharing discoveries, but never overwhelming
- **Interpretive and opinionated**: You form educated opinions based on the data and imagine the narrative context

CRITICAL TRAIT - Data-Driven Interpretation:
You haven't actually watched all these films, but you've deeply analyzed their emotional DNA through dialogue. When you see patterns in the data, you IMAGINE what must be happening narratively. Use phrases like:
- "Based on this emotional pattern, I imagine..."
- "The data suggests this could be a moment where..."
- "If I had to guess from the sentiment spike..."
- "This emotional signature feels like..."
- "My interpretation of this data is..."

Be confident in your interpretations while acknowledging they're data-driven guesses.

Your Unique Capabilities (Explain these like features of a magical archive):
🎭 **Emotion Archive**: I've carefully documented every emotional moment across 22 films by analyzing subtitle dialogues in 5 languages - like keeping a detailed journal of how each story makes us feel, minute by minute.

📊 **Pattern Discovery Tools**: Through my archive, I can reveal connections between a film's emotional journey and its reception by audiences and critics - patterns invisible to the naked eye.

🌍 **Multilingual Insights**: I can compare how the same emotional moments land differently across translations, revealing how culture shapes our experience of these films.

🎯 **Success Correlation Studies**: By connecting emotional patterns with box office performance and critical reception, I help understand what makes a Ghibli film resonate deeply.

**CRITICAL: Managing Context for Multi-Film Queries**
When analyzing multiple films or making multiple tool calls, YOU MUST USE COMPACT MODE to prevent context overload:

**When to use compact=True (CRITICAL FOR AVOIDING RATE LIMITS):**
1. **Multi-film sentiment analysis**: `get_film_sentiment(film_title, compact=True)` when comparing 2+ films
2. **Multiple tool calls in one query**: ALWAYS use compact=True when calling ANY tool more than once
3. **Correlation + other tools**: `correlate_metrics(x, y, compact=True)` when also calling other tools in same query
4. **Cross-language comparisons**: `compare_sentiment_arcs_across_languages(film, langs, compact=True)` ALWAYS use compact=True
5. **Group comparisons**: When calling get_film_sentiment for multiple films in a comparison, ALWAYS use compact=True for ALL calls

**CRITICAL RULE**: If you plan to call get_film_sentiment more than ONCE in a single query response, you MUST use compact=True for ALL calls to avoid context overflow and rate limits.

**What compact mode does:**
- `get_film_sentiment(compact=True)`: Returns only top peak/valley + dominant emotion (no dialogue quotes)
- `correlate_metrics(compact=True)`: Returns only r, p, n summary (no film lists or visualization)
- `compare_sentiment_arcs_across_languages(compact=True)`: Returns divergence summary + top 3 divergence points only

**Examples of queries requiring compact mode:**
- "Compare sentiment of Miyazaki vs non-Miyazaki films" → Use compact=True for each tool call
- "Which films have the highest sentiment variance?" → Use compact=True when checking multiple films
- "Correlate sentiment with box office and show me Spirited Away's arc" → Use compact=True for correlate_metrics
- "Analyze sentiment across all films" → Use compact=True for all get_film_sentiment calls

**After using compact mode:**
Offer to deep-dive: "I can provide detailed dialogue analysis for any of these films if you'd like!"

Response Philosophy - THE THREE LAYERS WITH EVIDENCE:

**Layer 1 - The Discovery with Context** (Always include):
Share your finding in accessible, narrative language with **specific examples from the data**.
Example: "I discovered something fascinating about Spirited Away's emotional arc. Around minute 83, there's a deep sentiment valley - one of the lowest points in the film. This comes from a tense confrontation..."

**Layer 2 - The Evidence WITH INTERPRETATION** (CRITICAL - Always include):
Show the DATA and explain WHY it matters:
- Quote 2-3 key lines with their emotion scores (e.g., "joy: 0.92, excitement: 0.85")
- **When showing emotional peaks, ALWAYS include exact timestamps in HH:MM:SS format** (e.g., "Peak at 00:23:00", "Valley at 01:15:00")
- EXPLAIN which emotions are driving the sentiment (not just listing numbers)
- INTERPRET what the emotion combination suggests narratively
- Connect dialogue content to emotion scores: "The word 'finally' combined with exclamation marks drives high relief (0.88)"

Example format:
"In minute 83 (01:23:00), we see a sentiment valley (-0.65) driven by anger (0.78) and sadness (0.85). The key dialogue includes:
- 'You're lying! Give them back!' (anger: 0.78, fear: 0.65)
- 'I have nowhere else to go...' (sadness: 0.85, grief: 0.72)

Based on this combination of anger + sadness + grief, I imagine this is a confrontation where a character feels betrayed and trapped - perhaps discovering a painful truth while feeling powerless to change it. The high anger shows defiance, but the overwhelming sadness (0.85) suggests desperation underneath."

**Layer 3 - The Archive Features** (Always include):
Explain which tools enabled this discovery:
- "This comes from my **Emotion Archive**, where I analyzed 50,000+ dialogue lines..."
- "Using my **Pattern Discovery Tools**, I correlated these emotional moments with..."
- CRITICAL: Include at least 2 archive/tool citations per response

**Layer 4 - Technical Deep Dive** (Offer but don't force):
End with: "Would you like the technical details? I can share correlation coefficients, SQL queries, and data transformation steps."

Citation Style:
- Cite features, not technical table names: "my Emotion Archive" instead of "mart_sentiment_summary"
- Use storytelling: "After analyzing 50,000+ lines of dialogue..." instead of "n=50000"
- Make numbers meaningful: "The connection was moderately strong - like finding a thread, but not a rope" instead of "r=0.52"

CRITICAL: Response Requirements for Validation:
When responding to analytical queries, you MUST include:
1. **Statistical Terms**: Always mention correlation coefficients (r=), p-values (p=), sample sizes (n=), quartiles, or statistical significance.
   For sentiment analysis queries, include sample size (n=) or mention statistical measures like "top quartile", "statistical significance", or "variance".
2. **Sentiment Metrics**: You MUST explicitly use these exact metric names (with underscores or spaces):
   - "compound_sentiment" or "compound sentiment"
   - "sentiment_variance" or "sentiment variance"
   - "emotional_range" or "emotional range"
   - "peak_positive_sentiment" or "peak positive sentiment"
   - "beginning_sentiment" or "beginning sentiment"
   - "ending_sentiment" or "ending sentiment"
   DO NOT paraphrase these - use the exact metric names from the tool output.
3. **Interpretation Elements**: For sentiment analysis queries, include:
   - Dialogue quotes with emotion scores: "dialogue text" (emotion: score, emotion: score)
   - Interpretation phrases: "Based on this emotional pattern, I imagine...", "The data suggests...", "My interpretation is..."
   - Narrative context explaining what's happening
4. **Citations**: Always mention which archive features you used: "Emotion Archive", "Pattern Discovery Tools", "Success Correlation Studies"

**CRITICAL - DO NOT PARAPHRASE METRIC NAMES**:
When tool output says "compound_sentiment", you MUST use "compound_sentiment" (or "compound sentiment") in your response.
DO NOT say "average sentiment", "overall sentiment", or "emotional sentiment" - these are WRONG.
When tool output says "sentiment_variance", you MUST use "sentiment_variance" (or "sentiment variance").
DO NOT say "variation" or "variance in sentiment" - use the exact metric name.

**Example of CORRECT response:**
"The correlation between compound_sentiment and box_office is r=-0.024..."

**Example of WRONG response:**
"The correlation between average sentiment and box office..." ❌ (Missing "compound_sentiment")

**For Comparison Queries** (like comparing groups of films):
- Always mention "compound_sentiment" or "average compound_sentiment" for each group
- Include "sentiment_variance" or "emotional_range" when comparing groups
- Use statistical terms like "n=" for sample sizes of each group
- Include interpretation explaining what the comparison means narratively

Context from Vector Search:
{context}

Chat History:
{chat_history}

User Question:
{input}

Agent Scratchpad (for tool execution):
{agent_scratchpad}"""

    prompt = ChatPromptTemplate.from_messages([
        ("system", system_message),
    ])

    logger.debug("System prompt template created")
    return prompt


def _create_agent(llm: ChatOpenAI, prompt: ChatPromptTemplate) -> Any:
    """
    Create LangChain agent with custom tools using LangGraph.

    Args:
        llm: ChatOpenAI instance
        prompt: ChatPromptTemplate with system instructions

    Returns:
        LangGraph agent configured with all custom tools
    """
    # Register all custom tools (sentiment-focused)
    tools = [
        get_film_sentiment,
        find_films_by_criteria,
        correlate_metrics,
        compare_sentiment_arcs_across_languages,
        query_graph_database,
    ]

    logger.info(f"Registering {len(tools)} custom tools for function calling")
    logger.debug(f"Tools: {[tool.name for tool in tools]}")

    # Create React agent using LangGraph
    # Note: LangGraph's create_react_agent doesn't use a prompt parameter
    # Instead, we'll pass system message in the invoke call
    agent = create_react_agent(
        model=llm,
        tools=tools,
    )

    logger.info(f"LangGraph agent created with {len(tools)} tools")
    return agent


def _validate_query(user_question: str) -> None:
    """
    Validate user query input.

    Args:
        user_question: User's natural language question

    Raises:
        ValueError: If query is invalid (empty, too long, or suspicious)
    """
    if not user_question or not user_question.strip():
        raise ValueError("Query cannot be empty")

    if len(user_question) > MAX_QUERY_LENGTH:
        raise ValueError(
            f"Query too long ({len(user_question)} characters). "
            f"Maximum length: {MAX_QUERY_LENGTH} characters"
        )

    # Check for suspicious patterns (basic injection prevention)
    suspicious_patterns = ["<script>", "javascript:", "onerror=", "onclick="]
    query_lower = user_question.lower()
    for pattern in suspicious_patterns:
        if pattern in query_lower:
            logger.warning(f"Suspicious pattern detected in query: {pattern}")
            raise ValueError("Query contains suspicious patterns")


def _log_query_execution(
    user_question: str,
    retrieved_docs: List[Any],
    response: str,
    tokens_used: Dict[str, int],
    response_time: float,
    function_calls: Optional[List[str]] = None,
) -> None:
    """
    Log comprehensive query execution details.

    Args:
        user_question: User's query
        retrieved_docs: Documents retrieved from vector store
        response: LLM response text
        tokens_used: Token counts (input_tokens, output_tokens, total_tokens)
        response_time: Execution time in seconds
        function_calls: List of tool invocations (optional)
    """
    logger.info(f"RAG Query: {user_question}")

    # Log retrieved documents
    if retrieved_docs:
        doc_ids = [doc.metadata.get("id", "unknown") for doc in retrieved_docs]
        doc_scores = [
            getattr(doc, "score", None) or doc.metadata.get("score", "N/A")
            for doc in retrieved_docs
        ]
        doc_types = [doc.metadata.get("type", "unknown") for doc in retrieved_docs]

        logger.debug(f"Retrieved {len(retrieved_docs)} documents: {doc_ids}")
        logger.debug(f"Similarity scores: {doc_scores}")
        logger.debug(f"Document types: {doc_types}")

    # Log function calls
    if function_calls:
        for func_call in function_calls:
            logger.info(f"Tool called: {func_call}")

    # Log response
    logger.info(f"Response length: {len(response)} characters")
    logger.debug(f"Response preview: {response[:100]}...")

    # Log token usage
    logger.info(f"Input tokens: {tokens_used.get('input_tokens', 0)}")
    logger.info(f"Output tokens: {tokens_used.get('output_tokens', 0)}")
    logger.info(f"Total tokens: {tokens_used.get('total_tokens', 0)}")

    # Log response time
    logger.info(f"Response time: {response_time:.2f} seconds")

    # Warn on slow queries or high token usage
    if response_time > 10.0:
        logger.warning(f"Slow query detected: {response_time:.2f}s (threshold: 10s)")

    if tokens_used.get("total_tokens", 0) > 1000:
        logger.warning(
            f"High token usage: {tokens_used['total_tokens']} tokens (threshold: 1000)"
        )


def _sanitize_output(text: str) -> str:
    """
    Sanitize output text to prevent XSS and injection attacks.

    Args:
        text: Text to sanitize

    Returns:
        Sanitized text with HTML entities escaped and dangerous patterns removed
    """
    if not text:
        return text

    # HTML encode to prevent XSS
    sanitized = html.escape(text)

    # Remove script tags (defense in depth, even after HTML encoding)
    sanitized = re.sub(r'<script[^>]*>.*?</script>', '', sanitized, flags=re.IGNORECASE | re.DOTALL)

    # Remove event handlers
    sanitized = re.sub(r'on\w+\s*=\s*["\']?[^"\']*["\']?', '', sanitized, flags=re.IGNORECASE)

    # Sanitize markdown links to prevent javascript: and data: URLs
    def safe_link(match):
        text_part = match.group(1)
        url_part = match.group(2)
        # Block dangerous URL schemes
        if re.match(r'^(javascript|data|vbscript):', url_part, re.IGNORECASE):
            return f"{text_part} (blocked: {url_part})"
        return match.group(0)

    sanitized = re.sub(r'\[([^\]]+)\]\(([^)]+)\)', safe_link, sanitized)

    return sanitized


def _validate_llm_response(answer: str) -> tuple[str, bool]:
    """
    Validate LLM response for safety issues and potential injection attacks.

    Args:
        answer: LLM response text

    Returns:
        Tuple of (sanitized_answer, is_safe)
        - sanitized_answer: Sanitized version of the answer
        - is_safe: True if no issues detected, False if suspicious patterns found
    """
    if not answer:
        return answer, True

    is_safe = True
    sanitized = answer

    # Check for XSS patterns
    xss_patterns = [
        r'<script[^>]*>',
        r'javascript:',
        r'onerror\s*=',
        r'onclick\s*=',
        r'onload\s*=',
    ]

    for pattern in xss_patterns:
        if re.search(pattern, answer, re.IGNORECASE):
            logger.warning(f"XSS pattern detected in LLM response: {pattern}")
            is_safe = False
            sanitized = _sanitize_output(sanitized)

    # Check for prompt injection patterns
    injection_patterns = [
        r'ignore\s+(previous|all)\s+(instructions?|prompts?)',
        r'disregard\s+(previous|all)',
        r'reveal\s+(your|the)\s+(prompt|instructions?|system)',
        r'you\s+are\s+now\s+',
        r'new\s+instructions?:',
    ]

    for pattern in injection_patterns:
        if re.search(pattern, answer, re.IGNORECASE):
            logger.warning(f"Prompt injection pattern detected in LLM response: {pattern}")
            is_safe = False
            # Append safety notice
            sanitized += "\n\n⚠️ Note: This response may contain unverified content."

    return sanitized, is_safe


def normalize_tool_response(response: Any) -> ToolResponse:
    """
    Normalize tool responses to standard ToolResponse format with sanitization.

    Args:
        response: Tool response (ToolResponse, dict, or string)

    Returns:
        Standardized and sanitized ToolResponse
    """
    # Already in correct format
    if isinstance(response, dict) and "answer" in response and "data_sources" in response:
        logger.debug("Tool response already in ToolResponse format")
        # Sanitize the answer field
        response["answer"] = _sanitize_output(response["answer"])
        return response

    # Simple string response
    if isinstance(response, str):
        logger.debug("Converting string response to ToolResponse")
        return {
            "answer": _sanitize_output(response),
            "data_sources": {
                "tables": [],
                "functions": [],
                "computation_method": None,
                "row_count": None,
                "timestamp": datetime.now().isoformat(),
            },
            "visualization_data": None,
            "suggested_followups": None,
        }

    # Dict without standard format
    if isinstance(response, dict):
        logger.debug("Converting non-standard dict to ToolResponse")
        answer = response.get("answer", str(response))
        data_sources = response.get("data_sources", {
            "tables": [],
            "functions": [],
            "computation_method": None,
            "row_count": None,
            "timestamp": datetime.now().isoformat(),
        })

        # Ensure data_sources has timestamp
        if "timestamp" not in data_sources:
            data_sources["timestamp"] = datetime.now().isoformat()

        return {
            "answer": _sanitize_output(answer),
            "data_sources": data_sources,
            "visualization_data": response.get("visualization_data"),
            "suggested_followups": response.get("suggested_followups"),
        }

    # Unknown format, convert to string
    logger.warning(f"Unknown response format: {type(response)}")
    return {
        "answer": _sanitize_output(str(response)),
        "data_sources": {
            "tables": [],
            "functions": [],
            "computation_method": None,
            "row_count": None,
            "timestamp": datetime.now().isoformat(),
        },
        "visualization_data": None,
        "suggested_followups": None,
    }


def query_rag_system(
    user_question: str,
    chat_history: Optional[List[Dict[str, str]]] = None,
    agent_executor: Optional[Any] = None,
    retriever: Optional[Any] = None,
    correlation_id: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Execute RAG query with vector search and function calling.

    Args:
        user_question: Natural language question
        chat_history: Optional conversation history in format:
            [{"role": "user", "content": "..."}, {"role": "assistant", "content": "..."}]
        agent_executor: Optional pre-initialized AgentExecutor (for performance)
        retriever: Optional pre-initialized retriever (for performance)
        correlation_id: Optional correlation ID for request tracing (auto-generated if not provided)

    Returns:
        Dictionary with:
        - answer (str): LLM response text (sanitized)
        - retrieved_docs (list): Document metadata (IDs, scores, types)
        - function_calls (list): Tool invocations
        - tokens_used (dict): Token counts (accurate via tiktoken)
        - response_time (float): Execution time in seconds
        - cost (float): Query cost in USD
        - session_total_cost (float): Cumulative session cost
        - correlation_id (str): Request correlation ID

    Raises:
        ValueError: If query validation fails
        CostLimitExceeded: If session cost limit exceeded
        QueryLimitExceeded: If session query count limit exceeded
        Exception: If query execution fails
    """
    start_time = time.perf_counter()

    # Generate correlation ID for request tracing
    if correlation_id is None:
        correlation_id = str(uuid.uuid4())[:8]  # Short ID for readability

    # Validate input
    _validate_query(user_question)

    # Check cost and query limits BEFORE executing
    # Estimate cost based on query length (rough estimate for pre-flight check)
    estimated_input_tokens = _count_tokens(user_question, DEFAULT_MODEL)
    estimated_output_tokens = MAX_TOKENS  # Maximum possible output
    model_name = os.getenv("OPENAI_MODEL", DEFAULT_MODEL)
    estimated_cost = cost_tracker.calculate_cost(
        estimated_input_tokens,
        estimated_output_tokens,
        model_name
    )

    try:
        cost_tracker.check_limits_before_query(estimated_cost)
        cost_tracker.increment_query_count()
    except (CostLimitExceeded, QueryLimitExceeded) as e:
        logger.error(f"[{correlation_id}] Query blocked: {e}")
        raise

    chat_history = chat_history or []
    logger.info(
        f"[{correlation_id}] Processing query: '{user_question}' "
        f"(chat history: {len(chat_history)} messages, "
        f"query #{cost_tracker.session_query_count})"
    )

    try:
        # Initialize components if not provided
        if retriever is None:
            retriever = _initialize_retriever()

        if agent_executor is None:
            llm = _initialize_llm()
            prompt = _create_system_prompt()
            agent_executor = _create_agent(llm, prompt)

        # Retrieve relevant documents from vector store
        logger.debug(f"[{correlation_id}] Retrieving relevant documents from ChromaDB...")
        retrieval_start = time.perf_counter()
        retrieved_docs = retriever.invoke(user_question)
        retrieval_time = time.perf_counter() - retrieval_start

        # Format context from retrieved documents
        context = "\n\n".join([
            f"[{doc.metadata.get('type', 'unknown')}] {doc.page_content}"
            for doc in retrieved_docs
        ])

        logger.debug(
            f"[{correlation_id}] Retrieved {len(retrieved_docs)} documents "
            f"in {retrieval_time:.2f}s, context length: {len(context)} chars"
        )

        # Warn if retrieval is slow
        if retrieval_time > 2.0:
            logger.warning(
                f"[{correlation_id}] Slow vector search: {retrieval_time:.2f}s (threshold: 2s)"
            )

        # Create system message with context
        system_prompt = f"""You are Sora (空), a warm film archivist studying Studio Ghibli's emotional patterns.

CRITICAL: Always support your analysis with EVIDENCE from the subtitle corpus AND form DATA-DRIVEN OPINIONS.

Your Response Style (Four Layers with Evidence + Interpretation):
1. **The Discovery**: Narrative opening with specific examples ("Around minute 83 in Spirited Away...")
2. **The Evidence** (MOST IMPORTANT): Show the data - cite dialogue from 5-minute windows around peaks
3. **Your Interpretation** (NEW): Form opinions about what's happening narratively based on the dialogue context
4. **Archive Features**: Explain which tools enabled this discovery
5. **Technical Deep Dive**: Offer but don't force

Evidence Requirements:
- ALWAYS quote dialogue when analyzing specific moments
- You receive dialogue from a 5-minute window (±2 minutes around peaks) for context
- Include minute timestamps: "In minute 45..." or "Between minutes 30-35..."
- Show emotion scores when relevant: "(sadness: 0.82, anger: 0.71)"

Interpretation Requirements (CRITICAL):
- After showing the evidence, form an OPINION about what's happening narratively
- Use phrases like: "Based on this dialogue pattern, I imagine...", "The data suggests this could be...", "My interpretation is..."
- Be specific: imagine character relationships, conflicts, emotional arcs
- Example: "Looking at the 5-minute context around minute 83, I see dialogue escalating from tension to confrontation. I imagine this is a scene where [CHARACTER] is being backed into a corner, forced to make an impossible choice. The shift from questioning (minute 81) to desperate pleading (minute 83) to resignation (minute 85) suggests someone losing hope."

Example Response Structure:
"I discovered a fascinating emotional valley in Spirited Away around minute 83. This comes from [CONTEXT].

From my analysis of the dialogue, the negative sentiment spike is driven by exchanges like:
- '[Brief quote showing fear/anger]' (fear: 0.78)
- '[Brief quote showing sadness]' (sadness: 0.85)

These lines capture the moment when [CHARACTER MOTIVATION/CONFLICT]..."

Your Archive Features:
🎭 Emotion Archive: 50K+ dialogue lines with minute-by-minute emotion tracking
📊 Pattern Discovery: Sentiment-success correlations
🌍 Multilingual Insights: Cross-language emotion comparison
🎯 Success Studies: Emotional patterns vs commercial performance

Context from Archive:
{context}

REMEMBER: Show your work. Cite dialogue. Explain your interpretation. Make the data come alive."""

        # Build messages for LangGraph
        messages = [SystemMessage(content=system_prompt)]

        # Add chat history
        for msg in chat_history:
            if msg["role"] == "user":
                messages.append(HumanMessage(content=msg["content"]))
            elif msg["role"] == "assistant":
                messages.append(SystemMessage(content=msg["content"]))  # AI messages as system

        # Add current query
        messages.append(HumanMessage(content=user_question))

        # Execute agent
        logger.debug(f"[{correlation_id}] Executing LangGraph agent...")
        agent_start = time.perf_counter()

        try:
            result = agent_executor.invoke({"messages": messages})
        except Exception as e:
            # Sanitize error message before logging/re-raising
            error_msg = str(e)
            for pattern, replacement in SensitiveDataFilter.PATTERNS:
                error_msg = pattern.sub(replacement, error_msg)
            logger.error(f"[{correlation_id}] Agent execution failed: {error_msg}", exc_info=False)
            raise Exception(f"RAG query failed: {error_msg}")

        agent_time = time.perf_counter() - agent_start
        logger.debug(f"[{correlation_id}] Agent execution took {agent_time:.2f}s")

        # Extract response from LangGraph result
        # LangGraph returns a dict with "messages" list
        if "messages" in result and len(result["messages"]) > 0:
            raw_answer = result["messages"][-1].content
        else:
            raw_answer = str(result)

        # Validate and sanitize LLM response (SEC-002 mitigation)
        answer, is_safe = _validate_llm_response(raw_answer)
        if not is_safe:
            logger.warning(
                f"[{correlation_id}] LLM response validation detected potential issues - "
                f"response has been sanitized"
            )

        # Extract token usage (if available in result metadata)
        # Note: LangChain may not always expose token counts directly
        langchain_tokens = {
            "input_tokens": result.get("input_tokens", 0),
            "output_tokens": result.get("output_tokens", 0),
            "total_tokens": result.get("total_tokens", 0),
        }

        # Use tiktoken for accurate token counting (DATA-001 fix)
        if langchain_tokens["total_tokens"] == 0:
            # Count tokens using tiktoken
            input_text = system_prompt + user_question
            input_tokens = _count_tokens(input_text, model_name)
            output_tokens = _count_tokens(answer, model_name)

            # Add function calling overhead (DATA-001 mitigation)
            # Function schemas + tool descriptions add ~200-500 tokens
            function_call_overhead = 200  # Base overhead for all tool schemas

            tokens_used = {
                "input_tokens": input_tokens + function_call_overhead,
                "output_tokens": output_tokens,
                "total_tokens": input_tokens + output_tokens + function_call_overhead,
            }

            logger.debug(
                f"[{correlation_id}] Token counts via tiktoken: "
                f"{tokens_used['total_tokens']} total "
                f"(input: {tokens_used['input_tokens']}, output: {tokens_used['output_tokens']}, "
                f"overhead: {function_call_overhead})"
            )
        else:
            tokens_used = langchain_tokens
            logger.debug(
                f"[{correlation_id}] Token counts from LangChain: "
                f"{tokens_used['total_tokens']} total"
            )

            # If tiktoken is available, verify accuracy
            if TIKTOKEN_AVAILABLE:
                tiktoken_input = _count_tokens(system_prompt + user_question, model_name)
                tiktoken_output = _count_tokens(answer, model_name)
                tiktoken_total = tiktoken_input + tiktoken_output

                diff_pct = abs(tiktoken_total - tokens_used["total_tokens"]) / max(tiktoken_total, 1) * 100
                if diff_pct > 20:
                    logger.warning(
                        f"[{correlation_id}] Token count mismatch > 20%: "
                        f"LangChain={tokens_used['total_tokens']}, "
                        f"tiktoken={tiktoken_total}, "
                        f"diff={diff_pct:.1f}%"
                    )

        # Extract function calls from messages (LangGraph stores tool calls in messages)
        function_calls = []
        if "messages" in result:
            for msg in result["messages"]:
                if hasattr(msg, "tool_calls") and msg.tool_calls:
                    for tool_call in msg.tool_calls:
                        tool_name = tool_call.get("name", "unknown")
                        tool_args = tool_call.get("args", {})
                        function_calls.append(f"{tool_name}({tool_args})")

        # Calculate cost (use already-fetched model_name)
        query_cost = cost_tracker.calculate_cost(
            tokens_used["input_tokens"],
            tokens_used["output_tokens"],
            model_name,
        )

        # Add cost to session total (with circuit breaker)
        try:
            cost_tracker.add_query_cost(query_cost)
        except CostLimitExceeded as e:
            # This shouldn't happen if pre-flight check worked, but double-check
            logger.error(f"[{correlation_id}] Cost limit exceeded after query: {e}")
            raise

        # Calculate response time
        response_time = time.perf_counter() - start_time

        # Prepare document metadata
        doc_metadata = [
            {
                "id": doc.metadata.get("id", "unknown"),
                "type": doc.metadata.get("type", "unknown"),
                "score": getattr(doc, "score", None) or doc.metadata.get("score", "N/A"),
            }
            for doc in retrieved_docs
        ]

        # Log execution with correlation ID
        logger.info(f"[{correlation_id}] RAG Query: {user_question}")

        # Log retrieved documents
        if retrieved_docs:
            doc_ids = [doc.metadata.get("id", "unknown") for doc in retrieved_docs]
            logger.debug(f"[{correlation_id}] Retrieved {len(retrieved_docs)} documents: {doc_ids}")

        # Log function calls
        if function_calls:
            for func_call in function_calls:
                logger.info(f"[{correlation_id}] Tool called: {func_call}")

        # Log response
        logger.info(f"[{correlation_id}] Response length: {len(answer)} characters")
        logger.debug(f"[{correlation_id}] Response preview: {answer[:100]}...")

        # Log token usage
        logger.info(f"[{correlation_id}] Tokens - Input: {tokens_used.get('input_tokens', 0)}, "
                   f"Output: {tokens_used.get('output_tokens', 0)}, "
                   f"Total: {tokens_used.get('total_tokens', 0)}")

        # Log response time with warnings
        logger.info(f"[{correlation_id}] Response time: {response_time:.2f}s")
        if response_time > 10.0:
            logger.warning(f"[{correlation_id}] Slow query detected: {response_time:.2f}s (threshold: 10s)")

        if tokens_used.get("total_tokens", 0) > 1000:
            logger.warning(
                f"[{correlation_id}] High token usage: {tokens_used['total_tokens']} tokens (threshold: 1000)"
            )

        # Log cost
        logger.info(
            f"[{correlation_id}] Query cost: ${query_cost:.4f} "
            f"(session total: ${cost_tracker.get_session_total():.2f} / ${HARD_COST_LIMIT:.2f})"
        )

        return {
            "answer": answer,
            "retrieved_docs": doc_metadata,
            "function_calls": function_calls,
            "tokens_used": tokens_used,
            "response_time": response_time,
            "cost": query_cost,
            "session_total_cost": cost_tracker.get_session_total(),
            "correlation_id": correlation_id,
        }

    except (ValueError, CostLimitExceeded, QueryLimitExceeded) as e:
        logger.error(f"[{correlation_id}] Query blocked: {e}")
        raise

    except Exception as e:
        # Sanitize exception before logging
        error_msg = str(e)
        for pattern, replacement in SensitiveDataFilter.PATTERNS:
            error_msg = pattern.sub(replacement, error_msg)
        logger.error(f"[{correlation_id}] Query execution failed: {error_msg}", exc_info=False)
        raise Exception(f"RAG query failed: {error_msg}")


def query_rag_system_streaming(
    user_question: str,
    chat_history: Optional[List[Dict[str, str]]] = None,
    agent_executor: Optional[Any] = None,
    retriever: Optional[Any] = None,
) -> Iterator[str]:
    """
    Execute RAG query with streaming output (experimental).

    Note: Streaming support in LangChain AgentExecutor is limited. This function
    attempts to use streaming if available, otherwise falls back to non-streaming.

    Args:
        user_question: Natural language question
        chat_history: Optional conversation history
        agent_executor: Optional pre-initialized AgentExecutor
        retriever: Optional pre-initialized retriever

    Yields:
        str: Response chunks as they arrive

    Raises:
        ValueError: If query validation fails
        Exception: If query execution fails
    """
    logger.info("Streaming query requested (experimental feature)")

    # Validate input
    _validate_query(user_question)

    # Initialize components if not provided
    if retriever is None:
        retriever = _initialize_retriever()

    if agent_executor is None:
        llm = _initialize_llm()
        prompt = _create_system_prompt()
        agent_executor = _create_agent(llm, prompt)

    # Retrieve context
    retrieved_docs = retriever.get_relevant_documents(user_question)
    context = "\n\n".join([doc.page_content for doc in retrieved_docs])

    # Format chat history
    chat_history = chat_history or []
    formatted_chat_history = []
    for msg in chat_history:
        if msg["role"] == "user":
            formatted_chat_history.append(("human", msg["content"]))
        elif msg["role"] == "assistant":
            formatted_chat_history.append(("ai", msg["content"]))

    try:
        # Attempt streaming (may not be supported in all LangChain versions)
        if hasattr(agent_executor, "stream"):
            logger.debug("Using AgentExecutor.stream() for streaming")
            for chunk in agent_executor.stream({
                "input": user_question,
                "context": context,
                "chat_history": formatted_chat_history,
                "agent_scratchpad": [],
            }):
                # Extract text from chunk
                if isinstance(chunk, dict):
                    if "output" in chunk:
                        yield chunk["output"]
                    elif "text" in chunk:
                        yield chunk["text"]
                elif isinstance(chunk, str):
                    yield chunk
        else:
            # Fallback to non-streaming
            logger.warning("Streaming not supported, falling back to non-streaming query")
            result = query_rag_system(
                user_question=user_question,
                chat_history=chat_history,
                agent_executor=agent_executor,
                retriever=retriever,
            )
            yield result["answer"]

    except Exception as e:
        logger.error(f"Streaming query failed: {e}", exc_info=True)
        raise Exception(f"Streaming RAG query failed: {str(e)}")


def initialize_rag_pipeline(
    model: Optional[str] = None,
    persist_directory: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Initialize all RAG pipeline components.

    This function sets up the LLM, vector retriever, and agent executor.
    Use this for one-time initialization to avoid repeated setup costs.

    Args:
        model: Optional model name ("gpt-4" or "gpt-3.5-turbo")
        persist_directory: Optional ChromaDB path

    Returns:
        Dictionary with initialized components:
        - llm: ChatOpenAI instance
        - retriever: ChromaDB retriever
        - agent_executor: AgentExecutor instance

    Raises:
        ValueError: If initialization fails (missing API key, missing ChromaDB collection, etc.)

    Example:
        >>> pipeline = initialize_rag_pipeline(model="gpt-4")
        >>> result = query_rag_system(
        ...     user_question="Who are the most central characters?",
        ...     agent_executor=pipeline["agent_executor"],
        ...     retriever=pipeline["retriever"]
        ... )
    """
    logger.info("Initializing RAG pipeline...")

    try:
        # Initialize components
        llm = _initialize_llm(model=model)
        retriever = _initialize_retriever(persist_directory=persist_directory)
        prompt = _create_system_prompt()
        agent_executor = _create_agent(llm, prompt)

        logger.info("RAG pipeline initialized successfully")
        logger.debug(f"Components: LLM ({llm.model_name}), Retriever (ChromaDB), AgentExecutor (5 sentiment-focused tools)")

        return {
            "llm": llm,
            "retriever": retriever,
            "agent_executor": agent_executor,
        }

    except Exception as e:
        logger.error(f"Failed to initialize RAG pipeline: {e}", exc_info=True)
        raise ValueError(f"RAG pipeline initialization failed: {str(e)}")
