"""
Interactive CLI interface for testing the RAG system.

This module provides a command-line interface for testing RAG queries before
building the Streamlit UI. It supports interactive chat sessions, debug mode,
conversation history saving, and session statistics.

Usage:
    Basic usage:
        $ python src/ai/rag_cli.py

    Debug mode:
        $ python src/ai/rag_cli.py --debug

    Custom log level:
        $ python src/ai/rag_cli.py --log-level DEBUG

Special Commands:
    /exit  - Quit the CLI
    /reset - Clear conversation history
    /stats - Show session statistics
    /help  - Show help message

Example:
    >>> python src/ai/rag_cli.py
    >>> Who are the most central characters?
    Based on character centrality analysis, Chihiro is the most central character...
    >>> /stats
    === Session Statistics ===
    Duration: 2 minutes 15 seconds
    Total Queries: 1
    ...
"""

import argparse
import json
import logging
import os
import sys
import time
import traceback
from datetime import datetime
from typing import Any, Dict, List, Optional

from src.ai.rag_pipeline import initialize_rag_pipeline, query_rag_system
from src.shared.exceptions import DatabaseError, RAGError, RateLimitError

# Module-level logger
logger = logging.getLogger(__name__)

# CLI version
__version__ = "1.0"


def parse_arguments() -> argparse.Namespace:
    """
    Parse command-line arguments for the CLI.

    Returns:
        Parsed argument namespace with debug, streaming, log_level, and save_history settings.
    """
    parser = argparse.ArgumentParser(
        description="SpiritedData RAG CLI - Interactive Query Testing",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python src/ai/rag_cli.py                     # Basic usage
  python src/ai/rag_cli.py --debug             # Enable debug mode
  python src/ai/rag_cli.py --log-level DEBUG   # Set log level
        """,
    )

    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable verbose output (retrieved docs, function calls, token usage)",
    )

    parser.add_argument(
        "--no-streaming",
        action="store_true",
        help="Disable streaming output effect",
    )

    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default="INFO",
        help="Set logging level (default: INFO)",
    )

    parser.add_argument(
        "--save-history",
        action="store_true",
        default=True,
        help="Enable conversation history saving (default: True)",
    )

    return parser.parse_args()


def display_welcome() -> None:
    """Display welcome message with CLI title, examples, and special commands."""
    print("=" * 80)
    print("✨ Welcome to Sora's Archive - Studio Ghibli Emotion Analysis")
    print(f"   Version {__version__} | Powered by sentiment analysis across 22 films")
    print("=" * 80)
    print()
    print("🎭 Meet Sora (空 - 'sky')")
    print("   A thoughtful archivist who studies emotional patterns in Ghibli films")
    print()
    print("📚 Try these questions to explore the archive:")
    print("  • \"What is the correlation between sentiment and box office revenue?\"")
    print("  • \"Show me the emotional arc of Spirited Away\"")
    print("  • \"Do films with rising emotional trajectories perform better with critics?\"")
    print("  • \"How does sentiment differ across languages for Spirited Away?\"")
    print("  • \"Which film has the highest emotional variance?\"")
    print()
    print("✨ Archive Features:")
    print("  🎭 Emotion Archive - Sentiment from 50K+ dialogue lines (5 languages)")
    print("  📊 Pattern Discovery - Correlations between emotions and film reception")
    print("  🌍 Multilingual Insights - Cross-translation emotion comparison")
    print("  🎯 Success Studies - Emotional patterns vs box office & critics")
    print()
    print("Special Commands:")
    print("  /exit  - Quit the CLI")
    print("  /reset - Clear conversation history")
    print("  /stats - Show session statistics")
    print("  /help  - Show this help message")
    print()
    print("Type your question or command:")
    print("-" * 80)


class ConversationSession:
    """
    Manage conversation session state and statistics.

    Attributes:
        history: List of conversation messages with role and content
        start_time: Session start timestamp
        total_queries: Counter for queries processed
        total_tokens: Cumulative token usage
        total_cost: Cumulative API cost in USD
        response_times: List of response times for calculating averages
    """

    def __init__(self) -> None:
        """Initialize a new conversation session."""
        self.history: List[Dict[str, str]] = []
        self.start_time: datetime = datetime.now()
        self.total_queries: int = 0
        self.total_tokens: int = 0
        self.total_cost: float = 0.0
        self.response_times: List[float] = []

    def add_message(self, role: str, content: str) -> None:
        """
        Add a message to conversation history.

        Args:
            role: Message role ("user" or "assistant")
            content: Message content
        """
        self.history.append({"role": role, "content": content})

    def get_history(self) -> List[Dict[str, str]]:
        """
        Get conversation history.

        Returns:
            List of message dictionaries with role and content
        """
        return self.history

    def reset(self) -> None:
        """Clear conversation history and reset counters."""
        self.history = []
        self.total_queries = 0
        self.total_tokens = 0
        self.total_cost = 0.0
        self.response_times = []
        logger.info("Session reset")

    def get_stats(self) -> Dict[str, Any]:
        """
        Get session statistics.

        Returns:
            Dictionary with session duration, query count, tokens, cost, and average response time
        """
        duration = datetime.now() - self.start_time
        avg_response_time = (
            sum(self.response_times) / len(self.response_times)
            if self.response_times
            else 0.0
        )

        return {
            "duration": duration,
            "total_queries": self.total_queries,
            "total_tokens": self.total_tokens,
            "total_cost": self.total_cost,
            "average_response_time": avg_response_time,
        }


def handle_special_command(
    command: str, session: ConversationSession
) -> Optional[str]:
    """
    Handle special CLI commands.

    Args:
        command: Special command string (e.g., "/exit", "/reset")
        session: Current conversation session

    Returns:
        "exit" signal if /exit command, None otherwise
    """
    if command == "/exit":
        return "exit"

    elif command == "/reset":
        session.reset()
        print("✓ Conversation history cleared")
        print()

    elif command == "/stats":
        stats = session.get_stats()
        duration = stats["duration"]
        hours, remainder = divmod(int(duration.total_seconds()), 3600)
        minutes, seconds = divmod(remainder, 60)

        print()
        print("=" * 30)
        print("Session Statistics")
        print("=" * 30)

        if hours > 0:
            print(f"Duration: {hours} hours {minutes} minutes {seconds} seconds")
        elif minutes > 0:
            print(f"Duration: {minutes} minutes {seconds} seconds")
        else:
            print(f"Duration: {seconds} seconds")

        print(f"Total Queries: {stats['total_queries']}")
        print(f"Total Tokens: {stats['total_tokens']:,}")
        print(f"Total Cost: ${stats['total_cost']:.2f}")

        if stats["average_response_time"] > 0:
            print(f"Average Response Time: {stats['average_response_time']:.1f} seconds")

        print("=" * 30)
        print()

    elif command == "/help":
        display_welcome()

    else:
        print(f"Unknown command: {command}")
        print("Type /help for available commands.")
        print()

    return None


def print_with_streaming(text: str, delay: float = 0.01) -> None:
    """
    Print text with streaming effect to simulate typing, preserving line breaks.

    Args:
        text: Text to print
        delay: Delay between words in seconds (default: 0.01)
    """
    # Split by lines first to preserve formatting
    lines = text.split('\n')

    for line_idx, line in enumerate(lines):
        if not line.strip():
            # Empty line - preserve it
            print()
            continue

        # Split line into words and stream them
        words = line.split()
        for word in words:
            print(word, end=" ", flush=True)
            time.sleep(delay)

        # Newline after each line (except the last one which gets it anyway)
        print()


def process_query(
    query: str,
    session: ConversationSession,
    debug: bool = False,
    streaming: bool = True,
) -> Dict[str, Any]:
    """
    Process RAG query and update session.

    Args:
        query: User query string
        session: Current conversation session
        debug: Enable debug output (retrieved docs, function calls, tokens)
        streaming: Enable streaming output effect

    Returns:
        Result dictionary from RAG pipeline with answer, docs, tokens, etc.

    Raises:
        ValueError: If query is empty or too long
        RAGError: If RAG system fails
        RateLimitError: If rate limit exceeded
        DatabaseError: If database operation fails
    """
    # Input validation
    query = query.strip()

    if not query:
        raise ValueError("Query cannot be empty")

    if len(query) > 1000:
        raise ValueError("Query too long (maximum 1000 characters)")

    # Execute RAG query
    logger.debug(f"Processing query: {query}")

    result = query_rag_system(
        user_question=query,
        chat_history=session.get_history(),
    )

    # Update session
    session.add_message("user", query)
    session.add_message("assistant", result["answer"])
    session.total_queries += 1

    # Update token and cost tracking
    if "tokens_used" in result:
        tokens_dict = result["tokens_used"]
        total_tokens = tokens_dict.get("total_tokens", 0)
        session.total_tokens += total_tokens

    if "cost" in result:
        session.total_cost += result["cost"]

    if "response_time" in result:
        session.response_times.append(result["response_time"])

    # Display debug info if enabled
    if debug:
        print()
        print("[DEBUG] Retrieved Documents:")
        retrieved_docs = result.get("retrieved_docs", [])
        if retrieved_docs:
            for doc in retrieved_docs[:3]:  # Show top 3
                doc_id = doc.get("id", "unknown")
                score = doc.get("score", 0.0)
                doc_type = doc.get("type", "unknown")
                print(f"  - {doc_id}: {doc_type} (score: {score:.2f})")
        else:
            print("  (no documents retrieved)")

        print()
        print("[DEBUG] Function Calls:")
        function_calls = result.get("function_calls", [])
        if function_calls:
            for call in function_calls:
                print(f"  - {call}")
        else:
            print("  (no function calls)")

        print()
        print("[DEBUG] Token Usage:")
        if "tokens_used" in result:
            tokens = result["tokens_used"]
            print(f"  Input: {tokens.get('input_tokens', 0)} tokens")
            print(f"  Output: {tokens.get('output_tokens', 0)} tokens")
            print(f"  Total: {tokens.get('total_tokens', 0)} tokens")
            print(f"  Cost: ${result.get('cost', 0.0):.4f}")

        print()
        print(f"[DEBUG] Response Time: {result.get('response_time', 0.0):.1f} seconds")
        print()

    # Display response
    answer = result["answer"]
    if streaming:
        print_with_streaming(answer)
    else:
        print(answer)

    print()

    return result


def handle_error(error: Exception, debug: bool = False) -> None:
    """
    Handle errors with user-friendly messages.

    Args:
        error: Exception that occurred
        debug: Enable debug mode (show full traceback)
    """
    if isinstance(error, RAGError):
        print(f"❌ RAG System Error: {error}")
        print("Please try again in a few minutes.")

    elif isinstance(error, RateLimitError):
        print(f"⚠️ Rate Limit Exceeded: {error}")
        print("Please wait and try again.")

    elif isinstance(error, DatabaseError):
        print(f"❌ Database Error: {error}")
        print("Please check database connection and try again.")

    elif isinstance(error, ValueError):
        print(f"⚠️ Invalid Input: {error}")

    else:
        print("❌ Unexpected Error: An error occurred")
        logger.error(f"Unexpected error: {error}", exc_info=True)

    if debug:
        print()
        print("[DEBUG] Full traceback:")
        traceback.print_exc()

    print()


def save_conversation_history(
    session: ConversationSession, save_dir: str = "logs"
) -> str:
    """
    Save conversation history to JSON file.

    Args:
        session: Conversation session to save
        save_dir: Directory to save conversation logs (default: "logs")

    Returns:
        Path to saved conversation file
    """
    # Create logs directory if not exists
    os.makedirs(save_dir, exist_ok=True)

    # Generate filename with timestamp
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    filename = f"rag_conversation_{timestamp}.json"
    filepath = os.path.join(save_dir, filename)

    # Get session stats
    stats = session.get_stats()
    end_time = datetime.now()

    # Prepare conversation data
    conversation_data = {
        "metadata": {
            "start_time": session.start_time.isoformat(),
            "end_time": end_time.isoformat(),
            "total_queries": session.total_queries,
            "total_tokens": session.total_tokens,
            "total_cost": session.total_cost,
        },
        "history": session.get_history(),
        "statistics": {
            "session_duration_seconds": int(stats["duration"].total_seconds()),
            "average_response_time": stats["average_response_time"],
            "queries_per_minute": (
                session.total_queries / (stats["duration"].total_seconds() / 60)
                if stats["duration"].total_seconds() > 0
                else 0.0
            ),
        },
    }

    # Write to JSON file
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(conversation_data, f, indent=2, ensure_ascii=False)

    logger.info(f"Conversation saved to: {filepath}")

    return filepath


def main() -> int:
    """
    Main CLI loop.

    Returns:
        Exit code (0 for success)
    """
    # Parse arguments
    args = parse_arguments()

    # Configure logging
    log_level = getattr(logging, args.log_level)
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    # Initialize RAG pipeline
    try:
        logger.info("Initializing RAG pipeline...")
        initialize_rag_pipeline()  # Initialize for module-level caching
        logger.info("RAG pipeline initialized successfully")
    except Exception as e:
        print(f"❌ Failed to initialize RAG pipeline: {e}")
        logger.error(f"Pipeline initialization failed: {e}", exc_info=True)
        return 1

    # Create conversation session
    session = ConversationSession()

    # Display welcome message
    display_welcome()

    # Main loop
    while True:
        try:
            # Get user input
            user_input = input(">>> ").strip()

            # Skip empty input
            if not user_input:
                continue

            # Handle special commands
            if user_input.startswith("/"):
                signal = handle_special_command(user_input, session)
                if signal == "exit":
                    break
                continue

            # Process RAG query
            try:
                process_query(
                    query=user_input,
                    session=session,
                    debug=args.debug,
                    streaming=not args.no_streaming,
                )
            except Exception as e:
                handle_error(e, debug=args.debug)

        except KeyboardInterrupt:
            print()
            print("Interrupted by user")
            break

    # Save conversation history if enabled
    if args.save_history and session.total_queries > 0:
        try:
            filepath = save_conversation_history(session)
            print(f"Conversation saved to: {filepath}")
        except Exception as e:
            logger.error(f"Failed to save conversation: {e}", exc_info=True)

    # Display goodbye message with stats
    print()
    print("Goodbye! Session summary:")
    print(f"  Queries: {session.total_queries}")
    print(f"  Tokens: {session.total_tokens:,}")
    print(f"  Cost: ${session.total_cost:.2f}")
    print()

    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print()
        print("Exiting CLI...")
        sys.exit(0)
