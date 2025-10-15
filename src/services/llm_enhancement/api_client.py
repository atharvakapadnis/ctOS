"""
OpenAI API client with retry logic for LLM Enhancement Service
Handles API calls with exponential backoff for transient failures
"""

import time
import logging
from functools import wraps
from typing import Optional

try:
    import openai
    from openai import OpenAI
except ImportError:
    raise ImportError("openai package required. Install with: pip install openai")

from .config import (
    OPENAI_API_KEY,
    OPENAI_MODEL,
    OPENAI_TEMPERATURE,
    OPENAI_MAX_TOKENS,
    OPENAI_TIMEOUT,
    RETRY_MAX_ATTEMPTS,
    RETRY_BASE_DELAY,
    RETRY_EXPONENTIAL_BASE,
    MOCK_OPENAI,
    SYSTEM_PROMPT,
)

logger = logging.getLogger(__name__)


def exponential_backoff_retry(
    max_attempts: int = RETRY_MAX_ATTEMPTS, base_delay: int = RETRY_BASE_DELAY
):
    """
    Decorator for exponential backoff retry logic

    Args:
        max_attempts: Maximum number of retry attempts
        base_delay: Base delay in seconds (doubles each retry)
    """

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None

            for attempt in range(1, max_attempts + 1):
                try:
                    return func(*args, **kwargs)

                except openai.APITimeoutError as e:
                    last_exception = e
                    if attempt < max_attempts:
                        delay = base_delay * (RETRY_EXPONENTIAL_BASE ** (attempt - 1))
                        logger.warning(
                            f"Timeout on attempt {attempt}/{max_attempts}, retrying in {delay}s..."
                        )
                        time.sleep(delay)
                    else:
                        logger.error(f"Max retries exceeded after timeout")
                        raise

                except openai.RateLimitError as e:
                    last_exception = e
                    if attempt < max_attempts:
                        delay = base_delay * (RETRY_EXPONENTIAL_BASE ** (attempt - 1))
                        logger.warning(
                            f"Rate limit hit on attempt {attempt}/{max_attempts}, waiting {delay}s..."
                        )
                        time.sleep(delay)
                    else:
                        logger.error(f"Max retries exceeded for rate limit")
                        raise

                except openai.APIError as e:
                    last_exception = e
                    # Check if it's a server error (500-599)
                    if hasattr(e, "status_code") and 500 <= e.status_code < 600:
                        if attempt < max_attempts:
                            delay = base_delay * (
                                RETRY_EXPONENTIAL_BASE ** (attempt - 1)
                            )
                            logger.warning(
                                f"API error on attempt {attempt}/{max_attempts}, retrying in {delay}s..."
                            )
                            time.sleep(delay)
                        else:
                            logger.error(f"Max retries exceeded for API error")
                            raise
                    else:
                        # Non-server error, don't retry
                        logger.error(f"Non-retryable API error: {type(e).__name__}")
                        raise

                except (openai.AuthenticationError, openai.BadRequestError) as e:
                    # Don't retry authentication or bad request errors
                    logger.error(f"Non-retryable error: {type(e).__name__}: {str(e)}")
                    raise

                except Exception as e:
                    # Catch-all for unexpected errors
                    logger.error(f"Unexpected error: {type(e).__name__}: {str(e)}")
                    raise

            # Should not reach here
            raise last_exception

        return wrapper

    return decorator


class OpenAIClient:
    """OpenAI API client with retry logic"""

    def __init__(self, api_key: Optional[str] = None):
        """
        Initialize OpenAI client

        Args:
            api_key: OpenAI API key (uses config default if not provided)
        """
        self.api_key = api_key or OPENAI_API_KEY
        self.model = OPENAI_MODEL
        self.temperature = OPENAI_TEMPERATURE
        self.max_tokens = OPENAI_MAX_TOKENS
        self.timeout = OPENAI_TIMEOUT
        self.mock_mode = MOCK_OPENAI

        if not self.mock_mode:
            if not self.api_key:
                raise ValueError(
                    "OpenAI API key is required. Set OPENAI_API_KEY environment variable."
                )

            self.client = OpenAI(api_key=self.api_key)
            logger.info(f"OpenAI client initialized (model={self.model})")
        else:
            self.client = None
            logger.warning("OpenAI client in MOCK MODE - using mock responses")

    @exponential_backoff_retry(
        max_attempts=RETRY_MAX_ATTEMPTS, base_delay=RETRY_BASE_DELAY
    )
    def call_api(self, user_prompt: str, system_prompt: str = SYSTEM_PROMPT) -> str:
        """
        Call OpenAI API with retry logic

        Args:
            user_prompt: User prompt string
            system_prompt: System prompt (uses default if not provided)

        Returns:
            LLM response text

        Raises:
            openai.OpenAIError: After max retries or non-retryable errors
        """
        if self.mock_mode:
            return self._mock_response(user_prompt)

        logger.debug(f"Calling OpenAI API (model={self.model})")

        try:
            response = self.client.chat.completions.create(
                model=self.model,
                temperature=self.temperature,
                max_tokens=self.max_tokens,
                timeout=self.timeout,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt},
                ],
            )

            # Extract response text
            content = response.choices[0].message.content

            logger.debug(f"API response received ({len(content)} characters)")

            return content

        except Exception as e:
            logger.error(f"OpenAI API call failed: {type(e).__name__}: {str(e)}")
            raise

    def _mock_response(self, user_prompt: str) -> str:
        """
        Generate mock response for testing

        Args:
            user_prompt: User prompt (used to extract product info)

        Returns:
            Mock JSON response string
        """
        logger.debug("Generating MOCK OpenAI response")

        # Extract item description from prompt if possible
        import re

        desc_match = re.search(r"Original Description: (.+)", user_prompt)
        original_desc = desc_match.group(1) if desc_match else "Mock Product"

        mock_response = {
            "enhanced_description": f"Enhanced mock description based on: {original_desc[:50]}...",
            "confidence_score": "0.75",
            "confidence_level": "Medium",
            "extracted_features": {
                "customer_name": "Mock Customer",
                "dimensions": "10 inch",
                "product": "Mock Product Type",
            },
        }

        import json

        return json.dumps(mock_response)


# Convenience function for backward compatibility
@exponential_backoff_retry(max_attempts=RETRY_MAX_ATTEMPTS, base_delay=RETRY_BASE_DELAY)
def call_openai_api_with_retry(
    user_prompt: str, system_prompt: str = SYSTEM_PROMPT
) -> str:
    """
    Call OpenAI API with retry logic (convenience function)

    Args:
        user_prompt: User prompt string
        system_prompt: System prompt

    Returns:
        LLM response text

    Raises:
        openai.OpenAIError: After max retries or non-retryable errors
    """
    client = OpenAIClient()
    return client.call_api(user_prompt, system_prompt)
