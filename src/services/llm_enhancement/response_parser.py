"""
Response parsing and validation for LLM Enhancement Service
Handles JSON extraction, validation, and flattening for database storage
"""

import json
import re
import logging
from typing import Dict, Optional

from .models import LLMResponse, ExtractedFeatures

logger = logging.getLogger(__name__)


class ResponseParser:
    """Parses and validates LLM responses"""

    def __init__(self):
        logger.debug("ResponseParser initialized")

    def extract_json_from_response(self, llm_response: str) -> dict:
        """
        Extract JSON from LLM response, handling various formats

        LLM might return:
        - Pure JSON: {"enhanced_description": ...}
        - Markdown wrapped: ```json\n{...}\n```
        - Text before/after JSON

        Args:
            llm_response: Raw LLM response text

        Returns:
            Parsed dictionary

        Raises:
            ValueError: If no valid JSON found
        """
        logger.debug(f"Extracting JSON from response ({len(llm_response)} chars)")

        # Try direct parsing first
        try:
            parsed = json.loads(llm_response.strip())
            logger.debug("Direct JSON parsing successful")
            return parsed
        except json.JSONDecodeError:
            logger.debug("Direct parsing failed, trying extraction methods")

        # Try extracting from markdown code block
        markdown_pattern = r"```json\s*(\{.*?\})\s*```"
        match = re.search(markdown_pattern, llm_response, re.DOTALL)
        if match:
            try:
                parsed = json.loads(match.group(1))
                logger.debug("Extracted JSON from markdown block")
                return parsed
            except json.JSONDecodeError:
                logger.debug("Markdown extraction failed")

        # Try finding JSON object in text
        json_pattern = r"\{[^{}]*(?:\{[^{}]*\}[^{}]*)*\}"
        match = re.search(json_pattern, llm_response, re.DOTALL)
        if match:
            try:
                parsed = json.loads(match.group(0))
                logger.debug("Extracted JSON from text")
                return parsed
            except json.JSONDecodeError:
                logger.debug("Pattern extraction failed")

        logger.error("Could not extract valid JSON from LLM response")
        raise ValueError("Could not extract valid JSON from LLM response")

    def validate_llm_response(self, parsed: dict, item_id: str) -> dict:
        """
        Validate LLM response structure and content

        Args:
            parsed: Parsed JSON dictionary
            item_id: Item ID for logging

        Returns:
            Validated and normalized response dict

        Raises:
            ValueError: If validation fails critically
        """
        logger.debug(f"Validating response for {item_id}")

        errors = []
        warnings = []

        # Check top-level required fields
        required_top_level = [
            "enhanced_description",
            "confidence_score",
            "confidence_level",
            "extracted_features",
        ]
        for field in required_top_level:
            if field not in parsed:
                errors.append(f"Missing required field: {field}")

        if errors:
            error_msg = f"Validation failed for {item_id}: {'; '.join(errors)}"
            logger.error(error_msg)
            raise ValueError(error_msg)

        # Validate enhanced_description
        if (
            not parsed["enhanced_description"]
            or len(parsed["enhanced_description"].strip()) == 0
        ):
            errors.append("enhanced_description cannot be empty")

        # Validate confidence_score
        try:
            score = float(parsed["confidence_score"])
            if not (0.0 <= score <= 1.0):
                warnings.append(
                    f"Confidence score {score} out of range, clamping to [0, 1]"
                )
                score = max(0.0, min(1.0, score))
            parsed["confidence_score"] = (
                f"{score:.2f}"  # Normalize to string with 2 decimals
            )
        except (ValueError, TypeError):
            errors.append(f"Invalid confidence_score: {parsed.get('confidence_score')}")

        # Validate confidence_level
        valid_levels = ["Low", "Medium", "High"]
        if parsed["confidence_level"] not in valid_levels:
            warnings.append(
                f"Invalid confidence_level '{parsed['confidence_level']}', defaulting to Medium"
            )
            parsed["confidence_level"] = "Medium"

        # Validate extracted_features
        if not isinstance(parsed["extracted_features"], dict):
            errors.append("extracted_features must be an object")
        else:
            # Check required nested field: product
            product = parsed["extracted_features"].get("product")
            if not product or (isinstance(product, str) and len(product.strip()) == 0):
                errors.append(
                    "extracted_features.product is required and cannot be empty"
                )

            # Normalize nullable fields to None
            for field in ["customer_name", "dimensions"]:
                value = parsed["extracted_features"].get(field)
                if value == "" or value == "null" or value is None:
                    parsed["extracted_features"][field] = None

        if errors:
            error_msg = f"Validation failed for {item_id}: {'; '.join(errors)}"
            logger.error(error_msg)
            raise ValueError(error_msg)

        # Log warnings
        for warning in warnings:
            logger.warning(f"[{item_id}] {warning}")

        logger.debug(f"Validation passed for {item_id}")
        return parsed

    def flatten_for_database(
        self, parsed: dict, item_id: str, rules_applied: List[Dict], pass_number: int
    ) -> dict:
        """
        Flatten nested LLM response for database storage

        Args:
            parsed: Validated LLM response
            item_id: Item ID
            rules_applied: List of rules that were applied
            pass_number: Current pass number

        Returns:
            Dict matching UpdateProcessingInput schema from Service 1
        """
        logger.debug(f"Flattening response for database: {item_id}")

        flattened = {
            "enhanced_description": parsed["enhanced_description"],
            "confidence_score": parsed["confidence_score"],
            "confidence_level": parsed["confidence_level"],
            "extracted_customer_name": parsed["extracted_features"].get(
                "customer_name"
            ),
            "extracted_dimensions": parsed["extracted_features"].get("dimensions"),
            "extracted_product": parsed["extracted_features"]["product"],
            "rules_applied": json.dumps(
                [r.get("rule_id", r.get("id", "")) for r in rules_applied]
            ),
            "pass_number": str(pass_number),
        }

        logger.debug(f"Flattened {len(flattened)} fields for {item_id}")
        return flattened

    def calculate_fallback_confidence(
        self,
        enhanced_description: str,
        extracted_features: dict,
        hts_context: Optional[dict],
        original_description: str,
    ) -> tuple[str, str]:
        """
        Calculate confidence score and level using rule-based fallback

        Based on legacy system scoring with adjustments

        Args:
            enhanced_description: Enhanced product description
            extracted_features: Extracted features dict
            hts_context: HTS context (may be None)
            original_description: Original product description

        Returns:
            (confidence_score, confidence_level) as strings
        """
        logger.debug("Calculating fallback confidence")

        score = 0.0
        max_score = 10.0

        # Feature extraction scoring (more weight)
        if extracted_features.get("product"):
            score += 3.0
        if extracted_features.get("dimensions"):
            score += 3.0
        if extracted_features.get("customer_name"):
            score += 1.5

        # Enhancement quality scoring
        if len(enhanced_description) > len(original_description):
            score += 1.5  # Description was actually enhanced

        # Basic parsing bonus
        if len(extracted_features) > 0:
            score += 1.0

        # HTS context scoring (reduced importance)
        if hts_context and hts_context.get("hierarchy_path"):
            score += 0.5
            if len(hts_context["hierarchy_path"]) >= 3:
                score += 0.5  # Deep hierarchy context

        # Normalize to 0-1 scale
        confidence_score = min(score / max_score, 1.0)

        # Determine confidence level (adjusted thresholds from legacy)
        if confidence_score >= 0.7:
            confidence_level = "High"
        elif confidence_score >= 0.4:
            confidence_level = "Medium"
        else:
            confidence_level = "Low"

        logger.debug(
            f"Fallback confidence calculated: {confidence_score:.2f} ({confidence_level})"
        )
        logger.debug(f"Score breakdown: {score}/{max_score}")

        return (f"{confidence_score:.2f}", confidence_level)
