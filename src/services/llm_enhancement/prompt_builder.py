"""
Prompt construction for LLM Enhancement Service
Builds system and user prompts with HTS context and rules
"""

import logging
from typing import Dict, List, Optional, Any

from pydantic.types import AnyItemType

logger = logging.getLogger(__name__)


class PromptBuilder:
    """Build prompts for OpenAI API calls"""

    def __init__(self, system_prompt: str):
        """
        Initialize Prompt builder

        Args:
            system_prompt: The constant system prompt
        """
        self.system_prompt = system_prompt
        logger.debug("PromptBuilder initialized")

    def build_user_prompt(
        self,
        product: Any,
        hts_context: Optional[Dict] = None,
        rules: Optional[List[Dict]] = None,
    ) -> str:
        """
        Build user prompt for a product

        Args:
            product: Product record from Service 1
            hts_context: HTS hierarchy context from Service 2
            rules: List of rules to apply (Pass 2+ only)

        Returns:
            Formatted user prompt string
        """
        logger.debug(f"Building prompt for product: {product['item_id']}")

        # Format HTS Hierarchy section
        hts_section = self._format_hts_hierarchy(hts_context)

        # Format rules section
        rules_section = self._format_rules(rules)

        # Build complete prompt
        prompt = f"""Product Information:
- Original Description: {product.item_description}
- Material: {product.material_detail or 'N/A'}
- Product Group: {product.product_group or 'N/A'}
- HTS Code: {product.final_hts}

{hts_section}{rules_section}
Task: Enhance the product description following the guidelines and JSON format specified in the system prompt."""

        logger.debug(f"Prompt build: {len(prompt)} characters")
        return prompt

    def _format_hts_hierarchy(self, hts_context: Optional[Dict]) -> str:
        """
        Format HTS hierarchy context for prompt

        Args:
            hts_context: HTS hierarchy context from Service 2

        Returns:
            Formatted HTS hierarchy section string
        """
        if not hts_context or not hts_context.get("hierarchy_path"):
            logger.debug("No HTS context provided")
            return ""

        hierarchy_path = hts_context.get("hierarchy_path", [])
        if not hierarchy_path:
            return ""

        hts_lines = ["HTS Classification Context:"]

        for level in hierarchy_path:
            indent = " " * level.get("indent", 0)
            code = level.get("code", "")
            description = level.get("description", "")
            hts_lines.append(f"{indent}[{code}] {description}")

        logger.debug(f"Formatted HTS hierarchy: {len(hierarchy_path)} levels")
        return "\n".join(hts_lines) + "\n"

    def _format_rules(self, rules: Optional[List[Dict]]) -> str:
        """
        Format rules for prompt

        Args:
            rules: List of rule dictionaries from Service 4

        Returns:
            Formatted rules section string
        """
        if not rules:
            logger.debug("No rules provided")
            return ""

        rules_lines = ["\nRules to Apply:"]

        for rule in rules:
            rule_content = rule.get("rule_content", rule.get("pattern", ""))
            if rule_content:
                rules_lines.append(f"- {rule_content}")

        logger.debug(f"Formatted {len(rules)} rules")
        return "\n".join(rules_lines) + "\n"

    def get_system_prompt(self) -> str:
        """
        Get the system prompt

        Returns:
            The system prompt string
        """
        return self.system_prompt
