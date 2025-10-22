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
        Build user prompt for LLM

        Args:
            product: Product object with description and metadata
            hts_context: Optional HTS hierarchy context
            rules: Optional list of rules to apply

        Returns:
            Formatted user prompt string
        """
        prompt_parts = []

        # Product description
        prompt_parts.append(f"Original Description: {product.item_description}")

        # Additional context
        if product.material_detail:
            prompt_parts.append(f"Material Detail: {product.material_detail}")

        if product.product_group:
            prompt_parts.append(f"Product Group: {product.product_group}")

        # HTS Context
        if hts_context and hts_context.get("found"):
            hierarchy_text = self._format_hts_hierarchy(
                hts_context.get("hierarchy_path", [])
            )
            if hierarchy_text:
                prompt_parts.append(f"\nHTS Classification Context:\n{hierarchy_text}")

        # RUles
        if rules:
            from src.services.rules import RuleManager

            rule_manager = RuleManager()
            rules_text = rule_manager.format_rules_for_prompt(rules)
            if rules_text:
                prompt_parts.append(f"\n{rules_text}")

        return "\n\n".join(prompt_parts)

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
            # Use the indent value directly from the hierarchy
            indent_value = level.get("indent", 0)
            indent_str = "  " * indent_value  # 2 spaces per indent level
            code = level.get("code", "")
            description = level.get("description", "")
            hts_lines.append(f"{indent_str}[{code}] {description}")

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
