"""
Prompt construction for LLM Enhancement Service
Builds system and user prompts with HTS context and rules
"""

import logging
from typing import Dict, List, Optional, Any

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
        # rules: Optional[List[Dict]] = None,
        rules: Optional[List] = None,
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

        # Add HTS code if available
        if hasattr(product, "final_hts") and product.final_hts:
            prompt_parts.append(f"HTS Code: {product.final_hts}")

        # HTS Contect
        if hts_context:
            # Check if found key exists and is True, or if hierarchy_path exists
            if hts_context.get("found") or hts_context.get("hierarchy_path"):
                hierarchy_text = self._format_hts_hierarchy(
                    hts_context.get("hierarchy_path", [])
                )
                if hierarchy_text:
                    prompt_parts.append(f"\n{hierarchy_text}")

        # Rules
        if rules:
            rules_text = self._format_rules_from_objects(rules)
            if rules_text:
                prompt_parts.append(f"\n{rules_text}")

        return "\n\n".join(prompt_parts)

    def _format_rules_from_objects(self, rules: List) -> str:
        """
        Format rules from objects for prompt

        Args:
            rules: List of rule objects

        Returns:
            Formatted rules section string
        """
        if not rules:
            logger.debug("No rules provided")
            return ""

        rules_lines = ["Rules to Apply:"]

        for rule in rules:
            # Handle both Rule objects and dictionaries
            if hasattr(rule, "rule_id"):
                rule_id = rule.rule_id
                rule_content = rule.rule_content
            elif isinstance(rule, dict):
                # Dictionary
                rule_id = rule.get("rule_id", "UNKNOWN")
                rule_content = rule.get("rule_content", "")
            else:
                logger.warning(f"Unknown rule type: {type(rule)}")
                continue

            rules_lines.append(f"- [{rule_id}] {rule_content}")

        logger.debug(f"Formatted {len(rules)} rules")
        return "\n".join(rules_lines)

    def _format_hts_hierarchy(self, hierarchy_path: List[Dict]) -> str:
        """
        Format HTS hierarchy path for prompt

        Args:
            hierarchy_path: List of hierarchy level dictionaries

        Returns:
            Formatted HTS hierarchy section string
        """
        if not hierarchy_path:
            logger.debug("No HTS hierarchy path provided")
            return ""

        hts_lines = ["HTS Classification Context:"]

        for level in hierarchy_path:
            # Each levle is a dictionary with 'indent', 'code', 'description'
            indent_value = level.get("indent", 0)
            indent_str = "  " * indent_value  # 2 spaces per indent level
            code = level.get("code", "")
            description = level.get("description", "")
            hts_lines.append(f"{indent_str}[{code}] {description}")

        logger.debug(f"Formatted HTS hierarchy: {len(hierarchy_path)} levels")
        return "\n".join(hts_lines)

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
