"""
Unit tests for LLM Enhancement Service (Service 3)
"""

import pytest
import json
import tempfile
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock

from src.services.llm_enhancement.models import (
    LLMResponse,
    ExtractedFeatures,
    BatchResult,
    ProductResult,
)
from src.services.llm_enhancement.prompt_builder import PromptBuilder
from src.services.llm_enhancement.response_parser import ResponseParser
from src.services.llm_enhancement.api_client import OpenAIClient
from src.services.llm_enhancement.config import SYSTEM_PROMPT


# ============= TEST PROMPT BUILDER =============


class TestPromptBuilder:
    """Test prompt construction"""

    def test_system_prompt_format(self):
        """Test system prompt is properly formatted"""
        builder = PromptBuilder(SYSTEM_PROMPT)

        system_prompt = builder.get_system_prompt()

        assert "product description enhancement" in system_prompt.lower()
        assert "JSON" in system_prompt
        assert "enhanced_description" in system_prompt
        assert "confidence_score" in system_prompt
        assert "extracted_features" in system_prompt

    def test_user_prompt_basic(self):
        """Test basic user prompt construction"""
        builder = PromptBuilder(SYSTEM_PROMPT)

        # Mock product
        product = Mock()
        product.item_id = "ITEM001"
        product.item_description = "DI SPACER 18 INCH"
        product.material_detail = "Ductile Iron"
        product.product_group = "FITTINGS"
        product.final_hts = "7307.19.30.60"

        prompt = builder.build_user_prompt(product, hts_context=None, rules=None)

        assert "ITEM001" not in prompt  # Item ID not in prompt
        assert "DI SPACER 18 INCH" in prompt
        assert "Ductile Iron" in prompt
        assert "FITTINGS" in prompt
        assert "7307.19.30.60" in prompt
        assert "Product Information:" in prompt

    def test_user_prompt_with_hts_context(self):
        """Test prompt with HTS hierarchy context"""
        builder = PromptBuilder(SYSTEM_PROMPT)

        product = Mock()
        product.item_id = "ITEM001"
        product.item_description = "Test product"
        product.material_detail = "Steel"
        product.product_group = "TEST"
        product.final_hts = "7307.19.30.60"

        hts_context = {
            "hierarchy_path": [
                {"code": "7307", "description": "Tube or pipe fittings", "indent": 0},
                {"code": "7307.19", "description": "Other fittings", "indent": 1},
                {
                    "code": "7307.19.30.60",
                    "description": "Specific fitting",
                    "indent": 3,
                },
            ]
        }

        prompt = builder.build_user_prompt(product, hts_context=hts_context, rules=None)

        assert "HTS Classification Context:" in prompt
        assert "7307" in prompt
        assert "Tube or pipe fittings" in prompt
        assert "7307.19.30.60" in prompt

    def test_user_prompt_with_rules(self):
        """Test prompt with rules"""
        builder = PromptBuilder(SYSTEM_PROMPT)

        product = Mock()
        product.item_id = "ITEM001"
        product.item_description = "Test product"
        product.material_detail = "Steel"
        product.product_group = "TEST"
        product.final_hts = "7307.19.30.60"

        rules = [
            {"rule_id": "R001", "rule_content": "Expand DI to Ductile Iron"},
            {"rule_id": "R002", "rule_content": "Include manufacturer name if present"},
        ]

        prompt = builder.build_user_prompt(product, hts_context=None, rules=rules)

        assert "Rules to Apply:" in prompt
        assert "Expand DI to Ductile Iron" in prompt
        assert "Include manufacturer name" in prompt

    def test_user_prompt_without_rules(self):
        """Test prompt without rules (Pass 1)"""
        builder = PromptBuilder(SYSTEM_PROMPT)

        product = Mock()
        product.item_id = "ITEM001"
        product.item_description = "Test product"
        product.material_detail = "Steel"
        product.product_group = "TEST"
        product.final_hts = "7307.19.30.60"

        prompt = builder.build_user_prompt(product, hts_context=None, rules=None)

        assert "Rules to Apply:" not in prompt

    def test_hts_hierarchy_formatting(self):
        """Test HTS hierarchy formatting with indentation"""
        builder = PromptBuilder(SYSTEM_PROMPT)

        hts_context = {
            "hierarchy_path": [
                {"code": "73", "description": "Articles of iron or steel", "indent": 0},
                {"code": "7307", "description": "Fittings", "indent": 1},
                {"code": "7307.19", "description": "Other", "indent": 2},
                {"code": "7307.19.30.60", "description": "Specific", "indent": 3},
            ]
        }

        formatted = builder._format_hts_hierarchy(hts_context)

        # Debug: Print the formatted output
        print("\n=== FORMATTED OUTPUT ===")
        print(repr(formatted))
        print("\n=== LINES ===")
        for i, line in enumerate(formatted.split("\n")):
            print(f"Line {i}: {repr(line)}")

        # First line shoudl be the header
        assert "HTS Classification Context:" in formatted

        # Check that all codes are present
        assert "[73]" in formatted
        assert "[7307]" in formatted
        assert "[7307.19]" in formatted
        assert "[7307.19.30.60]" in formatted

        # Check identation by looking at the actual lines
        lines = formatted.split("\n")

        # Find lines with codes and verify indentation
        for line in lines:
            if "[73]" in line:
                # indent 0 - should not start with spaces
                assert not line.startswith(
                    " "
                ), f"Line with [73] should not be indented: {repr(line)}"
            elif "[7307]" in line:
                # indent 1 - should start with 2 spaces
                assert line.startswith("  ") and not line.startswith(
                    "    "
                ), f"Line with [7307] should have 2 spaces: {repr(line)}"
            elif "[7307.19]" in line and "[7307.19.30.60]" not in line:
                # indent 2 - should start with 4 spaces
                assert line.startswith("    ") and not line.startswith(
                    "      "
                ), f"Line with [7307.19] should have 4 spaces: {repr(line)}"
            elif "[7307.19.30.60]" in line:
                # indent 3 - should start with 6 spaces
                assert line.startswith(
                    "      "
                ), f"Line with [7307.19.30.60] should have 6 spaces: {repr(line)}"


# ============= TEST RESPONSE PARSER =============


class TestResponseParser:
    """Test JSON parsing and validation"""

    def test_parse_valid_json(self):
        """Test parsing valid pure JSON"""
        parser = ResponseParser()

        response = json.dumps(
            {
                "enhanced_description": "Ductile iron spacer, 18-inch diameter",
                "confidence_score": "0.85",
                "confidence_level": "High",
                "extracted_features": {
                    "customer_name": "Smith Blair",
                    "dimensions": "18 inch",
                    "product": "Spacer",
                },
            }
        )

        parsed = parser.extract_json_from_response(response)

        assert parsed["enhanced_description"] == "Ductile iron spacer, 18-inch diameter"
        assert parsed["confidence_score"] == "0.85"
        assert parsed["extracted_features"]["product"] == "Spacer"

    def test_parse_json_with_markdown(self):
        """Test parsing JSON wrapped in markdown code block"""
        parser = ResponseParser()

        response = """```json
{
    "enhanced_description": "Test description",
    "confidence_score": "0.75",
    "confidence_level": "Medium",
    "extracted_features": {
        "customer_name": null,
        "dimensions": "10 inch",
        "product": "Fitting"
    }
}
```"""

        parsed = parser.extract_json_from_response(response)

        assert parsed["enhanced_description"] == "Test description"
        assert parsed["confidence_score"] == "0.75"
        assert parsed["extracted_features"]["product"] == "Fitting"

    def test_parse_json_with_extra_text(self):
        """Test parsing JSON with text before/after"""
        parser = ResponseParser()

        response = """Here is the enhanced description:
{
    "enhanced_description": "Test description",
    "confidence_score": "0.80",
    "confidence_level": "High",
    "extracted_features": {
        "customer_name": null,
        "dimensions": null,
        "product": "Article"
    }
}
I hope this helps!"""

        parsed = parser.extract_json_from_response(response)

        assert parsed["enhanced_description"] == "Test description"
        assert parsed["confidence_level"] == "High"

    def test_parse_invalid_json_raises_error(self):
        """Test that invalid JSON raises ValueError"""
        parser = ResponseParser()

        response = "This is not JSON at all"

        with pytest.raises(ValueError, match="Could not extract valid JSON"):
            parser.extract_json_from_response(response)

    def test_validate_complete_response(self):
        """Test validation of complete valid response"""
        parser = ResponseParser()

        parsed = {
            "enhanced_description": "Complete description",
            "confidence_score": "0.90",
            "confidence_level": "High",
            "extracted_features": {
                "customer_name": "Test Co",
                "dimensions": "20 inch",
                "product": "Valve",
            },
        }

        validated = parser.validate_llm_response(parsed, "ITEM001")

        assert validated["enhanced_description"] == "Complete description"
        assert validated["confidence_score"] == "0.90"
        assert validated["confidence_level"] == "High"
        assert validated["extracted_features"]["product"] == "Valve"

    def test_validate_missing_required_field(self):
        """Test validation fails on missing required field"""
        parser = ResponseParser()

        parsed = {
            "enhanced_description": "Test",
            "confidence_score": "0.80",
            # Missing confidence_level
            "extracted_features": {
                "customer_name": None,
                "dimensions": None,
                "product": "Test",
            },
        }

        with pytest.raises(ValueError, match="Missing required field"):
            parser.validate_llm_response(parsed, "ITEM001")

    def test_validate_invalid_confidence_score(self):
        """Test validation handles invalid confidence score"""
        parser = ResponseParser()

        parsed = {
            "enhanced_description": "Test",
            "confidence_score": "invalid",
            "confidence_level": "High",
            "extracted_features": {
                "customer_name": None,
                "dimensions": None,
                "product": "Test",
            },
        }

        with pytest.raises(ValueError, match="Invalid confidence_score"):
            parser.validate_llm_response(parsed, "ITEM001")

    def test_validate_missing_extracted_product(self):
        """Test validation fails when extracted_product missing"""
        parser = ResponseParser()

        parsed = {
            "enhanced_description": "Test",
            "confidence_score": "0.80",
            "confidence_level": "High",
            "extracted_features": {
                "customer_name": "Test",
                "dimensions": "10 inch",
                # Missing product
            },
        }

        with pytest.raises(ValueError, match="product is required"):
            parser.validate_llm_response(parsed, "ITEM001")

    def test_validate_clamps_out_of_range_score(self):
        """Test that out-of-range scores are clamped"""
        parser = ResponseParser()

        parsed = {
            "enhanced_description": "Test",
            "confidence_score": "1.5",  # Out of range
            "confidence_level": "High",
            "extracted_features": {
                "customer_name": None,
                "dimensions": None,
                "product": "Test",
            },
        }

        validated = parser.validate_llm_response(parsed, "ITEM001")

        # Should be clamped to 1.0
        assert float(validated["confidence_score"]) <= 1.0

    def test_validate_invalid_confidence_level(self):
        """Test invalid confidence level defaults to Medium"""
        parser = ResponseParser()

        parsed = {
            "enhanced_description": "Test",
            "confidence_score": "0.80",
            "confidence_level": "VeryHigh",  # Invalid
            "extracted_features": {
                "customer_name": None,
                "dimensions": None,
                "product": "Test",
            },
        }

        validated = parser.validate_llm_response(parsed, "ITEM001")

        assert validated["confidence_level"] == "Medium"

    def test_flatten_for_database(self):
        """Test flattening nested response for database"""
        parser = ResponseParser()

        parsed = {
            "enhanced_description": "Test description",
            "confidence_score": "0.85",
            "confidence_level": "High",
            "extracted_features": {
                "customer_name": "Test Co",
                "dimensions": "15 inch",
                "product": "Fitting",
            },
        }

        rules = [{"rule_id": "R001"}, {"rule_id": "R002"}]

        flattened = parser.flatten_for_database(parsed, "ITEM001", rules, pass_number=1)

        assert flattened["enhanced_description"] == "Test description"
        assert flattened["confidence_score"] == "0.85"
        assert flattened["confidence_level"] == "High"
        assert flattened["extracted_customer_name"] == "Test Co"
        assert flattened["extracted_dimensions"] == "15 inch"
        assert flattened["extracted_product"] == "Fitting"
        assert flattened["rules_applied"] == '["R001", "R002"]'
        assert flattened["pass_number"] == "1"

    def test_calculate_fallback_confidence(self):
        """Test fallback confidence calculation"""
        parser = ResponseParser()

        extracted_features = {
            "customer_name": "Test Co",
            "dimensions": "10 inch",
            "product": "Valve",
        }

        hts_context = {
            "hierarchy_path": [
                {"code": "7307", "description": "Fittings", "indent": 0},
                {"code": "7307.19", "description": "Other", "indent": 1},
            ]
        }

        score, level = parser.calculate_fallback_confidence(
            enhanced_description="Enhanced description longer than original",
            extracted_features=extracted_features,
            hts_context=hts_context,
            original_description="Short",
        )

        assert isinstance(score, str)
        assert level in ["Low", "Medium", "High"]
        assert 0.0 <= float(score) <= 1.0


# ============= TEST API CLIENT =============


class TestOpenAIClient:
    """Test OpenAI API client"""

    def test_client_initialization(self):
        """Test client initializes with API key"""
        # Test with mock mode
        with patch.dict("os.environ", {"MOCK_OPENAI": "true"}):
            client = OpenAIClient()
            assert client.mock_mode is True

    def test_mock_response_generation(self):
        """Test mock response generation"""
        with patch.dict("os.environ", {"MOCK_OPENAI": "true"}):
            client = OpenAIClient()

            response = client.call_api("Test prompt")

            # Should return valid JSON
            parsed = json.loads(response)
            assert "enhanced_description" in parsed
            assert "confidence_score" in parsed
            assert "extracted_features" in parsed

    @patch("src.services.llm_enhancement.api_client.OpenAI")
    def test_api_call_success(self, mock_openai_class):
        """Test successful API call"""
        # Mock the OpenAI client
        mock_client = MagicMock()
        mock_openai_class.return_value = mock_client

        # Mock the response
        mock_response = MagicMock()
        mock_response.choices = [MagicMock()]
        mock_response.choices[0].message.content = json.dumps(
            {
                "enhanced_description": "Test",
                "confidence_score": "0.80",
                "confidence_level": "High",
                "extracted_features": {
                    "customer_name": None,
                    "dimensions": None,
                    "product": "Test",
                },
            }
        )

        mock_client.chat.completions.create.return_value = mock_response

        with patch.dict("os.environ", {"MOCK_OPENAI": "false"}):
            client = OpenAIClient(api_key="test-key")
            response = client.call_api("Test prompt")

            assert "enhanced_description" in response


# ============= TEST BATCH PROCESSOR =============


class TestBatchProcessor:
    """Test batch processing logic"""

    @pytest.fixture
    def mock_db(self):
        """Mock database"""
        db = Mock()

        # Mock product
        product = Mock()
        product.item_id = "ITEM001"
        product.item_description = "Test product"
        product.material_detail = "Steel"
        product.product_group = "TEST"
        product.final_hts = "7307.19.30.60"

        db.get_unprocessed_products.return_value = [product]
        db.get_product_by_id.return_value = product
        db.update_processing_results.return_value = True

        return db

    @pytest.fixture
    def mock_hts_service(self):
        """Mock HTS service"""
        service = Mock()
        service.get_hts_context.return_value = {"found": True, "hierarchy_path": []}
        return service

    @pytest.fixture
    def mock_openai_client(self):
        """Mock OpenAI client"""
        client = Mock()
        client.call_api.return_value = json.dumps(
            {
                "enhanced_description": "Enhanced test product",
                "confidence_score": "0.85",
                "confidence_level": "High",
                "extracted_features": {
                    "customer_name": None,
                    "dimensions": None,
                    "product": "Test Product",
                },
            }
        )
        return client

    def test_process_batch_pass_1(self, mock_db, mock_hts_service, mock_openai_client):
        """Test Pass 1 batch processing"""
        from src.services.llm_enhancement.batch_processor import BatchProcessor

        processor = BatchProcessor(
            db=mock_db, hts_service=mock_hts_service, openai_client=mock_openai_client
        )

        result = processor.process_batch(batch_size=10, pass_number=1)

        assert result.pass_number == 1
        assert result.total_processed == 1
        assert result.successful == 1
        assert result.failed == 0
        assert result.success_rate == 1.0

    def test_process_batch_empty_products(self, mock_hts_service, mock_openai_client):
        """Test batch processing with no products"""
        from src.services.llm_enhancement.batch_processor import BatchProcessor

        mock_db = Mock()
        mock_db.get_unprocessed_products.return_value = []

        processor = BatchProcessor(
            db=mock_db, hts_service=mock_hts_service, openai_client=mock_openai_client
        )

        result = processor.process_batch(batch_size=10, pass_number=1)

        assert result.total_processed == 0
        assert result.successful == 0


# ============= TEST INTEGRATION =============


class TestIntegration:
    """Integration tests"""

    def test_full_pipeline_single_product(self):
        """Test complete pipeline with mock data"""
        from src.services.llm_enhancement.prompt_builder import PromptBuilder
        from src.services.llm_enhancement.response_parser import ResponseParser
        from src.services.llm_enhancement.config import SYSTEM_PROMPT

        # Mock product
        product = Mock()
        product.item_id = "ITEM001"
        product.item_description = "DI SPACER 18 INCH"
        product.material_detail = "Ductile Iron"
        product.product_group = "FITTINGS"
        product.final_hts = "7307.19.30.60"

        # Build prompt
        builder = PromptBuilder(SYSTEM_PROMPT)
        prompt = builder.build_user_prompt(product)

        assert "DI SPACER 18 INCH" in prompt

        # Mock LLM response
        llm_response = json.dumps(
            {
                "enhanced_description": "Ductile iron spacer, 18-inch diameter",
                "confidence_score": "0.85",
                "confidence_level": "High",
                "extracted_features": {
                    "customer_name": None,
                    "dimensions": "18 inch",
                    "product": "Spacer",
                },
            }
        )

        # Parse response
        parser = ResponseParser()
        parsed = parser.extract_json_from_response(llm_response)
        validated = parser.validate_llm_response(parsed, product.item_id)
        flattened = parser.flatten_for_database(validated, product.item_id, [], 1)

        assert (
            flattened["enhanced_description"] == "Ductile iron spacer, 18-inch diameter"
        )
        assert flattened["confidence_level"] == "High"
        assert flattened["extracted_product"] == "Spacer"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
