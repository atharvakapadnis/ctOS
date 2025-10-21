"""
Batch processing logic for LLM Enhancement Service
Handles Pass 1 (inital) and Pass 2+ (reprocessing with rules) execution
"""

import time
import logging
from typing import List, Optional, Dict, Any
from datetime import datetime

from .models import BatchResult, ProductResult, BatchConfig
from .prompt_builder import PromptBuilder
from .response_parser import ResponseParser
from .api_client import OpenAIClient
from .config import BATCH_SIZE_DEFAULT, SYSTEM_PROMPT

# Import services 1 and 2
from ..ingestion.database import ProductDatabase
from ..ingestion.models import UpdateProcessingInput
from ..hts_context.service import HTSContextService

logger = logging.getLogger(__name__)


class BatchProcessor:
    """Processes batches of products through LLM enhancement"""

    def __init__(
        self,
        db: Optional[ProductDatabase] = None,
        hts_service: Optional[HTSContextService] = None,
        openai_client: Optional[OpenAIClient] = None,
    ):
        """
        Initialize batch processor

        Args:
            db: ProductDatabase instance (Service 1)
            hts_service: HTSContextService instance (Service 2)
            openai_client: OpenAIClinet instance
        """
        self.db = db or ProductDatabase()
        self.hts_service = hts_service or HTSContextService()
        self.openai_client = openai_client or OpenAIClient()

        self.prompt_builder = PromptBuilder(SYSTEM_PROMPT)
        self.response_parser = ResponseParser()

        logger.info("BatchProcessor initialized with services 1 and 2")

    def process_batch(
        self,
        batch_size: int = BATCH_SIZE_DEFAULT,
        pass_number: int = 1,
        selected_item_ids: Optional[List[str]] = None,
    ) -> BatchResult:
        """
        Process a batch of products through LLM enhancement

        Args:
            batch_size: Number of products to process per batch
            pass_number: Current pass number
            selected_item_ids: Optional list of specific item IDs to process

        Returns:
            BatchResult containing processing statistics and results
        """
        logger.info(f"Starting Pass {pass_number} batch processing (size={batch_size})")

        # Step 1: Load products
        products = self._load_products(batch_size, pass_number, selected_item_ids)

        if not products:
            logger.warning("No products to process")
            return self._create_empty_batch_result(pass_number, batch_size)

        # Step 2: Load Rules only for Pass 2+
        rules = self._load_rules(pass_number)

        # Step 3: Process each product
        results = []
        successful = 0
        failed = 0
        confidence_distribution = {"Low": 0, "Medium": 0, "High": 0}

        start_time = time.time()

        for idx, product in enumerate(products, 1):
            try:
                logger.info(f"Processing [{idx}/{len(products)}]: {product.item_id}")

                # Get HTS Context
                hts_context = self._get_hts_context(product)

                # Build Prompt
                user_prompt = self.prompt_builder.build_user_prompt(
                    product, hts_context, rules
                )

                # Call OpenAI API with retry
                llm_response = self.openai_client.call_api(user_prompt)

                # Parse and validate response
                parsed = self.response_parser.extract_json_from_response(llm_response)
                validated = self.response_parser.validate_llm_response(
                    parsed, product.item_id
                )

                # Flatten for database
                db_update_dict = self.response_parser.flatten_for_database(
                    validated, product.item_id, rules, pass_number
                )

                # Convert dict to UpdateProcessing input
                db_update = UpdateProcessingInput(**db_update_dict)

                # Update database (crash sage)
                update_success = self._update_database(product.item_id, db_update)

                if not update_success:
                    raise RuntimeError(f"Database update failed for {product.item_id}")

                # Track statistics
                successful += 1
                confidence_distribution[validated["confidence_level"]] += 1

                # Log success
                logger.info(
                    f"Processed {product.item_id}: {validated['confidence_level']} confidence"
                )

                results.append(
                    ProductResult(
                        item_id=product.item_id,
                        success=True,
                        confidence_level=validated["confidence_level"],
                        confidence_score=validated["confidence_score"],
                    )
                )

            except Exception as e:
                failed += 1
                logger.error(f"Failed {product.item_id}: {str(e)}")

                results.append(
                    ProductResult(item_id=product.item_id, success=False, error=str(e))
                )

                # Continur to next product dont fail the batch
                continue

        processing_time = time.time() - start_time

        # Step 4: Create batch result
        batch_result = BatchResult(
            pass_number=pass_number,
            batch_size=batch_size,
            total_processed=len(products),
            successful=successful,
            failed=failed,
            success_rate=successful / len(products) if products else 0,
            confidence_distribution=confidence_distribution,
            processing_time=processing_time,
            avg_time_per_product=processing_time / len(products) if products else 0,
            results=results,
        )

        logger.info(
            f"Batch complete: {successful}/{len(products)} successful, ({batch_result.success_rate:.1%})"
        )
        logger.info(f"Confidence distribution: {confidence_distribution}")

        return batch_result

    def _load_products(
        self, batch_size: int, pass_number: int, selected_item_ids: Optional[List[str]]
    ) -> List[Any]:
        """Load products based on pass number"""
        if pass_number == 1:
            # Pass 1: Get unprocessed products
            products = self.db.get_unprocessed_products(limit=batch_size)
            logger.info(f"Loaded {len(products)} unprocessed products")
            return products
        else:
            # Pass 2+: Get selected products by ID
            if not selected_item_ids:
                raise ValueError("Pass 2+ requires selected_item_ids")

            products = []
            for item_id in selected_item_ids[:batch_size]:
                product = self.db.get_product_by_id(item_id)
                if product:
                    products.append(product)
            logger.info(f"Loaded {len(products)} selected products for reprocessing")
            return products

    def _load_rules(self, pass_number: int) -> List[Dict]:
        """Load rules for Pass 2+"""
        if pass_number == 1:
            logger.info("Pass 1: No rules loaded")
            return []

        # TODO: Integrate with Service 4 (Rules) when available
        # For now return empty list
        logger.warning("Service 4 (Rules) not yet integrated, returning empty rules")
        return []

    def _get_hts_context(self, product: Any) -> Optional[Dict]:
        """Get HTS context for a product"""
        try:
            hts_context = self.hts_service.get_hts_context(product.final_hts)
            return hts_context
        except Exception as e:
            logger.error(f"Failed to get HTS context for {product.final_hts}: {e}")
            return None

    def _update_database(self, item_id: str, db_update: Dict) -> bool:
        """Update database with processing results"""
        try:
            return self.db.update_processing_results(item_id, db_update)
        except Exception as e:
            logger.error(f"Database update failed for {item_id}: {e}")
            return False

    def _create_empty_batch_result(
        self, pass_number: int, batch_size: int
    ) -> BatchResult:
        """Create empty batch result for no products"""
        return BatchResult(
            pass_number=pass_number,
            batch_size=batch_size,
            total_processed=0,
            successful=0,
            failed=0,
            success_rate=0.0,
            confidence_distribution={"Low": 0, "Medium": 0, "High": 0},
            processing_time=0.0,
            avg_time_per_product=0.0,
            results=[],
        )


def process_batch(
    batch_size: int = BATCH_SIZE_DEFAULT,
    pass_number: int = 1,
    selected_item_ids: Optional[List[str]] = None,
) -> BatchResult:
    """
    Convenience function for processing a batch

    Args:
        batch_size: Number of products to process per batch
        pass_number: Current pass number (1 for initial, 2+ for reprocessing)
        selected_item_ids: For Pass 2+ to process specific items

    Returns:
        BatchResult containing processing statistics and results
    """
    processor = BatchProcessor()
    return processor.process_batch(batch_size, pass_number, selected_item_ids)


def resume_pass_1(batch_size: int = BATCH_SIZE_DEFAULT) -> Dict[str, Any]:
    """
    Resume Pass 1 processing from where it stopped

    Automatically detects unprocessed products and continues

    Args:
        batch_size: Number of products to process per batch

    Returns:
        Summary dictionary with overall statistics
    """
    logger.info(f"Resume Pass 1 processing...")

    # Initialize database
    db = ProductDatabase()

    # Check how many already processed
    stats = db.get_database_statistics()
    total_products = stats.total_products
    processed_count = stats.processed_count
    remaining = stats.unprocessed_count

    logger.info(
        f"Resuming Pass 1: {processed_count}/{total_products} already processed"
    )
    logger.info(f"Remaining: {remaining} products")

    if remaining == 0:
        logger.info("Pass 1 complete, no products to process")
        return {
            "status": "complete",
            "total_products": total_products,
            "processed_count": processed_count,
            "remaining": 0,
            "batches_processed": 0,
        }

    # Calculate batches needed
    batches_needed = (remaining + batch_size - 1) // batch_size
    logger.info(f"Processing {remaining} products in {batches_needed} batches")

    # Process batches
    processor = BatchProcessor(db=db)
    batch_count = (0,)
    total_successful = (0,)
    total_failed = (0,)
    overall_start = time.time()

    while True:
        batch_count += 1
        logger.info(f"Starting batch {batch_count}/{batches_needed}")

        # Process next batch
        result = processor.process_batch(
            batch_size=batch_size, pass_number=1, selected_item_ids=None
        )

        total_successful += result.successful
        total_failed += result.failed

        if result.total_processed == 0:
            logger.info("No more products to process, Pass 1 complete")
            break

        # Check if more batches needed
        stats = db.get_database_statistics()
        if stats.unprocessed_count == 0:
            logger.info("All products processed, Pass 1 complete")
            break

    overall_time = time.time() - overall_start

    summary = {
        "status": "complete",
        "total_products": total_products,
        "processed": total_successful,
        "failed": total_failed,
        "remaining": 0,
        "batches_processed": batch_count,
        "total_time": overall_time,
        "avg_time_per_batch": overall_time / batch_count if batch_count > 0 else 0,
    }

    logger.info(f"Pass 1 resume complete: {summary}")
    return summary
