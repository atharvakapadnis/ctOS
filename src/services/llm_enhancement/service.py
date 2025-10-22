"""
LLM Enhancement Service - Main orchestration service
High-level API for product description enhancement
"""

import logging
from typing import Optional, List, Dict, Any
from pathlib import Path

from .models import BatchResult, BatchConfig
from .batch_processor import (
    BatchProcessor,
    process_batch as batch_process,
    resume_pass_1,
)
from .api_client import OpenAIClient
from .config import (
    BATCH_SIZE_DEFAULT,
    DATABASE_PATH,
    LOG_FILE,
    ERROR_LOG_FILE,
    LOG_FORMAT,
    LOG_DATE_FORMAT,
    LOG_LEVEL,
)

# Import Services 1 and 2
from ..ingestion.database import ProductDatabase
from ..hts_context.service import HTSContextService

# Configure logging
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL),
    format=LOG_FORMAT,
    datefmt=LOG_DATE_FORMAT,
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.FileHandler(ERROR_LOG_FILE, mode="a"),
        logging.StreamHandler(),
    ],
)

logger = logging.getLogger(__name__)


class LLMEnhancementService:
    """
    Main LLM Enhancement Service

    Provides high-level API for enhancing product descriptions using OpenAI GPT-4
    Integrates with Services 1 (Database), 2 (HTS Context), and 4 (Rules)
    """

    def __init__(
        self, db_path: Optional[Path] = None, openai_api_key: Optional[str] = None
    ):
        """
        Initialize LLM Enhancement Service

        Args:
            db_path: Path to products database (uses default if None)
            openai_api_key: OpenAI API key (uses environment variable if None)
        """
        self.db_path = db_path or DATABASE_PATH

        # Initialize services
        logger.info("Initializing LLM Enhancement Service...")

        try:
            self.db = ProductDatabase(self.db_path)
            logger.info(f"Database connected: {self.db_path}")
        except Exception as e:
            logger.error(f"Failed to initialize database: {e}")
            raise

        try:
            self.hts_service = HTSContextService()
            logger.info("HTS Context Service initialized")
        except Exception as e:
            logger.error(f"Failed to initialize HTS Context Service: {e}")
            raise

        try:
            self.openai_client = OpenAIClient(api_key=openai_api_key)
            logger.info("OpenAI client initialized")
        except Exception as e:
            logger.error(f"Failed to initialize OpenAI client: {e}")
            raise

        # Initialize batch processor
        self.batch_processor = BatchProcessor(
            db=self.db, hts_service=self.hts_service, openai_client=self.openai_client
        )

        logger.info("LLM Enhancement Service initialized successfully")

    def process_batch(
        self,
        batch_size: int = BATCH_SIZE_DEFAULT,
        pass_number: int = 1,
        selected_item_ids: Optional[List[str]] = None,
        selected_rule_ids: Optional[List[str]] = None,
    ) -> BatchResult:
        """
        Process a batch of products

        Args:
            batch_size: Number of products to process
            pass_number: Pass number (1 for initial, 2+ for reprocessing)
            selected_item_ids: For Pass 2+, specific items to reprocess
            selected_rule_ids: Rule IDs to apply for Pass 2+

        Returns:
            BatchResult with statistics and results

        Example:
            >>> service = LLMEnhancementService()
            >>> result = service.process_batch(
            ...     batch_size=10,
            ...     pass_number=2,
            ...     selected_rule_ids=["R001", "R003"]
            ... )
            >>> print(f"Processed: {result.successful}/{result.total_processed}")
        """
        logger.info(f"Processing batch: size={batch_size}, pass={pass_number}")

        return self.batch_processor.process_batch(
            batch_size=batch_size,
            pass_number=pass_number,
            selected_item_ids=selected_item_ids,
            selected_rule_ids=selected_rule_ids,
        )

    def run_pass_1(
        self, batch_size: int = BATCH_SIZE_DEFAULT, max_batches: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        Run Pass 1: Process all unprocessed products without rules

        Args:
            batch_size: Size of each batch
            max_batches: Maximum number of batches to process (None = all)

        Returns:
            Summary dictionary with overall statistics

        Example:
            >>> service = LLMEnhancementService()
            >>> summary = service.run_pass_1(batch_size=100)
            >>> print(f"Pass 1 complete: {summary['processed']} products")
        """
        logger.info(
            f"Starting Pass 1: batch_size={batch_size}, max_batches={max_batches}"
        )

        stats = self.db.get_database_statistics()
        total_products = stats.total_products
        remaining = stats.unprocessed_count

        logger.info(f"Total products: {total_products}, Unprocessed: {remaining}")

        if remaining == 0:
            logger.info("No unprocessed products found")
            return {
                "status": "complete",
                "total_products": total_products,
                "processed": 0,
                "failed": 0,
                "remaining": 0,
                "batches_processed": 0,
            }

        batch_count = 0
        total_successful = 0
        total_failed = 0

        import time

        overall_start = time.time()

        while remaining > 0:
            if max_batches and batch_count >= max_batches:
                logger.info(f"Reached max_batches limit: {max_batches}")
                break

            batch_count += 1
            logger.info(f"Processing batch {batch_count}...")

            result = self.batch_processor.process_batch(
                batch_size=batch_size, pass_number=1, selected_item_ids=None
            )

            total_successful += result.successful
            total_failed += result.failed

            if result.total_processed == 0:
                logger.info("No more products to process")
                break

            # Update remaining count
            stats = self.db.get_database_statistics()
            remaining = stats.unprocessed_count

            logger.info(f"Batch {batch_count} complete. Remaining: {remaining}")

        overall_time = time.time() - overall_start

        summary = {
            "status": "complete" if remaining == 0 else "partial",
            "total_products": total_products,
            "processed": total_successful,
            "failed": total_failed,
            "remaining": remaining,
            "batches_processed": batch_count,
            "total_time": overall_time,
            "avg_time_per_batch": overall_time / batch_count if batch_count > 0 else 0,
        }

        logger.info(f"Pass 1 summary: {summary}")
        return summary

    def run_pass_2(
        self,
        selected_item_ids: List[str],
        batch_size: int = BATCH_SIZE_DEFAULT,
        selected_rule_ids: Optional[List[str]] = None,
    ) -> BatchResult:
        """
        Run Pass 2+: Reprocess selected products with rules

        Args:
            selected_item_ids: List of item IDs to reprocess
            batch_size: Maximum batch size
            selected_rule_ids: Rule IDs to apply for Pass 2+

        Returns:
            BatchResult with statistics

        Example:
            >>> service = LLMEnhancementService()
            >>> low_confidence_ids = ["ITEM001", "ITEM002", "ITEM003"]
            >>> result = service.run_pass_2(
            ...     selected_item_ids,
            ...     selected_rule_ids=["R001", "R002"]
            ... )
            >>> print(f"Reprocessed: {result.successful}/{result.total_processed}")
        """
        logger.info(f"Starting Pass 2: {len(selected_item_ids)} items selected")

        return self.batch_processor.process_batch(
            batch_size=batch_size,
            pass_number=2,
            selected_item_ids=selected_item_ids,
            selected_rule_ids=selected_rule_ids,
        )

    def get_processing_statistics(self) -> Dict[str, Any]:
        """
        Get current processing statistics

        Returns:
            Dictionary with database statistics

        Example:
            >>> service = LLMEnhancementService()
            >>> stats = service.get_processing_statistics()
            >>> print(f"Processed: {stats['processed_count']}/{stats['total_products']}")
        """
        stats = self.db.get_database_statistics()

        return {
            "total_products": stats.total_products,
            "processed_count": stats.processed_count,
            "unprocessed_count": stats.unprocessed_count,
            "confidence_distribution": stats.confidence_distribution,
            "processing_percentage": (
                (stats.processed_count / stats.total_products * 100)
                if stats.total_products > 0
                else 0
            ),
        }

    def get_low_confidence_items(self, limit: Optional[int] = None) -> List[Any]:
        """
        Get products with Low confidence for reprocessing

        Args:
            limit: Maximum number of items to return (None = all)

        Returns:
            List of products with Low confidence

        Example:
            >>> service = LLMEnhancementService()
            >>> low_conf = service.get_low_confidence_items(limit=50)
            >>> print(f"Found {len(low_conf)} low confidence items")
        """
        return self.db.get_products_by_confidence("Low", limit=limit)

    def get_medium_confidence_items(self, limit: Optional[int] = None) -> List[Any]:
        """
        Get products with Medium confidence for reprocessing

        Args:
            limit: Maximum number of items to return (None = all)

        Returns:
            List of products with Medium confidence

        Example:
            >>> service = LLMEnhancementService()
            >>> med_conf = service.get_medium_confidence_items(limit=50)
            >>> print(f"Found {len(med_conf)} medium confidence items")
        """
        return self.db.get_products_by_confidence("Medium", limit=limit)

    def resume_processing(self, batch_size: int = BATCH_SIZE_DEFAULT) -> Dict[str, Any]:
        """
        Resume interrupted Pass 1 processing

        Args:
            batch_size: Size of each batch

        Returns:
            Summary dictionary

        Example:
            >>> service = LLMEnhancementService()
            >>> summary = service.resume_processing()
            >>> print(f"Resumed and completed: {summary['processed']} products")
        """
        logger.info("Resuming interrupted processing...")
        return resume_pass_1(batch_size=batch_size)

    def get_product_details(self, item_id: str) -> Optional[Any]:
        """
        Get detailed information for a specific product

        Args:
            item_id: Product item ID

        Returns:
            Product with processing results or None if not found

        Example:
            >>> service = LLMEnhancementService()
            >>> product = service.get_product_details("ITEM001")
            >>> if product:
            ...     print(f"Confidence: {product.confidence_level}")
        """
        return self.db.get_product_by_id(item_id)

    def export_results(
        self, output_path: Path, confidence_level: Optional[str] = None
    ) -> int:
        """
        Export processing results to JSON

        Args:
            output_path: Path for output file
            confidence_level: Filter by confidence level (None = all)

        Returns:
            Number of records exported

        Example:
            >>> service = LLMEnhancementService()
            >>> count = service.export_results(Path("results.json"))
            >>> print(f"Exported {count} records")
        """
        logger.info(f"Exporting results to: {output_path}")

        if confidence_level:
            products = self.db.get_products_by_confidence(confidence_level)
        else:
            # Export all processed products
            products = self.db.get_all_products_with_processing()

        import json

        # Convert to dictionaries
        export_data = []
        for product in products:
            product_dict = {
                "item_id": product.item_id,
                "item_description": product.item_description,
                "enhanced_description": product.enhanced_description,
                "confidence_level": product.confidence_level,
                "confidence_score": product.confidence_score,
                "extracted_customer_name": product.extracted_customer_name,
                "extracted_dimensions": product.extracted_dimensions,
                "extracted_product": product.extracted_product,
                "final_hts": product.final_hts,
                "material_detail": product.material_detail,
                "product_group": product.product_group,
            }
            export_data.append(product_dict)

        # Write to file
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(export_data, f, indent=2, ensure_ascii=False)

        logger.info(f"Exported {len(export_data)} records")
        return len(export_data)


# Convenience functions for backward compatibility
def process_batch(
    batch_size: int = BATCH_SIZE_DEFAULT,
    pass_number: int = 1,
    selected_item_ids: Optional[List[str]] = None,
) -> BatchResult:
    """
    Convenience function to process a batch without initializing service

    Args:
        batch_size: Number of products to process
        pass_number: Pass number (1 for initial, 2+ for reprocessing)
        selected_item_ids: For Pass 2+, specific items to reprocess

    Returns:
        BatchResult with statistics and results
    """
    return batch_process(batch_size, pass_number, selected_item_ids)
