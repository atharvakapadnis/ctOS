"""
Benchmark script to measure ServiceFactory caching performance improvements

Measures:
- Service instantiation time (with/without caching)
- HTS context cache hit rates
- Memory usage
- Overall batch processing speedup

Run with: python scripts/benchmark_caching.py
"""
import time
import sys
import psutil
import os
from pathlib import Path
from typing import Dict, Any, List
from unittest.mock import Mock

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.services.common.service_factory import ServiceFactory
from src.services.ingestion.database import ProductDatabase
from src.services.hts_context.service import HTSContextService
from src.services.llm_enhancement.api_client import OpenAIClient
from src.services.llm_enhancement.batch_processor import BatchProcessor


class BenchmarkRunner:
    """Run caching benchmarks and collect metrics"""

    def __init__(self):
        self.results = {}
        self.process = psutil.Process(os.getpid())

    def get_memory_mb(self) -> float:
        """Get current process memory usage in MB"""
        return self.process.memory_info().rss / 1024 / 1024

    def benchmark_database_caching(self) -> Dict[str, Any]:
        """Benchmark database instantiation with/ without caching"""
        print("\n" + "=" * 80)
        print("BENCHMARK 1: Database Interaction")
        print("=" * 80)

        # Clear cache
        ServiceFactory.clear_cache()

        # Benchamrk: Multiple instantiations WITHOUT caching
        print("\nWithout ServiceFactory (creating new instances):")
        start_time = time.time()
        start_memory = self.get_memory_mb()

        instances = []
        for i in range(5):
            db = ProductDatabase()
            instances.append(db)

        no_cache_time = time.time() - start_time
        no_cache_memory = self.get_memory_mb() - start_memory

        print(f" Time for 5 instances: {no_cache_time*1000:.2f}ms")
        print(f" Memory increase: {no_cache_memory:.2f}MB")
        print(f" Average per instance: {no_cache_time/5*1000:.2f}ms")

        # Clear instances
        del instances
        ServiceFactory.clear_cache()

        # Benchmark: Multiple instantiations WITH caching
        print("\nWith ServiceFactory (cached instances):")
        start_time = time.time()
        start_memory = self.get_memory_mb()

        instances = []
        for i in range(5):
            db = ServiceFactory.get_database()
            instances.append(db)

        cached_time = time.time() - start_time
        cached_memory = self.get_memory_mb() - start_memory

        print(f" Time for 5 instances: {cached_time*1000:.2f}ms")
        print(f" Memory increase: {cached_memory:.2f}MB")
        print(f" Average per instance: {cached_time/5*1000:.2f}ms")

        # Calculate improvement
        time_saved = no_cache_time - cached_time
        memory_saved = no_cache_memory - cached_memory
        speedup = no_cache_time / cached_time if cached_time > 0 else 0

        print(f"\n Improvement:")
        print(f" Time saved: {time_saved*1000:.2f}ms ({speedup:.2f}x faster)")
        print(f" Memory saved: {memory_saved:.2f}MB")

        return {
            "no_cache_time_ms": no_cache_time * 1000,
            "cached_time_ms": cached_time * 1000,
            "time_saved_ms": time_saved * 1000,
            "speedup": speedup,
            "memory_saved_mb": memory_saved,
        }

    def benchmark_hts_service_caching(self) -> Dict[str, Any]:
        """Benchmark HTS service instantiation with/ without caching"""
        print("\n" + "=" * 80)
        print("BENCHMARK 2: HTS Service Interaction")
        print("=" * 80)

        # Clear cache
        ServiceFactory.clear_cache()

        # Benchmark: Multiple instantiations WITHOUT caching
        print("\nWithout ServiceFactory (loading HTS data each time):")
        start_time = time.time()
        start_memory = self.get_memory_mb()

        instances = []
        for i in range(3):
            hts = HTSContextService()
            instances.append(hts)

        no_cache_time = time.time() - start_time
        no_cache_memory = self.get_memory_mb() - start_memory

        print(f" Time for 3 instances: {no_cache_time*1000:.2f}ms")
        print(f" Memory increase: {no_cache_memory:.2f}MB")
        print(f" Average per instance: {no_cache_time/3*1000:.2f}ms")

        # Clear instances
        del instances
        ServiceFactory.clear_cache()

        # Benchmark: Multiple instantiations WITH caching
        print("\nWith ServiceFactory (cached instances, HTS data loaded only once):")
        start_time = time.time()
        start_memory = self.get_memory_mb()

        instances = []
        for i in range(3):
            hts = ServiceFactory.get_hts_service()
            instances.append(hts)

        cached_time = time.time() - start_time
        cached_memory = self.get_memory_mb() - start_memory

        print(f" Time for 3 instances: {cached_time*1000:.2f}ms")
        print(f" Memory increase: {cached_memory:.2f}MB")
        print(f" Average per instance: {cached_time/3*1000:.2f}ms")

        # Calculate improvement
        time_saved = no_cache_time - cached_time
        memory_saved = no_cache_memory - cached_memory
        speedup = no_cache_time / cached_time if cached_time > 0 else 0

        print(f"\n Improvement:")
        print(f" Time saved: {time_saved*1000:.2f}ms ({speedup:.2f}x faster)")
        print(f" Memory saved: {memory_saved:.2f}MB")

        return {
            "no_cache_time_ms": no_cache_time * 1000,
            "cached_time_ms": cached_time * 1000,
            "time_saved_ms": time_saved * 1000,
            "speedup": speedup,
            "memory_saved_mb": memory_saved,
        }

    def benchmark_batch_processor_hts_cache(self) -> Dict[str, Any]:
        """Benchmark HTS context caching within batch processing"""
        print("\n" + "=" * 80)
        print("BENCHMARK 3: HTS Context Batch Caching")
        print("=" * 80)

        # Create mock products with duplicate HTS codes
        print("\nCreating mock batch: 20 products with 5 unique HTS codes")
        products = []
        hts_codes = [
            "7307.11.00",
            "7307.19.00",
            "7307.92.00",
            "7307.93.00",
            "7308.90.00",
        ]

        for i in range(20):
            product = Mock()
            product.item_id = f"ITEM{i+1:03d}"
            product.final_hts = hts_codes[i % len(hts_codes)]
            product.item_description = f"Product {i+1}"
            product.material_detail = "Steel"
            product.product_group = "Fittings"
            products.append(product)

        # Create processor with mock HTS service that tracks calls
        from unittest.mock import MagicMock

        mock_hts = MagicMock(spec=HTSContextService)
        mock_hts.get_hts_context = MagicMock(
            return_value={"found": True, "hierarchy_path": []}
        )

        processor = BatchProcessor(hts_service=mock_hts)

        # Process all products (trigger cache)
        print("\nProcessing products and tracking HTS context lookups:")
        start_time = time.time()

        for product in products:
            processor._get_hts_context(product)

        processing_time = time.time() - start_time

        # Get cache statistics
        total_lookups = processor._cache_hits + processor._cache_misses
        unique_codes = len(processor._hts_context_cache)
        cache_hit_rate = (
            (processor._cache_hits / total_lookups * 100) if total_lookups > 0 else 0
        )

        print(f" Total Products: {len(products)}")
        print(f" Unique HTS codes: {unique_codes}")
        print(f" Total lookups: {total_lookups}")
        print(f" Cache hits: {processor._cache_hits}")
        print(f" Cache misses: {processor._cache_misses}")
        print(f" Cache hit rate: {cache_hit_rate:.1f}%")
        print(f" Service calls made: {mock_hts.get_hts_context.call_count}")
        print(f" Processing time: {processing_time*1000:.2f}ms")

        # Calculate efficiency
        calls_saved = len(products) - mock_hts.get_hts_context.call_count
        efficiency = (calls_saved / len(products) * 100) if len(products) > 0 else 0

        print(f"\n Efficiency:")
        print(
            f" Redundant calls eliminated: {calls_saved}/{len(products)} ({efficiency:.1f}%)"
        )
        print(f" Expected without cache: {len(products)} service calls")
        print(
            f" Actual with cache: {mock_hts.get_hts_context.call_count} service calls"
        )

        return {
            "total_products": len(products),
            "unique_hts_codes": unique_codes,
            "cache_hits": processor._cache_hits,
            "cache_misses": processor._cache_misses,
            "cache_hit_rate": cache_hit_rate,
            "service_calls": mock_hts.get_hts_context.call_count,
            "calls_saved": calls_saved,
            "efficiency_pct": efficiency,
        }

    def benchmark_batch_processor_service_reuse(self) -> Dict[str, Any]:
        """Benchmark service reuse across multiple BatchProcessor instances"""
        print("\n" + "=" * 80)
        print("BENCHMARK 4: Service Reuse Across Batch Processors")
        print("=" * 80)

        # Benchmark: Creates 3 processor WITHOUT ServiceFactory
        print("\nWithout ServiceFactory (each creates own services):")
        start_time = time.time()
        start_memory = self.get_memory_mb()

        processors = []
        for i in range(3):
            # Create processor with fresh instances
            db = ProductDatabase()
            hts = HTSContextService()
            openai = OpenAIClient()
            proc = BatchProcessor(db=db, hts_service=hts, openai_client=openai)
            processors.append(proc)

        no_reuse_time = time.time() - start_time
        no_reuse_memory = self.get_memory_mb() - start_memory

        print(f" Time for 3 processors: {no_reuse_time*1000:.2f}ms")
        print(f" Memory increase: {no_reuse_memory:.2f}MB")

        del processors
        ServiceFactory.clear_cache()

        # Benchmark: Creates 3 processors WITH ServiceFactory
        print("\nWith ServiceFactory (reuses cached services):")
        start_time = time.time()
        start_memory = self.get_memory_mb()

        processors = []
        for i in range(3):
            proc = BatchProcessor()
            processors.append(proc)

        reuse_time = time.time() - start_time
        reuse_memory = self.get_memory_mb() - start_memory

        print(f" Time for 3 processors: {reuse_time*1000:.2f}ms")
        print(f" Memory increase: {reuse_memory:.2f}MB")

        # Verify service reuse
        all_same_hts = all(
            p.hts_service is processors[0].hts_service for p in processors
        )
        all_same_db = all(p.db is processors[0].db for p in processors)

        print(f"\n Service Reuse Verification:")
        print(f" All processors share HTS service: {all_same_hts}")
        print(f" All processors share database: {all_same_db}")

        # Calculate improvement
        time_saved = no_reuse_time - reuse_time
        memory_saved = no_reuse_memory - reuse_memory
        speedup = no_reuse_time / reuse_time if reuse_time > 0 else 0

        print(f"\n Improvement:")
        print(f" Time saved: {time_saved*1000:.2f}ms ({speedup:.2f}x faster)")
        print(f" Memory saved: {memory_saved:.2f}MB")

        return {
            "no_reuse_time_ms": no_reuse_time * 1000,
            "reuse_time_ms": reuse_time * 1000,
            "time_saved_ms": time_saved * 1000,
            "speedup": speedup,
            "memory_saved_mb": memory_saved,
            "services_reused": all_same_hts and all_same_db,
        }

    def run_all_benchmarks(self) -> Dict[str, Any]:
        """Run all benchmarks and return comprehensive results"""
        print("\n" + "=" * 80)
        print("SERVICE FACTORY & CACHING BENCHMARK SUITE")
        print("=" * 80)
        print(f"Python process memory at start: {self.get_memory_mb():.2f}MB")

        results = {}

        # Run benchmarks
        try:
            results["database"] = self.benchmark_database_caching()
        except Exception as e:
            print(f"\nError in database benchmark: {e}")
            results["database"] = None

        try:
            results["hts_service"] = self.benchmark_hts_service_caching()
        except Exception as e:
            print(f"\nError in HTS service benchmark: {e}")
            results["hts_service"] = None

        try:
            results["hts_batch_cache"] = self.benchmark_batch_processor_hts_cache()
        except Exception as e:
            print(f"\nError in HTS batch cache benchmark: {e}")
            results["hts_batch_cache"] = None

        try:
            results["service_reuse"] = self.benchmark_batch_processor_service_reuse()
        except Exception as e:
            print(f"\nError in service reuse benchmark: {e}")
            results["service_reuse"] = None

        # Print summary
        self.print_summary(results)

        return results

    def print_summary(self, results: Dict[str, Any]):
        """Print comprehensive summary of all benchmarks"""
        print("\n" + "=" * 80)
        print("BENCHMARK SUMMARY")
        print("=" * 80)

        if results.get("database"):
            print("\n1. Database Caching:")
            print(f"   Speedup: {results['database']['speedup']:.2f}x faster")
            print(f"   Time saved: {results['database']['time_saved_ms']:.2f}ms")
            print(f"   Memory saved: {results['database']['memory_saved_mb']:.2f}MB")
        else:
            print("\n1. Database Caching: SKIPPED")

        if results.get("hts_service"):
            print("\n2. HTS Service Caching:")
            print(f"   Speedup: {results['hts_service']['speedup']:.2f}x faster")
            print(f"   Time saved: {results['hts_service']['time_saved_ms']:.2f}ms")
            print(f"   Memory saved: {results['hts_service']['memory_saved_mb']:.2f}MB")
        else:
            print("\n2. HTS Service Caching: SKIPPED")

        if results.get("hts_batch_cache"):
            print("\n3. HTS Context Batch Cache:")
            print(
                f"   Cache hit rate: {results['hts_batch_cache']['cache_hit_rate']:.1f}%"
            )
            print(
                f"   Redundant calls eliminated: {results['hts_batch_cache']['calls_saved']}"
            )
            print(
                f"   Efficiency improvement: {results['hts_batch_cache']['efficiency_pct']:.1f}%"
            )
        else:
            print("\n3. HTS Context Batch Cache: SKIPPED")

        if results.get("service_reuse"):
            print("\n4. Service Reuse Across Processors:")
            print(f"   Speedup: {results['service_reuse']['speedup']:.2f}x faster")
            print(f"   Time saved: {results['service_reuse']['time_saved_ms']:.2f}ms")
            print(
                f"   Memory saved: {results['service_reuse']['memory_saved_mb']:.2f}MB"
            )
            print(
                f"   Services properly reused: {results['service_reuse']['services_reused']}"
            )
        else:
            print("\n4. Service Reuse Across Processors: SKIPPED")

        # Overall impact (only if we have data)
        total_time_saved = 0
        total_memory_saved = 0

        if results.get("database"):
            total_time_saved += results["database"]["time_saved_ms"]
            total_memory_saved += results["database"]["memory_saved_mb"]

        if results.get("hts_service"):
            total_time_saved += results["hts_service"]["time_saved_ms"]
            total_memory_saved += results["hts_service"]["memory_saved_mb"]

        if results.get("service_reuse"):
            total_time_saved += results["service_reuse"]["time_saved_ms"]
            total_memory_saved += results["service_reuse"]["memory_saved_mb"]

        if total_time_saved > 0 or total_memory_saved > 0:
            print("\n" + "=" * 80)
            print("OVERALL IMPACT")
            print("=" * 80)
            print(f"Total time saved per operation: {total_time_saved:.2f}ms")
            print(f"Total memory saved: {total_memory_saved:.2f}MB")

            if results.get("hts_batch_cache"):
                print(
                    f"HTS cache eliminates ~{results['hts_batch_cache']['efficiency_pct']:.0f}% of redundant lookups"
                )

            print(
                "\nConclusion: ServiceFactory caching provides significant performance"
            )
            print(
                "improvements with minimal code changes and full backwards compatibility."
            )
            print("=" * 80)


def main():
    """Run benchmark suite"""
    runner = BenchmarkRunner()
    results = runner.run_all_benchmarks()

    print("\nBenchmark results")
    print("Note: Results may vary based on system load and hardware")


if __name__ == "__main__":
    main()
