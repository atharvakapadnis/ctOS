"""
Service Factory - Centralized service instance manager with caching

Provides singleton access to all major services:
- Product Database
- HTS Context Service
- OpenAI Client
- Rule Manager

Features:
- Lazy initialization (Created only when needed)
- Thread-safe instance creation
- Path aware caching for database
- File Modification detection for rules
- Clearable cache for testing
- Backwards compatible (can pass custom instaces)
"""

from functools import cached_property
import logging
import threading
from pathlib import Path
from typing import Optional, Dict, Any

from ..ingestion.database import ProductDatabase
from ..hts_context.service import HTSContextService
from ..llm_enhancement.api_client import OpenAIClient
from ..rules.manager import RuleManager

from ..ingestion.config import DATABASE_PATH
from ..hts_context.config import HTS_REFERENCE_PATH
from ..rules.config import RULES_FILE

logger = logging.getLogger(__name__)


class ServiceFactory:
    """
    Centralized service instance manager with caching

    Provides singelton access to all major serbices with intelligent caching
    to eliminate redundant service instantiation and improve performance.

    Thread-safe and backwards compatible with existing code.
    """

    _instance: Dict[str, Any] = {}
    _lock: threading.Lock = threading.Lock()

    @classmethod
    def get_databse(cls, db_path: Optional[Path] = None) -> ProductDatabase:
        """
        Get or create ProductDatabase instance with path-aware caching

        Args:
            db_path: Path to database file (uses default DATABASE_PATH if None)

        Returns:
            ProductDatabase instance (cached per path)

        Usage:
            db = ServiceFactory.get_database()
            custom_db = ServiceFactory.get_database(Path('/custom/path.db'))

        Notes:
            - Different paths create separate cached instances
            - Same path always returns same instance
            - Thread-safe instance creation
        """
        cache_key = f"database_{str(db_path or DATABASE_PATH)}"

        if cache_key in cls._instances:
            logger.debug(
                f"[ServiceFactory] Returning cached database instance for {db_path or DATABASE_PATH}"
            )
            return cls._instances[cache_key]

        with cls._lock:
            if cache_key not in cls._instances:
                logger.debug(
                    f"[ServiceFactory] Creating new database instance for {db_path or DATABASE_PATH}"
                )
                cls._instances[cache_key] = ProductDatabase(db_path)

        return cls._instances[cache_key]

    @classmethod
    def get_hts_service(cls, hts_file_path: Optional[Path] = None) -> HTSContextService:
        """
        Get or create HTSContextService instance (singleton)

        Args:
            hts_file_path: Path to HTS data file (uses default HTS_REFERENCE_PATH if None)

        Returns:
            HTSContextService instance (always same cached instance)

        Usage:
            hts_service = ServiceFactory.get_hts_service()

        Notes:
            - Always returns same singleton instance
            - Loads 5MB JSON file only once
            - 100-200ms saved on subsequent calls
            - Thread-safe instance creation
        """
        cache_key = "hts_service"

        if cache_key in cls._instances:
            logger.debug("[ServiceFactory] Returning cached HTS service instance")
            return cls._instances[cache_key]

        with cls._lock:
            if cache_key not in cls._instances:
                logger.info(
                    "[ServiceFactory] Creating new HTS service instance (loading HTS data...)"
                )
                cls._instances[cache_key] = HTSContextService(hts_file_path)
                logger.info("[ServiceFactory] HTS service initialized suvvessfully")

        return cls._instances[cache_key]

    @classmethod
    def get_openai_client(cls, api_key: Optional[str] = None) -> OpenAIClient:
        """
        Get or create OpenAIClient instance

        Args:
            api_key: Optional custom API key (creates fresh instance if provided)

        Returns:
            OpenAIClient instance (cached for default key, fresh for custom keys)

        Usage:
            client = ServiceFactory.get_openai_client()
            custom_client = ServiceFactory.get_openai_client(api_key='sk-custom')

        Notes:
        - Default API key: Returns cached singleton instance
        - Custom API key: Always creates fresh instance (not cached)
        - Security: Never caches custom API keys
        - Thread-safe for default key path
        """
        if api_key is not None:
            logger.debug(
                "[ServiceFactory] Creating fresh OpenAI client with custom API key (not cached)"
            )
            return OpenAIClient(api_key=api_key)

        cache_key = "openai_client"

        if cache_key in cls._instances:
            logger.debug("[ServiceFactory] Returning cached OpenAI client instance")
            return cls._instances[cache_key]

        with cls._lock:
            if cache_key not in cls._instances:
                logger.info("[ServiceFactory] Creating new OpenAI client instance")
                cls._instances[cache_key] = OpenAIClient()

        return cls._instances[cache_key]

    @classmethod
    def get_rule_manager(cls, rules_file: Optional[Path] = None) -> RuleManager:
        """
        Get or create RuleManager instance with automatic file modification detection

        Args:
            rules_file: Path to rules JSON file (uses default RULES_FILE if None)

        Returns:
            Rule Manager instance (cached, auto-reloads on file modification)

        Usage:
            rule_manager = ServiceFactory.get_rule_manager()

        Notes:
            - Returns same cached instance
            - Automatically detects if rules.json was modified
            - Reloads rules if file changed since last access
            - Thread-safe instance creation and file checking
        """
        cache_key = "rule_manager"

        if cache_key in cls._instances:
            instance = cls._instances[cache_key]

            if cls._check_rules_file_modified():
                logger.info("[ServiceFactory] Rules file modified, reloading rules")
                instance._cache_loaded = False
                instance.load_rules()

            return instance

        with cls._lock:
            if cache_key not in cls._instances:
                logger.debug("[ServiceFactory] Creating new RuleManager instance")
                instance = RuleManager(rules_file)
                cls._instances[cache_key] = instance

                rules_path = rules_file or RULES_FILE
                if rules_path.exists():
                    cls._instances["_rules_file_mtime"] = rules_path.stat().st_mtime

        return cls._instances[cache_key]

    @classmethod
    def reload_rules(cls) -> None:
        """
        Force reload of rules (called after CRUD operations)

        Usage:
            ServiceFactory.reload_rules()

        Notes:
            - Call this after creating/updating/deleting rules
            - Forces RuleManager to reload from file
            - Updates stored file modification time
            - Safe to call even if RuleManager not yet cached
        """
        cache_key = "rule_manager"

        if cache_key not in cls._instances:
            logger.debug("[ServiceFactory] No RuleManager instance to reload")
            return

        logger.info("[ServiceFactory] Forcing rule reload")
        instance = cls._instances[cache_key]
        instance._cache_loaded = False
        instance.load_rules()

        if RULES_FILE.exists():
            cls._instances["_rules_file_mtime"] = RULES_FILE.stat().st_mtime

    @classmethod
    def clear_cache(cls) -> None:
        """
        Clear all cached service instances

        Usage:
            ServiceFactory.clear_cache()

        Notes:
            - Clears all cached instances
            - Useful for testing (clear between tests)
            - Useful for troubleshooting (force fresh start)
            - Thread-safe operation
        """
        with cls._lock:
            count = len(cls._instances)
            logger.info(f"[ServiceFactory] Clearing ServiceFactory cache")
            cls._instances.clear()
            logger.info(f"[ServiceFactory] Cleared {count} cached service instances")

    @classmethod
    def get_cache_stats(cls) -> Dict[str, Any]:
        f"""
        Get Statistics about cached instances (for debugging/ monitoring)

        Returns:
            Dictionary with cache statistics

        Usage:
            stats = ServiceFactory.get_cache_stats()
            print(f"Total instances: {stats['total_instances']}")

        Notes:
            - Useful for debugging
            - Shows what's currently cached
            - Can display in Streamlit dashboard
        """
        stats = {
            "total_instances": len(
                [k for k in cls._instances.keys() if not k.startswith("_")]
            ),
            "instance_types": [
                k for k in cls._instances.keys() if not k.startswith("_")
            ],
            "database_paths": [
                k.replace("database_", "")
                for k in cls._instances.keys()
                if k.startswith("database_")
            ],
            "has_hts_service": "hts_service" in cls._instances,
            "has_rule_manager": "rule_manager" in cls._instances,
            "has_openai_client": "openai_client" in cls._instances,
        }

        return stats

    @classmethod
    def _check_rules_file_modified(cls) -> bool:
        """
        Check if rules.json file has been modified since last load

        Returns:
            True if file was modified, False otherwise

        Notes:
            - Used current file mtime with stored mtime
            - Updates stored mtime if file changed
            - Returns False if file doesn't exist or no stored mtime
        """
        if not RULES_FILE.exists():
            return False

        stored_mtime = cls._instances.get("_rules_file_mtime")
        if stored_mtime is None:
            return False

        current_mtime = RULES_FILE.stat().st_mtime

        if current_mtime != stored_mtime:
            cls._instances["_rules_file_mtime"] = current_mtime
            return True

        return False
