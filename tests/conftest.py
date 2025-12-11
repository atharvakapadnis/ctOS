import pytest
import sys
from pathlib import Path

# Add src to Path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))


@pytest.fixture(autouse=True)
def clear_service_factory_cache():
    """
    Auto Clear ServiceFactory cache before and after each test

    Ensuers test isolation by preventing cached instances from one test affecting another test.

    This fixture runs automatically for ALL tests (autouse=True)
    """

    from src.services.common.service_factory import ServiceFactory

    # Clear ServiceFactory cache before test
    ServiceFactory.clear_cache()

    # Yield control back to test
    yield

    # Clear ServiceFactory cache after test
    ServiceFactory.clear_cache()
