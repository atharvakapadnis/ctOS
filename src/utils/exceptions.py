"""
Custom exceptions for ctOS Service 1
"""


class DataValidationError(Exception):
    """Raised when data validation fails"""

    pass


class CSVLoadError(Exception):
    """Raised when CSV file loading fails"""

    pass


class HTSLookupError(Exception):
    """Raised when HTS lookup fails"""

    pass


class DatabaseError(Exception):
    """Raised when database operations fail"""

    pass


class ConfigurationError(Exception):
    """Raised when configuration issues occur"""

    pass
