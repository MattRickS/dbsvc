class InvalidSchema(Exception):
    """Raised when a request does not match the database schema"""


class InvalidComparison(Exception):
    """Raised when a comparison is invalid, eg, filters or joining columns"""


class InvalidFilters(Exception):
    """Raised when filters are not in a valid format"""
