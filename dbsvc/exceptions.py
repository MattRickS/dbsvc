class InvalidComparison(Exception):
    """Raised when a comparison is invalid, eg, filters or joining columns"""


class InvalidFilters(Exception):
    """Raised when filters are not in a valid format"""


class InvalidJoin(Exception):
    """Raised when a join uses the wrong format"""


class InvalidSchema(Exception):
    """Raised when a request does not match the database schema"""
