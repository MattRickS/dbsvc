class DatabaseError(Exception):
    """Generic base exception for all database errors"""


class InvalidComparison(DatabaseError):
    """Raised when a comparison is invalid, eg, filters or joining columns"""


class InvalidFilters(DatabaseError):
    """Raised when filters are not in a valid format"""


class InvalidJoin(DatabaseError):
    """Raised when a join uses the wrong format"""


class InvalidSchema(DatabaseError):
    """Raised when a request does not match the database schema"""


class InvalidBatchCommand(DatabaseError):
    """Raised when a batch command is incorrectly structured"""

    def __init__(self, msg: str, index: int) -> None:
        super().__init__(msg)
        self.index = index
