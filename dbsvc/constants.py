import enum


class Order(enum.IntEnum):
    Asc = 0
    Desc = 1


COMPARISON_MAP = {
    "eq": "is_",
    "ne": "is_not",
    "in": "in_",
    "not_in": "notin_",
    "like": "like",
    "unlike": "notlike",
    "lt": "__lt__",
    "le": "__le__",
    "gt": "__gt__",
    "ge": "__ge__",
}
FILTER_OR = "or"
ALL_COLUMNS = "*"
VALID_FILTER_KEYS = [FILTER_OR] + list(COMPARISON_MAP)
