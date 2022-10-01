from __future__ import annotations
from typing import Dict, List, Tuple, Union, TYPE_CHECKING

from .constants import Order
from .statement import Statement

if TYPE_CHECKING:
    VALUE_T = Union[str, int, float, bool]
    FILTER_VALUES_T = Dict[str, VALUE_T]
    FILTERS_T = Dict[str, Union[FILTER_VALUES_T, "FILTERS_T"]]
    ENTITY_T = Dict[str, VALUE_T]


class DBService:
    def create(self, cls: str, fields: List[str], list_of_values: List[List[VALUE_T]]) -> List[int]:
        """INSERT INTO {cls} ({fields}) VALUES {list_of_values}"""
        pass

    def read(
        self,
        cls: str,
        fields: List[str],
        filters: FILTERS_T,
        joins: Dict[str, str],
        limit: int = None,
        ordering: List[Tuple[str, Order]] = None,
    ) -> List[ENTITY_T]:
        """SELECT {cls}.{fields} FROM {cls} [JOIN {joins}] WHERE {filters} [ORDER BY {ordering}][LIMIT {limit}]"""
        Statement.read(cls, fields=fields, filters=filters)

    def update(self, cls: str, filters: FILTERS_T, field_values: Dict[str, VALUE_T]) -> int:
        """UPDATE {cls} SET {field}={value}, ... WHERE {filters}"""
        pass

    def delete(self, cls: str, filters: FILTERS_T) -> int:
        """DELETE FROM {cls} WHERE {filters}"""
        pass
