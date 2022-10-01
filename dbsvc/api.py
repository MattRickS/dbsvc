from __future__ import annotations
from typing import Dict, Iterator, List, Tuple, Union, TYPE_CHECKING

from sqlalchemy import (
    and_,
    asc,
    delete,
    desc,
    insert,
    or_,
    create_engine,
    select,
    update,
    Column,
    Index,
    Integer,
    MetaData,
    String,
    Table,
)

from dbsvc.constants import Order

if TYPE_CHECKING:
    from sqlalchemy.sql import ClauseElement

    VALUE_T = Union[str, int, float, bool]
    FILTER_VALUES_T = Dict[str, VALUE_T]
    # {cmp: {column: value}, or: [{cmp: {column: value}}]}
    FILTERS_T = Dict[str, Union[FILTER_VALUES_T, List["FILTERS_T"]]]
    ENTITY_T = FIELDS_T = Dict[str, VALUE_T]
    # {alias: [(field, method, table, field), ...]}
    JOINS_T = Dict[str, List[Tuple[str, str, str, str]]]
    JOIN_TABLES_T = Dict[str, Table]


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


class Database:
    def __init__(self, uri: str) -> None:
        self._engine = create_engine(uri, echo=True)
        self._engine.connect()
        self._metadata = self._build_tables()
        self._metadata.create_all(self._engine)

    def _build_tables(self):
        metadata = MetaData()

        shot_table = Table(
            "Shot",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("name", String, nullable=False),
        )
        asset_table = Table(
            "Asset",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("name", String, nullable=False),
        )
        asset_shot_table = Table(
            "AssetXShot",
            metadata,
            Column("asset_id", Integer, nullable=False),
            Column("shot_id", Integer, nullable=False),
        )
        Index("asset_shot", asset_shot_table.c.asset_id, asset_shot_table.c.shot_id, unique=True)

        return metadata

    def create(self, cls: str, list_of_values: List["ENTITY_T"]) -> List[int]:
        """Inserts multiple rows into the database"""
        table = self._table(cls)
        # XXX: Generate IDs?
        with self._engine.connect() as conn:
            cursor = conn.execute(insert(table), list_of_values)
            # TODO: Better return info
            return cursor.rowcount

    def read(
        self,
        cls: str,
        columns: List[str] = ("*",),
        filters: "FILTERS_T" = None,
        joins: "JOINS_T" = None,
        limit: int = None,
        ordering: List[Tuple[str, Order]] = None,
    ) -> Iterator["ENTITY_T"]:
        """SELECT {cls}.{fields} FROM {cls} [JOIN {joins}] WHERE {filters} [ORDER BY {ordering}][LIMIT {limit}]"""
        table = self._table(cls)
        join_tables = self._joins(joins) if joins else None
        columns = [self._column(table, col, join_tables=join_tables) for col in columns]
        stmt = select(*columns)

        if join_tables:
            tablestmt = table
            for join_stmt in join_tables.values():
                tablestmt = tablestmt.join(join_stmt)
            stmt.select_from(tablestmt)

        if filters:
            where_clause = self._filters(table, filters, join_tables=join_tables)
            stmt = stmt.where(where_clause)

        if ordering:
            order_by = []
            for colname, order in ordering:
                column = self._column(table, colname, join_tables=join_tables)
                method = asc if order == Order.Ascending else desc
                order_by.append(method(column))
            stmt = stmt.order_by(*order_by)

        if limit:
            stmt = stmt.limit(limit)

        with self._engine.connect() as conn:
            rows = conn.execute(stmt).fetchall()

        return map(dict, rows)

    def update(self, cls: str, field_values: "FIELDS_T", filters: "FILTERS_T" = None) -> int:
        """UPDATE {cls} SET {field}={value}, ... WHERE {filters}"""
        # TODO: bindparam options to perform different updates per row?
        table = self._table(cls)
        stmt = update(table)
        if filters:
            where_clause = self._filters(table, filters)
            stmt = stmt.where(where_clause)
        stmt = stmt.values(field_values)

        with self._engine.connect() as conn:
            cursor = conn.execute(stmt)
            return cursor.rowcount

    def delete(self, cls: str, filters: "FILTERS_T" = None) -> int:
        """DELETE FROM {cls} WHERE {filters}"""
        table = self._table(cls)
        stmt = delete(table)
        if filters:
            where_clause = self._filters(table, filters)
            stmt = stmt.where(where_clause)

        with self._engine.connect() as conn:
            cursor = conn.execute(stmt)
            return cursor.rowcount

    # ==================================================================================
    # Private

    def _cmp(self, column: Column, method: str, value: "VALUE_T") -> "ClauseElement":
        """Calls a comparison function for the column and value, eg, "eq" -> column.__eq__(value)"""
        funcname = COMPARISON_MAP[method]
        return getattr(column, funcname)(value)

    def _table(self, name: str) -> Table:
        """Fetches a table object matching the given name"""
        return self._metadata.tables[name]

    def _column_or_table(self, table: Table, name: str) -> Union[Column, Table]:
        return table if name == ALL_COLUMNS else getattr(table.c, name)

    """
    XXX: Could have presets for alias fields on classes

    presets:
    - class: Shot
      alias: assets
      joins:
      - ["id", "shot_id", "AssetShot"]
      - ["asset_id", "id", "Asset"]
      default_fields: ["*"]
    
    Can then by used like

        read("Shot", ["assets"]) -> implicitly uses default_fields -> `assets.*`
        read("Shot", ["assets.name"]) -> explicit -> `assets.name`
    """

    def _column(self, table: Table, name: str, join_tables=None) -> Union[Column, Table]:
        """
        Fetches the column from the default or joined table

        Column name format can be either '{column}' or '{table}.{column}'.
        The former will use the column from the default table.
        The latter will lookup the table to use from join_tables.
        """
        index = name.find(".")
        if index == -1:
            return self._column_or_table(table, name)

        alias = name[:index]
        if alias == table.name:
            return self._column_or_table(table, name)

        if not join_tables or alias not in join_tables:
            raise Exception(f"Column requires alias that wasn't provided: {alias}")

        name = name[index + 1 :]
        if "." in name:
            raise Exception(
                "Too many components in column, format must be either '{column}' or '{alias}.{column}'"
            )

        join_table = join_tables[alias]
        return self._column_or_table(join_table, name)

    def _joins(self, table: Table, joins: "JOINS_T") -> "JOIN_TABLES_T":
        """Generates the join statements for each given alias"""
        join_tables = {}
        for alias, join_steps in joins.items():
            stmt = table
            for field, join_method, join_class, join_field in join_steps:
                join_table = self._metadata.tables[join_class]
                stmt = stmt.join(
                    join_table,
                    self._cmp(
                        self._column(stmt, field), join_method, self._column(join_table, join_field)
                    ),
                )
            join_tables[alias] = stmt
        return join_tables

    def _filters(
        self, table: Table, filters: "FILTERS_T", join_tables: "JOIN_TABLES_T" = None
    ) -> "ClauseElement":
        """Generates the where clause for the filters"""
        stmts = []
        for key, value in filters.items():
            if key == FILTER_OR:
                stmts.append(
                    or_(
                        *(
                            self._filters(table, subfilters, join_tables=join_tables).self_group()
                            for subfilters in value
                        )
                    ).self_group()
                )
            else:
                for field, val in value.items():
                    column = self._column(table, field, join_tables=join_tables)
                    stmts.append(self._cmp(column, key, val))

        return and_(*stmts)


if __name__ == "__main__":
    memdb = Database("sqlite://")
    print(
        memdb.create(
            "Shot",
            [{"id": 1, "name": "First"}, {"id": 2, "name": "Second"}, {"id": 3, "name": "Third"}],
        )
    )
    print(list(memdb.read("Shot")))
    print(list(memdb.read("Shot", filters={"lt": {"id": 3}})))
    print(memdb.update("Shot", {"name": "Other"}, filters={"eq": {"id": 1}}))
    print(list(memdb.read("Shot")))
    print(memdb.delete("Shot", filters={"eq": {"id": 2}}))
    print(list(memdb.read("Shot")))
