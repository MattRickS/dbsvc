from __future__ import annotations
from typing import Dict, Iterator, List, Tuple, Union, TYPE_CHECKING

from sqlalchemy import (
    and_,
    asc,
    delete,
    desc,
    exc,
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

from dbsvc import constants, exceptions

if TYPE_CHECKING:
    from sqlalchemy.sql.elements import ClauseElement, Label

    VALUE_T = Union[str, int, float, bool]
    FILTER_VALUES_T = Dict[str, VALUE_T]
    # {cmp: {column: value}, or: [{cmp: {column: value}}]}
    FILTERS_T = Dict[str, Union[FILTER_VALUES_T, List["FILTERS_T"]]]
    ENTITY_T = COL_VALUES_T = Dict[str, VALUE_T]
    # {alias: [(column, method, table, column), ...]}
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
    def __init__(self, uri: str, debug: bool = False) -> None:
        self._engine = create_engine(uri, echo=debug)
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

    def create(self, tablename: str, list_of_values: List["ENTITY_T"]) -> List[int]:
        """Inserts multiple rows into the database"""
        table = self._table(tablename)
        # XXX: Generate IDs?
        with self._engine.connect() as conn:
            cursor = conn.execute(insert(table), list_of_values)
            # TODO: Better return info
            return cursor.rowcount

    def read(
        self,
        tablename: str,
        colnames: List[str] = ("*",),
        filters: "FILTERS_T" = None,
        joins: "JOINS_T" = None,
        limit: int = None,
        ordering: List[Tuple[str, constants.Order]] = None,
    ) -> Iterator["ENTITY_T"]:
        """Reads `colnames` for all rows matching `filters`"""
        table = self._table(tablename)
        join_tables = self._join_tables(joins) if joins else None
        columns = self._select_columns(table, colnames, join_tables=join_tables)
        stmt = select(*columns)

        if joins:
            stmt = self._joins(stmt, table, joins)

        if filters:
            where_clause = self._filters(table, filters, join_tables=join_tables)
            stmt = stmt.where(where_clause)

        if ordering:
            order_by = []
            for colname, order in ordering:
                column = self._column(table, colname, join_tables=join_tables)
                method = asc if order == constants.Order.Ascending else desc
                order_by.append(method(column))
            stmt = stmt.order_by(*order_by)

        if limit:
            stmt = stmt.limit(limit)

        with self._engine.connect() as conn:
            rows = conn.execute(stmt).fetchall()

        return map(dict, rows)

    def update(
        self, tablename: str, col_values: "COL_VALUES_T", filters: "FILTERS_T" = None
    ) -> int:
        """Updates the given values on all rows matching filters"""
        # TODO: bindparam options to perform different updates per row?
        table = self._table(tablename)
        stmt = update(table)
        if filters:
            where_clause = self._filters(table, filters)
            stmt = stmt.where(where_clause)

        stmt = stmt.values(col_values)
        with self._engine.connect() as conn:
            cursor = conn.execute(stmt)
            return cursor.rowcount

    def delete(self, tablename: str, filters: "FILTERS_T" = None) -> int:
        """Deletes all rows matching filters"""
        table = self._table(tablename)
        stmt = delete(table)
        if filters:
            where_clause = self._filters(table, filters)
            stmt = stmt.where(where_clause)

        with self._engine.connect() as conn:
            cursor = conn.execute(stmt)
            return cursor.rowcount

    # ==================================================================================
    # Private

    def _table(self, name: str) -> Table:
        """Fetches a table object matching the given name"""
        try:
            return self._metadata.tables[name]
        except KeyError:
            raise exceptions.InvalidSchema(f"No table exists called {name}")

    """
    XXX: Could have presets for aliases

    presets:
    - class: Shot
      alias: assets
      joins:
      - ["id", "shot_id", "AssetShot"]
      - ["asset_id", "id", "Asset"]
      default_columns: ["*"]
    
    Can then by used like

        read("Shot", ["assets"]) -> implicitly uses default_columns -> `assets.*`
        read("Shot", ["assets.name"]) -> explicit -> `assets.name`
    """

    def _table_and_colname(
        self, table: Table, colname: str, join_tables: "JOIN_TABLES_T" = None
    ) -> Tuple[Table, str]:
        """
        Column name format can be either '{column}' or '{table}.{column}'.
        The former will return the default table and colname.
        The latter will lookup the table to use from join_tables and split the colname.
        """
        index = colname.find(".")
        if index == -1:
            return (table, colname)

        alias = colname[:index]
        colname = colname[index + 1 :]
        if "." in colname:
            raise exceptions.InvalidSchema(
                "Too many components in column, format must be either '{column}' or '{alias}.{column}'"
            )

        if alias == table.name:
            return (table, colname)

        if not join_tables or alias not in join_tables:
            raise exceptions.InvalidSchema(f"Column requires alias that wasn't provided: {alias}")

        join_table = join_tables[alias]
        return (join_table, colname)

    def __column(self, table: Table, colname: str):
        """Fetches the column from the table, raises an exception if invalid"""
        try:
            return getattr(table.c, colname)
        except AttributeError:
            raise exceptions.InvalidSchema(f"Table {table.name} has no column {colname}")

    def _column(
        self, table: Table, colname: str, join_tables: "JOIN_TABLES_T" = None
    ) -> Union[Column, Table]:
        """Fetches the column from the default or joined table"""
        table, colname = self._table_and_colname(table, colname, join_tables=join_tables)
        return self.__column(table, colname)

    def _select_columns(
        self, table: Table, columns: List[str], join_tables: "JOIN_TABLES_T"
    ) -> List["Label"]:
        """
        Generates the columns to use in a select query with suitable labels.

        If the given column name does not define a table/alias, the colname is the label.
        If a table/alias is defined, the full table.column name is used as the label.
        Wildcards are expanded but follow the same rules when which label is used.
        """
        for name in columns:
            coltable, colname = self._table_and_colname(table, name, join_tables=join_tables)
            if colname == ALL_COLUMNS:
                for column in coltable.columns:
                    # Can't use `name`, it might be a wildcard. Explicitly join table and column
                    yield column.label(
                        f"{coltable.name}.{column.name}" if "." in name else column.name
                    )
            else:
                yield self.__column(coltable, colname).label(name)

    def _cmp(self, column: Column, method: str, value: "VALUE_T") -> "ClauseElement":
        """Calls a comparison function for the column and value, eg, "eq" -> column.__eq__(value)"""
        try:
            funcname = COMPARISON_MAP[method]
            return getattr(column, funcname)(value)
        except exc.ArgumentError as e:
            raise exceptions.InvalidComparison(
                f"Comparison '{method}' got unexpected type {type(value)} for column '{column.name}'"
            )
        except KeyError:
            raise exceptions.InvalidComparison(f"'{method}' is not a valid comparison method")

    def _join_tables(self, joins: "JOINS_T") -> "JOIN_TABLES_T":
        """Generates a mapping of alias to the final table in a join"""
        return {alias: self._table(j[-1][-2]) for alias, j in joins.items()}

    def _joins(self, stmt: "ClauseElement", table: Table, joins: "JOINS_T") -> "ClauseElement":
        """Generates the join statements for each given alias"""
        stmt = stmt.select_from(table)
        for join_steps in joins.values():
            prev_table = table
            for colname, join_method, join_class, join_colname in join_steps:
                join_table = self._metadata.tables[join_class]
                stmt = stmt.join(
                    join_table,
                    self._cmp(
                        self._column(prev_table, colname),
                        join_method,
                        self._column(join_table, join_colname),
                    ),
                )
                prev_table = join_table
        return stmt

    def _filters(
        self, table: Table, filters: "FILTERS_T", join_tables: "JOIN_TABLES_T" = None
    ) -> "ClauseElement":
        """Generates the where clause for the filters"""
        stmts = []
        for key, value in filters.items():
            if key == FILTER_OR:
                if not isinstance(value, (list, tuple)):
                    raise exceptions.InvalidFilters(
                        f"'or' filter requires a list of filters, got {type(value)}"
                    )
                stmts.append(
                    or_(
                        *(
                            self._filters(table, subfilters, join_tables=join_tables).self_group()
                            for subfilters in value
                        )
                    ).self_group()
                )
            elif not isinstance(value, dict):
                raise exceptions.InvalidFilters(
                    f"'{key}' filter requires a dictionary of columns and values, got {type(value)}"
                )
            else:
                for colname, val in value.items():
                    try:
                        column = self._column(table, colname, join_tables=join_tables)
                        stmts.append(self._cmp(column, key, val))
                    except (exceptions.InvalidComparison, exceptions.InvalidSchema) as e:
                        raise exceptions.InvalidFilters(str(e)) from e

        return and_(*stmts)


if __name__ == "__main__":
    memdb = Database("sqlite://", debug=True)
    print(
        memdb.create(
            "Shot",
            [{"id": 1, "name": "First"}, {"id": 2, "name": "Second"}, {"id": 3, "name": "Third"}],
        )
    )
    print(
        memdb.create(
            "Asset",
            [
                {"id": 1, "name": "James"},
                {"id": 2, "name": "Gun"},
                {"id": 3, "name": "AstonMartin"},
            ],
        )
    )
    print(
        memdb.create(
            "AssetXShot",
            [
                {"asset_id": 1, "shot_id": 1},
                {"asset_id": 2, "shot_id": 1},
                {"asset_id": 3, "shot_id": 1},
                {"asset_id": 1, "shot_id": 2},
                {"asset_id": 2, "shot_id": 2},
                {"asset_id": 1, "shot_id": 3},
            ],
        )
    )
    print(list(memdb.read("Shot")))
    print(
        list(
            memdb.read(
                "Shot",
                colnames=["*", "Asset.*"],
                joins={
                    "Asset": [
                        ("id", "eq", "AssetXShot", "shot_id"),
                        ("asset_id", "eq", "Asset", "id"),
                    ]
                },
                filters={
                    "or": [
                        {"gt": {"id": 1}, "lt": {"Asset.id": 2}},
                        {"eq": {"Asset.name": "AstonMartin"}},
                    ]
                },
            )
        )
    )
    # print(memdb.update("Shot", {"name": "Other"}, filters={"eq": {"id": 1}}))
    # print(list(memdb.read("Shot")))
    # print(memdb.delete("Shot", filters={"eq": {"id": 2}}))
    # print(list(memdb.read("Shot")))
