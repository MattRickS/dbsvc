from __future__ import annotations
from contextlib import contextmanager
from typing import Any, Dict, Iterator, List, Tuple, Union, TYPE_CHECKING

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
import sqlalchemy

from dbsvc import constants, exceptions

if TYPE_CHECKING:
    from sqlalchemy.sql.elements import ClauseElement, Label
    from sqlalchemy.engine import Connection

    VALUE_T = Union[str, int, float, bool]
    FILTER_VALUES_T = Dict[str, VALUE_T]
    # {cmp: {column: value}, or: [{cmp: {column: value}}]}
    FILTERS_T = Dict[str, Union[FILTER_VALUES_T, List["FILTERS_T"]]]
    ENTITY_T = COL_VALUES_T = Dict[str, VALUE_T]
    # {alias: [(column, method, table, column), ...]}
    JOINS_T = Dict[str, List[Tuple[str, str, str, str]]]
    ALIAS_TABLES_T = Dict[str, Table]


def uri(
    dialect: str,
    driver: str = None,
    user: str = None,
    password: str = None,
    host: str = None,
    port: int = None,
    database: str = None,
):
    """
    Generates a suitable sqlalchemy db URI in the format

        dialect+driver://username:password@host:port/database

    Examples:
        > uri("sqlite", database="/path/to/file.sqlite")
        sqlite:////path/to/file.sqlite

        > uri("postgresql", user="root", host="localhost")
        postgresql://root@localhost
    """
    uri = dialect
    if driver:
        uri += f"+{driver}"
    uri += "://"
    if user:
        uri += user
        if password:
            uri += f":{password}"
    if host:
        uri += f"@{host}"
        if port:
            uri += f":{port}"
    if database:
        uri += f"/{database}"

    return uri


@contextmanager
def sql_exception_handler():
    try:
        yield
    except exc.IntegrityError as e:
        raise exceptions.DatabaseError(e) from e


class Database:
    def __init__(self, uri: str, debug: bool = False) -> None:
        self._engine = create_engine(uri, echo=debug)
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
        Index(
            "asset_shot",
            asset_shot_table.c.asset_id,
            asset_shot_table.c.shot_id,
            unique=True,
        )

        return metadata

    def create(
        self,
        tablename: str,
        list_of_values: List["ENTITY_T"],
        transaction: "Connection" = None,
    ) -> int:
        """Inserts multiple rows into the database"""
        table = self._table(tablename)
        # XXX: Generate IDs?
        with self._transaction(transaction=transaction) as conn:
            with sql_exception_handler():
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
        transaction: "Connection" = None,
    ) -> Iterator["ENTITY_T"]:
        """Reads `colnames` for all rows matching `filters`"""
        table = self._table(tablename)
        alias_tables = self._validate_joins(joins) if joins else None
        columns = self._select_columns(table, colnames, alias_tables=alias_tables)
        stmt = select(*columns)

        if joins:
            stmt = self._joins(stmt, table, joins)

        if filters:
            where_clause = self._filters(table, filters, alias_tables=alias_tables)
            stmt = stmt.where(where_clause)

        if ordering:
            order_by = []
            for colname, order in ordering:
                column = self._column(table, colname, alias_tables=alias_tables)
                method = asc if order == constants.Order.Asc else desc
                order_by.append(method(column))
            stmt = stmt.order_by(*order_by)

        if limit:
            stmt = stmt.limit(limit)

        with self._transaction(transaction=transaction) as txn:
            with sql_exception_handler():
                rows = txn.execute(stmt).fetchall()

        return map(dict, rows)

    def update(
        self,
        tablename: str,
        values: "COL_VALUES_T",
        filters: "FILTERS_T" = None,
        transaction: "Connection" = None,
    ) -> int:
        """Updates the given values on all rows matching filters"""
        # TODO: bindparam options to perform different updates per row?
        table = self._table(tablename)
        stmt = update(table)
        if filters:
            where_clause = self._filters(table, filters)
            stmt = stmt.where(where_clause)

        stmt = stmt.values(values)
        with self._transaction(transaction=transaction) as conn:
            with sql_exception_handler():
                cursor = conn.execute(stmt)
                return cursor.rowcount

    def delete(
        self,
        tablename: str,
        filters: "FILTERS_T" = None,
        transaction: "Connection" = None,
    ) -> int:
        """Deletes all rows matching filters"""
        table = self._table(tablename)
        stmt = delete(table)
        if filters:
            where_clause = self._filters(table, filters)
            stmt = stmt.where(where_clause)

        with self._transaction(transaction=transaction) as txn:
            with sql_exception_handler():
                cursor = txn.execute(stmt)
                return cursor.rowcount

    def batch(
        self,
        cmds: List[Dict[str, Union[str, Dict[str, Any]]]],
        transaction: "Connection" = None,
    ) -> List[Union[List[int], int]]:
        """
        Executes multiple CUD (no read) commands in a single transaction.

        Either all commands will succeed or no changes will be made.

        Example:
            # Creates, updates, and deletes the same entity.
            # Even if update/delete failed, there will never be an entity left in the db
            batch([
                {
                    "cmd": "create",
                    "kwargs": {
                        "entity_type": "Shot",
                        "list_of_values": [{"name": "abc"}]
                    }
                },
                {
                    "cmd": "update",
                    "kwargs": {
                        "entity_type": "Shot",
                        "values": [{"name": "def"}],
                        "filters": {"eq": {"name": "abc"}}
                    }
                },
                {
                    "cmd": "delete",
                    "kwargs": {
                        "entity_type": "Shot",
                        "filters": {"eq": {"name": "def"}}
                    }
                },
            ])

        Returns:
            List of return values for each call in the same order as provided.
        """
        with self._transaction(transaction=transaction) as txn:
            results = []
            for i, cmd in enumerate(cmds):
                try:
                    if cmd["cmd"] == "read":
                        raise exceptions.InvalidBatchCommand("Read commands unsupported by batch", i)
                    results.append(getattr(self, cmd["cmd"])(**cmd["kwargs"], transaction=txn))
                except (
                    AttributeError,
                    KeyError,
                    TypeError,
                    exceptions.DatabaseError,
                ) as e:
                    raise exceptions.InvalidBatchCommand(e, i)

        return results

    # ==================================================================================
    # Private

    @contextmanager
    def _transaction(self, transaction: "Connection" = None) -> Iterator["Connection"]:
        """Opens or reuses a database connection"""
        if transaction is not None:
            yield transaction
        else:
            with self._engine.begin() as txn:
                yield txn

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
        self, table: Table, colname: str, alias_tables: "ALIAS_TABLES_T" = None
    ) -> Tuple[Table, str]:
        """
        Column name format can be either '{column}' or '{table}.{column}'.
        The former will return the default table and colname.
        The latter will lookup the table to use from alias_tables and split the colname.
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

        if not alias_tables or alias not in alias_tables:
            raise exceptions.InvalidSchema(f"Column requires alias that wasn't provided: {alias}")

        alias_table = alias_tables[alias]
        return (alias_table, colname)

    def __column(self, table: Table, colname: str):
        """Fetches the column from the table, raises an exception if invalid"""
        try:
            return getattr(table.c, colname)
        except AttributeError:
            raise exceptions.InvalidSchema(f"Table {table.name} has no column {colname}")

    def _column(self, table: Table, colname: str, alias_tables: "ALIAS_TABLES_T" = None) -> Union[Column, Table]:
        """Fetches the column from the default or joined table"""
        table, colname = self._table_and_colname(table, colname, alias_tables=alias_tables)
        return self.__column(table, colname)

    def _select_columns(self, table: Table, columns: List[str], alias_tables: "ALIAS_TABLES_T") -> List["Label"]:
        """
        Generates the columns to use in a select query with suitable labels.

        If the given column name does not define a table/alias, the colname is the label.
        If a table/alias is defined, the full table.column name is used as the label.
        Wildcards are expanded but follow the same rules when which label is used.
        """
        for name in columns:
            coltable, colname = self._table_and_colname(table, name, alias_tables=alias_tables)
            if colname == constants.ALL_COLUMNS:
                for column in coltable.columns:
                    # Can't use `name`, it might be a wildcard. Explicitly join table and column
                    yield column.label(f"{coltable.name}.{column.name}" if "." in name else column.name)
            else:
                yield self.__column(coltable, colname).label(name)

    def _cmp(self, column: Column, method: str, value: "VALUE_T") -> "ClauseElement":
        """Calls a comparison function for the column and value, eg, "eq" -> column.__eq__(value)"""
        try:
            funcname = constants.COMPARISON_MAP[method]
            return getattr(column, funcname)(value)
        except exc.ArgumentError as e:
            raise exceptions.InvalidComparison(
                f"Comparison '{method}' got unexpected type {type(value)} for column '{column.name}'"
            ) from e
        except KeyError:
            raise exceptions.InvalidComparison(f"'{method}' is not a valid comparison method")

    def _validate_joins(self, joins: "JOINS_T") -> "ALIAS_TABLES_T":
        """Ensures joins are in a valid format and returns a mapping of alias to the final table"""
        alias_tables = {}
        for alias, join_steps in joins.items():
            if not join_steps:
                raise exceptions.InvalidJoin("Joins require at least one set of fields")
            for steps in join_steps:
                if len(steps) != 4:
                    raise exceptions.InvalidJoin("Each join step requires four parameters")
            alias_tables[alias] = self._table(join_steps[-1][-2])

        return alias_tables

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

    def _filters(self, table: Table, filters: "FILTERS_T", alias_tables: "ALIAS_TABLES_T" = None) -> "ClauseElement":
        """Generates the where clause for the filters"""
        stmts = []
        for key, value in filters.items():
            if key == constants.FILTER_OR:
                if not isinstance(value, (list, tuple)):
                    raise exceptions.InvalidFilters(f"'or' filter requires a list of filters, got {type(value)}")
                stmts.append(
                    or_(
                        *(
                            self._filters(table, subfilters, alias_tables=alias_tables).self_group()
                            for subfilters in value
                        )
                    ).self_group()
                )
            elif key not in constants.COMPARISON_MAP:
                raise exceptions.InvalidFilters(f"'{key}' is not a valid comparison method")
            elif not isinstance(value, dict):
                raise exceptions.InvalidFilters(
                    f"'{key}' filter requires a dictionary of columns and values, got {type(value)}"
                )
            else:
                for colname, val in value.items():
                    # Let InvalidSchema errors raise separately
                    column = self._column(table, colname, alias_tables=alias_tables)
                    try:
                        stmts.append(self._cmp(column, key, val))
                    except exceptions.InvalidComparison as e:
                        raise exceptions.InvalidFilters(str(e)) from e

        return and_(*stmts)
