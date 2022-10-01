from typing import Dict, List, Tuple


class Statement:
    def __init__(self, cls) -> None:
        self.cls = cls
        self.aliases = set()

    def field(self, field):
        parts = field.split(".")
        if len(parts) == 1:
            field = f"{self.cls}.{field}"
        else:
            self.aliases.add(parts[0])
        if len(parts) > 2:
            raise Exception("Too many field components")
        return f"`{field}`"

    # def _and(cls, values):
    #     return " AND ".join(filters_stmt(cls, filters) for filters in values)
    def _and(self, values):
        return " AND ".join(values)

    def _or(self, values):
        return f"({' OR '.join(f'({self.filters(filters)})' for filters in values)})"

    def _eq(self, values):
        return self._and(f"{self.field(k)} = {v}" for k, v in values.items())

    def _ne(self, values):
        return self._and(f"{self.field(k)} != {v}" for k, v in values.items())

    def _like(self, values):
        return self._and(f"{self.field(k)} LIKE {v}" for k, v in values.items())

    def _unlike(self, values):
        return self._and(f"{self.field(k)} NOT LIKE {v}" for k, v in values.items())

    def _in(self, values):
        return self._and(f"{self.field(k)} IN {v}" for k, v in values.items())

    def _not_in(self, values):
        return self._and(f"{self.field(k)} NOT IN {v}" for k, v in values.items())

    def _le(self, values):
        return self._and(f"{self.field(k)} <= {v}" for k, v in values.items())

    def _lt(self, values):
        return self._and(f"{self.field(k)} < {v}" for k, v in values.items())

    def _ge(self, values):
        return self._and(f"{self.field(k)} >= {v}" for k, v in values.items())

    def _gt(self, values):
        return self._and(f"{self.field(k)} > {v}" for k, v in values.items())

    FILTER_METHODS = {
        "eq": _eq,
        "ne": _ne,
        "like": _like,
        "unlike": _unlike,
        "in": _in,
        "not_in": _not_in,
        "le": _le,
        "lt": _lt,
        "ge": _ge,
        "gt": _gt,
        "or": _or,
    }

    def filters(self, filters):
        return self._and(self.FILTER_METHODS[key](self, value) for key, value in filters.items())

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

        read("Shot", ["assets"]) -> implicitly uses default_fields SELECT `assets.*`
        read("Shot", ["assets.name"]) -> explicit SELECT `assets.name`
    """

    def parse_joins(self, joins: Dict[str, List[Tuple[str, str, str, str]]]) -> str:
        # Self joins are supported, but the joins key must be an alias name
        assert self.cls not in joins
        stmt_joins = []
        for alias in self.aliases:
            curralias = self.cls
            for i, (field, join_method, join_class, join_field) in enumerate(joins[alias]):
                method = {"eq": "=", None: "=", "": "="}[join_method]
                newalias = alias if i == len(joins) - 1 else f"{alias}{i}"
                stmt_joins.append(
                    f"JOIN `{join_class}` AS `{newalias}` ON `{curralias}.{field}` {method} `{newalias}.{join_field}`"
                )
                curralias = newalias
        return stmt_joins

    @classmethod
    def read(self, cls, fields=("*",), filters=None, joins=None):
        stmt = Statement(cls)
        stmt_fields = [stmt.field(field) for field in fields]
        filter_stmt = "" if filters is None else stmt.filters(filters)

        joins = joins or {}
        missing = stmt.aliases.difference(joins)
        if missing:
            raise Exception(f"Missing joins for requested aliases: {missing}")

        args = [f"SELECT {','.join(stmt_fields)} FROM {cls}"]
        args.extend(stmt.parse_joins(joins))
        if filter_stmt:
            args.append("WHERE")
            args.append(filter_stmt)

        return " ".join(args) + ";"


if __name__ == "__main__":
    print(
        Statement.read(
            "Shot",
            joins={
                "Asset": [("id", "eq", "AssetShot", "shot_id"), ("asset_id", "eq", "Asset", "id")]
            },
            fields=["name"],
        )
    )
    print(
        Statement.read(
            "Shot",
            joins={
                "Asset": [("id", "eq", "AssetShot", "shot_id"), ("asset_id", "eq", "Asset", "id")]
            },
            fields=["name", "Asset.name", "Asset.other"],
        )
    )
    print(
        Statement.read(
            "Shot",
            joins={
                "Asset": [("id", "eq", "AssetShot", "shot_id"), ("asset_id", "eq", "Asset", "id")],
                "Camera": [("id", "eq", "Camera", "shot_id")],
            },
            fields=["name", "Asset.name", "Camera.*"],
        )
    )
    print(
        Statement.read(
            "Shot",
            joins={
                "Asset": [("id", "eq", "AssetShot", "shot_id"), ("asset_id", "eq", "Asset", "id")],
                "Camera": [("id", "eq", "Camera", "shot_id")],
            },
            fields=["name", "Asset.name", "Camera.*"],
            filters={"eq": {"Asset.name": "apple"}},
        )
    )
    print(
        Statement.read(
            "Shot",
            fields=["name"],
            filters={"or": [{"eq": {"name": "banana", "frames": 100}}, {"eq": {"name": "apple"}}]},
        )
    )
    print(
        Statement.read(
            "Shot",
            joins={
                "Asset": [("id", "eq", "AssetShot", "shot_id"), ("asset_id", "eq", "Asset", "id")]
            },
            filters={
                "or": [
                    {"eq": {"name": "banana", "frames": 100}},
                    {"eq": {"Asset.name": "apple"}},
                ]
            },
        )
    )
    print(
        Statement.read(
            "Shot",
            joins={
                "Asset": [("id", "eq", "AssetShot", "shot_id"), ("asset_id", "eq", "Asset", "id")]
            },
            filters={
                "or": [
                    {"eq": {"name": "banana", "frames": 100}},
                    {"eq": {"Asset.name": "apple"}},
                ],
                "like": {"sequence": "sq1%"},
            },
        )
    )
