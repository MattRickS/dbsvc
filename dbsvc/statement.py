import re


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

    def _is(self, values):
        return self._and(f"{self.field(k)} = {v}" for k, v in values.items())

    def _is_not(self, values):
        return self._and(f"{self.field(k)} != {v}" for k, v in values.items())

    def _like(self, values):
        return self._and(f"{self.field(k)} LIKE {v}" for k, v in values.items())

    def _unlike(self, values):
        return self._and(f"{self.field(k)} NOT LIKE {v}" for k, v in values.items())

    def _in(self, values):
        return self._and(f"{self.field(k)} IN {v}" for k, v in values.items())

    def _not_in(self, values):
        return self._and(f"{self.field(k)} NOT IN {v}" for k, v in values.items())

    FILTER_METHODS = {
        "is": _is,
        "is_not": _is_not,
        "like": _like,
        "unlike": _unlike,
        "in": _in,
        "not_in": _not_in,
        "or": _or,
    }

    def filters(self, filters):
        return self._and(
            self.FILTER_METHODS[key](self, value) for key, value in filters.items()
        )

    def parse_joins(self, joins):
        # Self joins are supported, but the joins key must be an alias name
        assert self.cls not in joins
        # field[method:class.field] where method is optional, EG,
        # # "asset_id[is:Asset.id]"
        # # "asset_id[Asset.id]"
        PATTERN = r"(\w+)\[(?:(\w+):)?(\w+)\.(\w+)\]"
        stmt_joins = []
        for alias in self.aliases:
            curralias = self.cls
            matches = re.findall(PATTERN, joins[alias])
            for i, (field, join_method, join_class, join_field) in enumerate(matches):
                method = {"is": "=", None: "=", "": "="}[join_method]
                newalias = alias if i == len(matches) - 1 else f"{alias}{i}"
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
            joins={"Asset": "id[AssetShot.shot_id].asset_id[Asset.id]"},
            fields=["name"],
        )
    )
    print(
        Statement.read(
            "Shot",
            joins={"Asset": "id[AssetShot.shot_id].asset_id[Asset.id]"},
            fields=["name", "Asset.name", "Asset.other"],
        )
    )
    print(
        Statement.read(
            "Shot",
            joins={
                "Asset": "id[AssetShot.shot_id].asset_id[Asset.id]",
                "Camera": "id[Camera.shot_id]",
            },
            fields=["name", "Asset.name", "Camera.*"],
        )
    )
    print(
        Statement.read(
            "Shot",
            joins={
                "Asset": "id[AssetShot.shot_id].asset_id[Asset.id]",
                "Camera": "id[Camera.shot_id]",
            },
            fields=["name", "Asset.name", "Camera.*"],
            filters={"is": {"Asset.name": "apple"}},
        )
    )
    print(
        Statement.read(
            "Shot",
            fields=["name"],
            filters={
                "or": [{"is": {"name": "banana", "frames": 100}}, {"is": {"name": "apple"}}]
            },
        )
    )
    print(
        Statement.read(
            "Shot",
            joins={"Asset": "id[AssetShot.shot_id].asset_id[Asset.id]"},
            filters={
                "or": [
                    {"is": {"name": "banana", "frames": 100}},
                    {"is": {"Asset.name": "apple"}},
                ]
            },
        )
    )
    print(
        Statement.read(
            "Shot",
            joins={"Asset": "id[AssetShot.shot_id].asset_id[Asset.id]"},
            filters={
                "or": [
                    {"is": {"name": "banana", "frames": 100}},
                    {"is": {"Asset.name": "apple"}},
                ],
                "like": {"sequence": "sq1%"},
            },
        )
    )
