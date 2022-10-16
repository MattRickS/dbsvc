import uuid
import pytest
from dbsvc import exceptions
from unittest import mock

from dbsvc import api, constants
from sqlalchemy import Column, Index, Integer, String, Table


class TestIDManager(api.IDManager):
    def generate_id(self, table: Table) -> int:
        """sqlite doesn't support 64 bit integers"""
        return int(uuid.uuid1()) >> 96


class TestSchema(api.Schema):
    def build(self, metadata: api.MetaData):
        Table(
            "Shot",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("name", String, nullable=False),
        )
        Table(
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


@pytest.fixture(scope="function")
def memdb():
    return api.Database("sqlite://", TestSchema(), id_manager=TestIDManager(), debug=True)


def populate_shot_assets(db: api.Database):
    db.create(
        "Shot",
        [{"id": 1, "name": "First"}, {"id": 2, "name": "Second"}, {"id": 3, "name": "Third"}],
    )
    db.create(
        "Asset",
        [{"id": 1, "name": "James"}, {"id": 2, "name": "Gun"}, {"id": 3, "name": "AstonMartin"}],
    )
    db.create(
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


def test_create__id_manager__success(memdb):
    to_create = [
        {"name": "a"},
        {"name": "b", "id": 123},
        {"name": "c"},
    ]
    ids = memdb.create("Shot", to_create)

    # Created the correct number of IDs
    assert len(ids) == len(to_create)

    # Provided entities were not modified
    assert "id" not in to_create[0]
    assert to_create[1]["id"] == 123
    assert "id" not in to_create[2]

    # Provided ID was kept
    assert ids[1] == 123

    # Integer IDs were used for all entities
    assert all(isinstance(val, int) for val in ids)

    # IDs are all functional
    shots = list(
        memdb.read("Shot", filters={"in": {"id": ids}}, ordering=[("name", constants.Order.Asc)])
    )
    assert shots == [
        {"name": "a", "id": ids[0]},
        {"name": "b", "id": 123},
        {"name": "c", "id": ids[2]},
    ]


@pytest.mark.parametrize(
    "kwargs, expected",
    [
        # All rows
        (
            {"tablename": "Shot"},
            [{"id": 1, "name": "First"}, {"id": 2, "name": "Second"}, {"id": 3, "name": "Third"}],
        ),
        # Explicit columns
        (
            {"tablename": "Shot", "colnames": ["*"]},
            [{"id": 1, "name": "First"}, {"id": 2, "name": "Second"}, {"id": 3, "name": "Third"}],
        ),
        (
            {"tablename": "Shot", "colnames": ["name"]},
            [{"name": "First"}, {"name": "Second"}, {"name": "Third"}],
        ),
        # Filters
        (
            {"tablename": "Shot", "filters": {"eq": {"id": 2}}},
            [{"id": 2, "name": "Second"}],
        ),
        (
            {"tablename": "Shot", "filters": {"like": {"name": "%d"}}},
            [{"id": 2, "name": "Second"}, {"id": 3, "name": "Third"}],
        ),
        (
            {"tablename": "Shot", "filters": {"or": [{"eq": {"id": 1}}, {"eq": {"id": 3}}]}},
            [{"id": 1, "name": "First"}, {"id": 3, "name": "Third"}],
        ),
        (
            {"tablename": "Shot", "filters": {"in": {"id": [1, 3]}}},
            [{"id": 1, "name": "First"}, {"id": 3, "name": "Third"}],
        ),
        # Ordering
        (
            {"tablename": "Shot", "ordering": [("id", constants.Order.Desc)]},
            [{"id": 3, "name": "Third"}, {"id": 2, "name": "Second"}, {"id": 1, "name": "First"}],
        ),
        (
            {
                "tablename": "Shot",
                "joins": {
                    "ShotAssets": ("Shot", "id", "eq", "AssetXShot", "shot_id"),
                    "Asset": ("ShotAssets", "asset_id", "eq", "Asset", "id"),
                },
                "ordering": [
                    ("Asset.id", constants.Order.Desc),
                    ("name", constants.Order.Asc),
                ],
            },
            [
                {"id": 1, "name": "First"},
                {"id": 1, "name": "First"},
                {"id": 2, "name": "Second"},
                {"id": 1, "name": "First"},
                {"id": 2, "name": "Second"},
                {"id": 3, "name": "Third"},
            ],
        ),
        # Complex query
        (
            {
                "tablename": "Shot",
                "colnames": ["*", "AssetAlias.*"],
                "joins": {
                    "ShotAssets": ("Shot", "id", "eq", "AssetXShot", "shot_id"),
                    "AssetAlias": ("ShotAssets", "asset_id", "eq", "Asset", "id"),
                },
                "filters": {
                    "or": [
                        {"gt": {"id": 1}, "lt": {"AssetAlias.id": 2}},
                        {"eq": {"AssetAlias.name": "AstonMartin"}},
                    ]
                },
            },
            [
                {"id": 1, "name": "First", "AssetAlias.id": 3, "AssetAlias.name": "AstonMartin"},
                {"id": 2, "name": "Second", "AssetAlias.id": 1, "AssetAlias.name": "James"},
                {"id": 3, "name": "Third", "AssetAlias.id": 1, "AssetAlias.name": "James"},
            ],
        ),
    ],
)
def test_read__success(memdb, kwargs, expected):
    populate_shot_assets(memdb)
    entities = list(memdb.read(**kwargs))
    assert entities == expected


@pytest.mark.parametrize(
    "kwargs",
    [
        # Missing comparison method
        {"tablename": "Shot", "filters": {"name": "First"}},
        # Invalid comparison method
        {"tablename": "Shot", "filters": {"is": {"name": "First"}}},
        # "or" requires a list of filters
        {"tablename": "Shot", "filters": {"or": {"name": "First"}}},
        # "in" requires a list of values
        {"tablename": "Shot", "filters": {"in": {"id": 1}}},
    ],
)
def test_read__invalid_filters__fails(memdb, kwargs):
    populate_shot_assets(memdb)
    with pytest.raises(exceptions.InvalidFilters):
        list(memdb.read(**kwargs))


@pytest.mark.parametrize(
    "kwargs",
    [
        # Not a table
        {"tablename": "Banana"},
        # Not a column on the table
        {"tablename": "Shot", "colnames": ["banana"]},
        # Requires table without a join
        {"tablename": "Shot", "colnames": ["Asset.name"]},
        # Requests alias without a matching join
        {"tablename": "Shot", "filters": {"eq": {"Asset.name": "Gun"}}},
        # Join table is invalid
        {"tablename": "Shot", "joins": {"Asset": ("Shot", "id", "eq", "Banana", "id")}},
    ],
)
def test_read__invalid_schema__fails(memdb, kwargs):
    populate_shot_assets(memdb)
    with pytest.raises(exceptions.InvalidSchema):
        list(memdb.read(**kwargs))


def test_batch__success(memdb):
    batch = [
        {
            "cmd": "create",
            "kwargs": {
                "tablename": "Shot",
                "list_of_values": [
                    {"id": 123, "name": "ShotA"},
                    {"id": 456, "name": "ShotB"},
                ],
            },
        },
        {
            "cmd": "create",
            "kwargs": {
                "tablename": "Asset",
                "list_of_values": [
                    {"id": 321, "name": "AssetA"},
                    {"id": 654, "name": "AssetB"},
                ],
            },
        },
        {
            "cmd": "update",
            "kwargs": {
                "tablename": "Shot",
                "filters": {"eq": {"id": 123}},
                "values": {"name": "ShotC"},
            },
        },
        {
            "cmd": "delete",
            "kwargs": {
                "tablename": "Asset",
                "filters": {"gt": {"id": 500}},
            },
        },
    ]
    ret = memdb.batch(batch)
    # Return value of each command in order
    assert ret == [[mock.ANY, mock.ANY], [mock.ANY, mock.ANY], 1, 1]

    assert list(memdb.read("Shot")) == [
        {"id": 123, "name": "ShotC"},
        {"id": 456, "name": "ShotB"},
    ]
    assert list(memdb.read("Asset")) == [{"id": 321, "name": "AssetA"}]


@pytest.mark.parametrize(
    "invalid_cmd",
    [
        # Invalid type
        [],
        # No arguments
        {},
        # Invalid command
        {"cmd": "not a real command", "kwargs": {"tablename": "Shot"}},
        # Missing kwargs
        {"cmd": "create"},
        # Read commands not supported
        {"cmd": "read", "kwargs": {"tablename": "Shot"}},
        # Invalid types
        {
            "cmd": "create",
            "kwargs": {
                "tablename": "Shot",
                "list_of_values": [{"name": 1, "id": "abc"}],
            },
        },
    ],
)
def test_batch__errors__transaction_rolled_back(memdb, invalid_cmd):
    batch = [
        {
            "cmd": "create",
            "kwargs": {
                "tablename": "Shot",
                "list_of_values": [
                    {"id": 123, "name": "ShotA"},
                    {"id": 456, "name": "ShotB"},
                ],
            },
        },
        {
            "cmd": "create",
            "kwargs": {
                "tablename": "Asset",
                "list_of_values": [
                    {"id": 321, "name": "AssetA"},
                    {"id": 654, "name": "AssetB"},
                ],
            },
        },
        {
            "cmd": "update",
            "kwargs": {
                "tablename": "Shot",
                "filters": {"eq": {"id": 123}},
                "values": {"name": "ShotC"},
            },
        },
        {
            "cmd": "delete",
            "kwargs": {
                "tablename": "Asset",
                "filters": {"gt": {"id": 500}},
            },
        },
        invalid_cmd,
    ]
    with pytest.raises(exceptions.InvalidBatchCommand):
        memdb.batch(batch)

    assert list(memdb.read("Shot")) == []
    assert list(memdb.read("Asset")) == []
