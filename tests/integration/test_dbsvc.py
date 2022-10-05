import pytest
from dbsvc import exceptions

from dbsvc import api, constants


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
                    "Asset": [
                        ("id", "eq", "AssetXShot", "shot_id"),
                        ("asset_id", "eq", "Asset", "id"),
                    ]
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
                "colnames": ["*", "Asset.*"],
                "joins": {
                    "Asset": [
                        ("id", "eq", "AssetXShot", "shot_id"),
                        ("asset_id", "eq", "Asset", "id"),
                    ]
                },
                "filters": {
                    "or": [
                        {"gt": {"id": 1}, "lt": {"Asset.id": 2}},
                        {"eq": {"Asset.name": "AstonMartin"}},
                    ]
                },
            },
            [
                {"id": 1, "name": "First", "Asset.id": 3, "Asset.name": "AstonMartin"},
                {"id": 2, "name": "Second", "Asset.id": 1, "Asset.name": "James"},
                {"id": 3, "name": "Third", "Asset.id": 1, "Asset.name": "James"},
            ],
        ),
    ],
)
def test_read__success(kwargs, expected):
    memdb = api.Database("sqlite://", debug=True)
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
def test_read__invalid_filters__fails(kwargs):
    memdb = api.Database("sqlite://", debug=True)
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
        {"tablename": "Shot", "joins": {"Asset": [("id", "eq", "Banana", "id")]}},
    ],
)
def test_read__invalid_schema__fails(kwargs):
    memdb = api.Database("sqlite://", debug=True)
    populate_shot_assets(memdb)
    with pytest.raises(exceptions.InvalidSchema):
        list(memdb.read(**kwargs))


def test_batch__success():
    memdb = api.Database("sqlite://", debug=True)
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
    assert ret == [2, 2, 1, 1]

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
def test_batch__errors__transaction_rolled_back(invalid_cmd):
    memdb = api.Database("sqlite://", debug=True)
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
