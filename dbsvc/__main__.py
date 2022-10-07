import click

from sqlalchemy import Column, Index, Integer, String, Table

from dbsvc import api, service


class SQLiteIDManager(api.IDManager):
    def generate_id(self, table: Table) -> int:
        return super().generate_id(table) >> 32


class Schema(api.Schema):
    def build(self, metadata: api.MetaData):
        Table(
            "Shot",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("name", String, nullable=False, unique=True),
        )
        Table(
            "Asset",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("name", String, nullable=False, unique=True),
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
        output_table = Table(
            "Output",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("context", String, nullable=False),
            Column("type", String, nullable=False),
            Column("name", String, nullable=False),
            Column("version", Integer, nullable=False),
        )
        Index(
            "unique_output",
            output_table.c.context,
            output_table.c.type,
            output_table.c.name,
            output_table.c.version,
            unique=True,
        )


def build_service():
    return service.build("sqlite://", Schema(), id_manager=SQLiteIDManager())


@click.group()
def cli():
    pass


@cli.command()
def run():
    svc = build_service()
    svc.run(host="localhost", port=8080, debug=True, reloader=True)


@cli.command()
def routes():
    svc = build_service()
    for route in svc.routes:
        print(f"{route.method:7} {route.rule}")


if __name__ == "__main__":
    cli()
