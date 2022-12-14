"""
Demo run of the service.

Loads a super basic animation studio schema with in-memory sqlite db.
"""
import click

from sqlalchemy import func, Column, DateTime, Index, Integer, String, Table

from dbsvc import api, service


class SQLiteIDManager(api.IDManager):
    def generate_id(self, table: Table) -> int:
        return super().generate_id(table) >> 32


class Schema(api.Schema):
    def build(self, metadata: api.MetaData):
        project_table = Table(
            "Project",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("name", String, nullable=False, unique=True),
        )

        sequence_table = Table(
            "Sequence",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("name", String, nullable=False),
            Column("project_id", Integer, nullable=False),
        )
        Index(
            "unique_sequence_name_project_id",
            sequence_table.c.name,
            sequence_table.c.project_id,
            unique=True,
        )

        shot_table = Table(
            "Shot",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("name", String, nullable=False),
            Column("sequence_id", Integer, nullable=False),
            Column("frame_start", Integer),
            Column("frame_end", Integer),
        )
        Index(
            "unique_shot_name_sequence_id",
            shot_table.c.name,
            shot_table.c.sequence_id,
            unique=True,
        )

        asset_table = Table(
            "Asset",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("type", String, nullable=False),
            Column("name", String, nullable=False),
            Column("project_id", Integer, nullable=False),
        )
        Index(
            "unique_asset_name_project_id",
            asset_table.c.name,
            asset_table.c.project_id,
            unique=True,
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
            Column("datatype", String, nullable=False),
            Column("version", Integer, nullable=False),
            Column("template", String, nullable=False),
            Column("ext", String, nullable=False),
            Column("frame_range", String),  # eg, 1-10,19
            Column("variant", String),
            Column("status", Integer, default=0),
            Column("created_at", DateTime, nullable=False, default=func.now()),
            Column("created_by", String, nullable=False),
            Column("updated_at", DateTime, default=func.now()),
            Column("updated_by", String, nullable=False),
            Column("approved_at", DateTime),
            Column("approved_by", String),
        )
        Index(
            "unique_output",
            output_table.c.context,
            output_table.c.datatype,
            output_table.c.version,
            unique=True,
        )


def build_service(uri: str, ids: bool = False):
    id_manager = None
    if ids:
        id_manager = SQLiteIDManager() if uri.startswith("sqlite") else api.IDManager()
    return service.build(uri, Schema(), id_manager=id_manager)


@click.group()
@click.pass_context
@click.option(
    "--uri", default="sqlite://", help="Database URI to use, defaults to memory sqlite DB"
)
@click.option(
    "--ids", is_flag=True, flag_value=True, help="If enabled, generates IDs for the schema"
)
# Ideally needs some way to set the schema and/or IDManager
def cli(ctx, uri: str = None, ids: bool = False):
    ctx.obj = build_service(uri, ids=ids)


@cli.command()
@click.pass_context
def run(ctx):
    svc = ctx.obj
    svc.run(host="localhost", port=8080, debug=True, reloader=True)


@cli.command()
@click.pass_context
def routes(ctx):
    svc = ctx.obj
    for route in svc.routes:
        print(f"{route.method:7} {route.rule}")


if __name__ == "__main__":
    cli()
