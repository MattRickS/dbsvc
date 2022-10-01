import click

from dbsvc import service


@click.group()
def cli():
    pass


@cli.command()
def run():
    svc = service.build()
    svc.run(host="localhost", port=8080, debug=True, reloader=True)


@cli.command()
def routes():
    svc = service.build()
    for route in svc.routes:
        print(f"{route.method:7} {route.rule}")


if __name__ == "__main__":
    cli()
