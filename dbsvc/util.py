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
