import click

from yato import Yato


@click.group()
def cli():
    pass


@cli.command()
@click.argument("sql")
@click.option("--db", help="Path to the DuckDB database.", default="yato.duckdb", show_default=True)
@click.option("--schema", help="Path to the DuckDB database.", default="transform")
@click.option("--dialect", help="The SQL dialect to use as backend database.", default="duckdb", show_default=True)
def run(sql, db, schema, dialect):
    """
    Run yato against a DuckDB database using the SQL files.

    SQL is Folder in which the SQL files are located.
    """
    yato = Yato(
        database_path=db,
        sql_folder=sql,
        schema=schema,
        dialect=dialect,
    )

    yato.run()


if __name__ == "__main__":
    cli()
