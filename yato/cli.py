import click
import duckdb

from yato import Yato


@click.group()
def cli():
    pass


@cli.command()
@click.argument("sql")
@click.option("--db", help="Path to the DuckDB database.", default="yato.duckdb", show_default=True)
@click.option("--schema", help="The schema to use in the DuckDB database.", default="transform")
@click.option(
    "--ui",
    help="Open the local DuckDB UI upon run completion (requires DuckDB >= v1.2.1)",
    is_flag=True,
    default=False,
    show_default=True,
)
def run(sql, db, schema, ui):
    """
    Run yato against a DuckDB database using the SQL files.

    SQL is Folder in which the SQL files are located.
    """
    yato = Yato(
        database_path=db,
        sql_folder=sql,
        schema=schema,
    )

    try:
        con, context = yato.run()
    except Exception as err:
        raise err

    if duckdb.__version__ >= "1.2.1":
        if ui:
            with context.console.status("[bold green]Started DuckDB UI. Press CTRL+C to exit..."):
                context.sql("call start_ui()")
                try:
                    while True:
                        pass
                except KeyboardInterrupt:
                    pass
    else:
        if ui:
            context.console.print(
                "[bold yellow]DuckDB UI requires version >= 1.2.1. Please update DuckDB to use this feature."
            )


if __name__ == "__main__":
    cli()
