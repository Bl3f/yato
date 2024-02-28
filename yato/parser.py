import os

from sqlglot import exp, parse_one


def get_table_name(table) -> str:
    """
    Get the name of a SQLGlot Table object with the database and catalog if available.
    :param table: The SQLGlot Table object.
    :return: The name of the table as a string.
    """
    if isinstance(table.this, exp.Anonymous):
        if table.this.this == 'read_parquet':
            return table.this.expressions[0].this

    db = table.text("db")
    catalog = table.text("catalog")
    output = ""

    if db:
        output += db + "."
    if catalog:
        output += catalog + "."

    return output + table.name


def read_sql(filename) -> str:
    """
    Read the SQL from the given file.
    :param filename: The name of the file to read.
    :return: The content of the file as a string.
    """
    with open(filename, "r") as f:
        sql = f.read()
    return sql


def get_tables(sql, dialect="duckdb") -> list[str]:
    """
    Get the tables used in the given SQL.

    :param sql: The SQL to parse.
    :param dialect: The dialect to use for parsing the SQL.
    :return: The data returned is a list of table names used in the SQL.
    """
    tree = parse_one(sql, dialect=dialect)
    ctes = [c.alias_or_name for c in tree.find_all(exp.CTE)]
    all_tables = [get_table_name(t) for t in tree.find_all(exp.Table)]
    return list(set([t for t in all_tables if t not in ctes]))


def get_dependencies(sql_folder, dialect="duckdb") -> dict:
    """
    Get the dependencies of the SQL files in the given folder.

    :param sql_folder: The folder in which the SQL files are located.
    :param dialect: The dialect to use for parsing the SQL files.
    :return: The data returned is a dictionary with the SQL file names as keys and the tables used in the
             SQL files as values.
    """
    dependencies = {}
    for file in os.listdir(sql_folder):
        name, ext = os.path.splitext(file)
        filename = os.path.join(sql_folder, file)
        sql = read_sql(filename)
        dependencies[name] = get_tables(sql, dialect)
    return dependencies
