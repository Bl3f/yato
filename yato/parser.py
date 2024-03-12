import os
from dataclasses import dataclass

from sqlglot import exp, parse

from yato.python_context import load_class_from_file_path


def get_table_name(table) -> str:
    """
    Get the name of a SQLGlot Table object with the database and catalog if available.
    :param table: The SQLGlot Table object.
    :return: The name of the table as a string.
    """
    # import pdb; pdb.set_trace()
    if isinstance(table.this, exp.Anonymous):
        if table.this.this == "read_parquet":
            return table.this.expressions[0].this

    db = table.text("db")
    catalog = table.text("catalog")
    output = ""

    if db:
        output += db + "."
    if catalog:
        output += catalog + "."

    return output + table.name


def snake_to_camel(snake_str):
    """
    Convert a snake_case string to camelCase.
    :param snake_str: The snake_case string to convert.
    :return: The camelCase string.
    """
    components = snake_str.split("_")
    return "".join(x.title() for x in components)


def read_and_get_python_instance(filename):
    """
    Read the Python file and return the instance of the class with the same name as the file.
    :param filename: The name of the file to read.
    :return: The instance of the class with the same name as the file.
    """
    name, ext = os.path.splitext(filename)
    if ext == ".py":
        class_name = snake_to_camel(os.path.basename(name))
        klass = load_class_from_file_path(filename, class_name)
        instance = klass()
        return instance


def read_sql(filename) -> str:
    """
    Read the SQL from the given file.
    If the file is a Python file it will read the SQL query using the yato helpers.
    :param filename: The name of the file to read.
    :return: The content of the file as a string.
    """

    name, ext = os.path.splitext(filename)
    if ext == ".py":
        instance = read_and_get_python_instance(filename)
        return instance.source_sql()

    if ext == ".sql":
        with open(filename, "r") as f:
            sql = f.read()
        return sql


def is_select_tree(tree):
    """
    Check if the given SQLGlot tree is a SELECT query.
    :param tree: The SQLGlot tree to check.
    :return: True if the tree is a SELECT query, False otherwise.
    """
    return isinstance(tree, exp.Select) or isinstance(tree, exp.CTE) or isinstance(tree, exp.Pivot)


def is_insert_tree(tree):
    """
    Check if the given SQLGlot tree is an INSERT query.
    :param tree: The SQLGlot tree to check.
    :return: True if the tree is an INSERT query, False otherwise.
    """
    return isinstance(tree, exp.Insert)


def find_select_query(trees: list[exp.Expression]):
    """
    Find the select query in the given list of SQLGlot trees.
    :param trees: SQLGlot trees list.
    :return:
    """
    if sum([is_select_tree(t) for t in trees]) > 1:
        raise ValueError("Only one SELECT query is allowed.")
    if sum([is_insert_tree(t) for t in trees]) == 1:
        return [t for t in trees if is_insert_tree(t)][0]
    return [t for t in trees if is_select_tree(t)][0]


def parse_sql(sql, dialect="duckdb"):
    """
    Parse the given SQL using the SQLGlot parser.
    :param sql: The SQL to parse.
    :param dialect: The dialect to use for parsing the SQL.
    :return: The data returned is a list of SQLGlot trees.
    """
    return parse(sql, dialect=dialect)


def get_tables(sql, dialect="duckdb") -> list[str]:
    """
    Get the tables used in the given SQL.

    :param sql: The SQL to parse.
    :param dialect: The dialect to use for parsing the SQL.
    :return: The data returned is a list of table names used in the SQL.
    """
    trees = parse_sql(sql, dialect=dialect)
    select = find_select_query(trees)
    ctes = [c.alias_or_name for c in select.find_all(exp.CTE)]
    all_tables = [get_table_name(t) for t in select.find_all(exp.Table)]
    return list(set([t for t in all_tables if t not in ctes]))


@dataclass
class Dependency:
    deps: list[str]
    filename: str


def get_dependencies(folder, dialect="duckdb") -> dict:
    """
    Get the dependencies of the files (SQL or Python) in the given folder.

    :param folder: The folder in which the files (SQL or Python) are located.
    :param dialect: The dialect to use for parsing the SQL queries.
    :return: The data returned is a dictionary with the filenames as keys and the tables used in the
             SQL queries as values.
    """
    dependencies = {}
    for dirpath, dirnames, filenames in os.walk(folder):
        for file in filenames:
            name, ext = os.path.splitext(file)
            if ext == ".sql" or ext == ".py":
                filename = os.path.join(dirpath, file)
                sql = read_sql(filename)
                dependencies[name] = Dependency(deps=get_tables(sql, dialect), filename=filename)
    return dependencies
