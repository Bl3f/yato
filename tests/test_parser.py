from pathlib import Path

import pytest
from sqlglot import exp

from yato.parser import (
    Dependency,
    find_select_query,
    get_dependencies,
    get_table_name,
    get_tables,
    parse_sql,
    read_and_get_python_instance,
    read_sql,
    snake_to_camel,
)


def create_table(name=None, db=None, catalog=None, func=None):
    return exp.Table(
        this=exp.Identifier(this=name) if name else func,
        db=exp.Identifier(this=db) if db else None,
        catalog=exp.Identifier(this=catalog) if catalog else None,
    )


def test_get_table_name_simple():
    names = ["orders", "sales.orders", "production.sales.orders"]
    for name in names:
        assert get_table_name(create_table(name)) == name


def test_get_table_name_anonymous_read_parquet():
    table = create_table(func=exp.Anonymous(this="read_parquet", expressions=[exp.Literal(this="file.parquet")]))
    assert get_table_name(table) == "file.parquet"


def test_snake_to_camel():
    assert snake_to_camel("snake_case") == "SnakeCase"
    assert snake_to_camel("snake") == "Snake"


def test_read_and_get_one_python_instance_with_python_file(tmp_path):
    instance = read_and_get_python_instance("tests/files/module.py")
    assert type(instance).__name__ == "Module"
    # Also test that the class name does not need to match the file name.
    temp_file = tmp_path / "mock_module.py"
    temp_file.write_bytes(b"\n\nclass MockClass:\n    pass\n")
    instance = read_and_get_python_instance(temp_file)
    assert type(instance).__name__ == "MockClass"


def test_read_and_get_multi_python_instance_with_python_file(tmp_path):
    """Test that the function raises an error when multiple classes are defined in the Python file."""
    temp_file = tmp_path / "mock_module.py"
    temp_file.write_bytes(Path("tests/files/module.py").read_bytes() + b"\n\nclass MockClass:\n    pass\n")
    with pytest.raises(ValueError, match="Only one Transformation class is allowed."):
        read_and_get_python_instance(temp_file)


def test_read_and_get_none_python_instance_with_python_file(tmp_path):
    """Test that the function raises an error when no classes are defined in the Python file."""
    temp_file = tmp_path / "mock_module.py"
    temp_file.write_bytes(b"print('Hello, World!')")
    with pytest.raises(ValueError, match="No Transformation class found in the Python file:"):
        read_and_get_python_instance(temp_file)


def test_read_and_get_python_instance_with_sql_file():
    instance = read_and_get_python_instance("tests/files/query.sql")
    assert instance is None


def test_read_sql_python():
    sql = read_sql("tests/files/module.py")
    assert sql == "SELECT * FROM something"


def test_read_sql_sql():
    sql = read_sql("tests/files/query.sql")
    assert sql == "SELECT 42"


def test_find_select_query_raises():
    trees = [
        exp.Select(),
        exp.Select(),
        exp.Insert(),
    ]
    with pytest.raises(ValueError, match="Only one SELECT query is allowed."):
        find_select_query(trees)


def test_find_select_query():
    trees = [
        exp.Insert(),
        exp.Select(),
        exp.Insert(),
    ]
    assert find_select_query(trees) == trees[1]


def test_find_select_query():
    trees = [
        exp.Insert(),
        exp.Select(),
    ]
    assert find_select_query(trees) == trees[0]


def test_parse_sql():
    parsed = parse_sql("SELECT 42")
    assert isinstance(parsed, list)
    assert isinstance(parsed[0], exp.Expression)


def test_get_tables():
    tables = get_tables("SELECT * FROM orders")
    assert tables == ["orders"]

    tables = get_tables("SELECT * FROM orders LEFT JOIN products ON orders.product_id = products.id")
    assert frozenset(tables) == frozenset(["orders", "products"])

    tables = get_tables(
        sql="WITH data AS (SELECT * FROM orders) SELECT * FROM data RIGHT JOIN products ON data.product_id = products.id"
    )
    assert frozenset(tables) == frozenset(["orders", "products"])


def test_get_dependencies():
    deps = get_dependencies("tests/files/case0")
    assert len(deps) == 2
    assert frozenset(deps.keys()) == frozenset(["table0", "table1"])
    assert deps["table0"] == Dependency(deps=["source_orders"], filename="tests/files/case0/table0.sql")
    assert deps["table1"] == Dependency(deps=["table0"], filename="tests/files/case0/table1.sql")
