<h1 align="center">
    <strong>yato — yet another transformation orchestrator</strong>
</h1>
<p align="center">
yato is the smallest orchestrator on Earth to orchestrate SQL data transformations on top of DuckDB. You just give a folder with SQL queries and it guesses the DAG and runs the queries in the right order.
</p>

## Installation

yato works with Python 3.8+.

```bash
pip install yato-lib
```

## Get Started

Create a folder named `sql` and put your SQL files in it, you can for instance uses the 2 queries given in the example folder.

```python
from yato import Yato

yato = Yato(
    # The path of the file in which yato will run the SQL queries.
    # If you want to run it in memory, just set it to :memory:
    database_path="tmp.duckdb",
    # This is the folder where the SQL files are located.
    # The names of the files will determine the name of the table created.
    sql_folder="sql/",
    # The name of the DuckDB schema where the tables will be created.
    schema="transform",
)

# Runs yato against the DuckDB database with the queries in order.
yato.run()
```

You can also run yato with the cli:

```bash
yato run --db tmp.duckdb sql/
```

## Works with dlt

yato is designed to work in pair with dlt. dlt handles the data loading and yato the data transformation.

```python
import dlt
from yato import Yato

yato = Yato(
    database_path="db.duckdb",
    sql_folder="sql/",
    schema="transform",
)

# You restore the database from S3 before runnning dlt
yato.restore()

pipeline = dlt.pipeline(
    pipeline_name="get_my_data",
    destination="duckdb",
    dataset_name="production",
    credentials="db.duckdb",
)

data = my_source()

load_info = pipeline.run(data)

# You backup the database after a successful dlt run
yato.backup()
yato.run()
```

## Advanced usage

### Mixing SQL and Python transformation
Even if we would love to do everything is SQL it happens sometimes that writing a transformation in Python with pandas (or other libraries) might be faster.

This is why you can mix SQL and Python transformation in yato.

In order to do it you can add a Python file in the transformation folder. In this Python file you have to implement a `Transformation` class with a `run` method. If you depend on other SQL transformation you have to define the source SQL query in a static method called `source_sql`.

Below an example of a transformation (like `orders.py`). The framework will understand that orders needs to run after source_orders.
```python
from yato import Transformation


class Orders(Transformation):
    @staticmethod
    def source_sql():
        return "SELECT * FROM source_orders"

    def run(self, context, *args, **kwargs):
        df = self.get_source(context)

        df["new_column"] = 1

        return df
```



## How does it work?

yato runs relies on the amazing SQLGlot library to syntactically parse the SQL queries and build a DAG of the dependencies. Then, it runs the queries in the right order.