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

### Environment variables
yato supports env variables in the SQL queries (like in the example below). Be careful by default it raises an issue if the env variable is not defined.

```sql
SELECT {{ VALUE }}, {{ OTHER_VALUE }}
```

### Other features
* **Subfolders** — in the main folder, just create the folders you want to organise your transformations, folders have no impact on the DAG inference. Be careful not to have 2 transformations with the same name.
* **Multiple SQL statements** — in the same file, yato will run them in the order they appear. Warning: you can only have one SELECT statement. Other statements can be SET, etc. Still the dependencies (hence the DAG) are computed on the SELECT only for the moment.


## How does it work?

yato runs relies on the amazing SQLGlot library to syntactically parse the SQL queries and build a DAG of the dependencies. Then, it runs the queries in the right order.

## FAQ

**Why choose yato over dbt Core, SQLMesh or lea?**

There is no good answer to this question but yato has not be designed to fully replace SQL transformation orchestrators. yato is meant to be fast to setup and configure with a few features. You give a folder with a bunch of SQL (or Python) inside and it runs. 

You can imagine yato like black for transformations orchestration. Only one parameter and here you go.

**Why only DuckDB**

For the moment yato only supports DuckDB as backend/dialect. The main reason is that DuckDB offers features that would be hard to implement with a client/server database. I do not exclude to add Postgres or cloud warehouses, but it would require to think how to do it, especially when mixing SQL and Python transformations.

**Can yato support Jinja templating?**

I does not. I'm not sure it should. I think that when you're adding Jinja templating to your SQL queries you're already too far. I would recommend not to use yato for this. Still if you really want to use yato and have Jinja support reach me. 

Small note, yato support env variables in the SQL queries.

**Can I contribute?**

Yes obviously, right now the project is in its early stage and I would be happy to have feedbacks and contributions. Keep in mind this is a small orchestrator and covering the full gap with other ochestrators makes no sense because just use them they are awesome.



## Limitations
* You can't have 2 transformations with the same name.
* There are no tests for the moment. I'm working on it.