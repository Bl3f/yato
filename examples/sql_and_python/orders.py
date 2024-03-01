import pandas as pd

from yato import Transformation


class Orders(Transformation):
    @staticmethod
    def source_sql():
        return "SELECT * FROM source_orders"

    def run(self, context, *args, **kwargs) -> pd.DataFrame:
        df = self.get_source(context)

        df["new_column"] = 1

        return df
