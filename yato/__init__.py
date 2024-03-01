from abc import ABC, abstractmethod

import pandas as pd

from yato.yato import Yato


class Transformation(ABC):
    def get_source(self, context) -> pd.DataFrame:
        return context.con.sql(self.source_sql()).df()

    @staticmethod
    def source_sql():
        pass

    @abstractmethod
    def run(self, context, *args, **kwargs):
        pass
