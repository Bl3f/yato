from abc import ABC, abstractmethod

from yato.yato import Yato


class Transformation(ABC):
    def get_source(self, context):
        return context.con.sql(self.source_sql()).df()

    @staticmethod
    def source_sql():
        pass

    @abstractmethod
    def run(self, context, *args, **kwargs):
        pass
