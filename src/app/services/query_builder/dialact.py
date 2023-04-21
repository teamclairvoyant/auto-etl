import logging
from typing import Any, Dict, List

from pydantic.dataclasses import dataclass
from pypika import Query, Schema, Table
from pypika.enums import Dialects, JoinType
from pypika.queries import QueryBuilder
from pypika.utils import builder

from app.services.query_builder.join_queries import JoinQuery

logger = logging.getLogger(__name__)


class RedshiftQuery(Query):
    """
    Query class for AWS Redshift
    """

    @classmethod
    def _builder(cls, **kwargs: Any) -> "RedshiftQueryBuilder":
        return RedshiftQueryBuilder(**kwargs)


class RedshiftQueryBuilder(QueryBuilder):
    QUERY_CLS = RedshiftQuery

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(dialect=Dialects.REDSHIFT, **kwargs)

    @builder
    def join(
        self, item: Table, how: JoinType = JoinType.inner
    ) -> "JoinQuery":
        if isinstance(item, Table):
            return JoinQuery(self, item, how, label="table")

        raise ValueError(f"Cannot join on type {type(item)}")

    def inner_join(self, item: Table) -> "JoinQuery":
        return self.join(item, JoinType.inner)

    def left_join(self, item: Table) -> "JoinQuery":
        return self.join(item, JoinType.left)


@dataclass()
class RedshiftDialact:
    target_table_conf: List[Dict]
    joins_and_filers_conf: Dict[str, Dict]

    def build(self) -> None:
        """Method to trrigger Redshift query builder
        """
        logger.info('building Redshift query from the mappings file')

        _query = RedshiftQuery()
        for _index in self.joins_and_filers_conf:

            _map = self.joins_and_filers_conf[_index]

            if int(_index) == 0:
                schema1, schema2 = Schema(_map['driving_table'].split(
                    '.')[0]), Schema(_map['reference_table'].split('.')[0])

                table1, table2 = Table(_map['driving_table'].split('.')[1],
                                       schema=schema1, alias=_map['driving_table_alias']), \
                    Table(_map['reference_table'].split('.')[1],
                          schema=schema2, alias=_map['reference_table_alias'])

                _query = _query.from_(table1).inner_join(  # type: ignore
                    table2).on(_map['join_condition'])
            else:
                schema2 = Schema(_map['reference_table'].split(
                    '.')[0])

                table2 = Table(_map['reference_table'].split('.')[1],
                               schema=schema2, alias=_map['driving_table_alias'])

                _query = _query.left_join(table2).on(
                    _map['join_condition'])

        print(_query.select('*'))
