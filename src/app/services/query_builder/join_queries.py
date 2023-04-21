import logging
from typing import Any, Optional, Union

from pydantic.dataclasses import dataclass
from pypika import Criterion, Table
from pypika.enums import JoinType
from pypika.queries import Join, Joiner, QueryBuilder
from pypika.terms import Term

from app.etl_exceptions import AutoETLException

logger = logging.getLogger(__name__)


class Config:
    arbitrary_types_allowed = True


@dataclass(config=Config)
class JoinQuery(Joiner):
    query: QueryBuilder
    item: Table
    how: JoinType
    label: str

    def __post_init__(self):
        super().__init__(self.query, self.item, self.how, self.label)

    def on(self, criterion: Criterion, collate: Optional[str] = None) -> QueryBuilder:
        if criterion is None:
            logger.error(
                "Parameter 'criterion' is required for a %s JOIN but was not supplied.", self.label)
            raise AutoETLException(
                f"Parameter 'criterion' is required for a {self.label} JOIN but was not supplied.")

        self.query.do_join(JoinQueryOn(
            self.item, self.how, criterion, collate))  # type: ignore
        return self.query


@dataclass(config=Config)
class JoinQueryOn(Join):
    def __init__(self, item: Term, how: JoinType, criteria: Union[QueryBuilder, str],
                 collate: Optional[str] = None) -> None:
        super().__init__(item, how)
        self.criterion = criteria
        self.collate = collate

    def get_sql(self, **kwargs: Any) -> str:
        join_sql = super().get_sql(**kwargs)
        criteria = self.criterion if isinstance(
            self.criterion, str) else self.criterion.get_sql(subquery=True, **kwargs)
        return f"{join_sql} ON {criteria} {f' COLLATE {self.collate}' if self.collate else ''}"
