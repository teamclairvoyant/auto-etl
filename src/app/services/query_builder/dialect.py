import functools
import logging
import textwrap
from collections import defaultdict
from typing import (Any, Callable, Dict, Iterable, List, Mapping, NamedTuple,
                    Optional, Sequence, Tuple, TypeVar, Union)

from pydantic.dataclasses import dataclass

from app.etl_exceptions import AutoETLException

logger = logging.getLogger(__name__)


_T = TypeVar('_T', bound='QueryValue')


_Q = TypeVar('_Q', bound='BaseQuery')
_QArg = Union[str, Tuple[str, ...]]


class QueryValue(NamedTuple):
    value: str
    alias: str = ''
    on_condn: str = ''
    keyword: str = ''
    is_subquery: bool = False

    @classmethod
    def from_arg(cls, arg: _QArg, **kwargs: Any) -> 'QueryValue':
        """method to set the parameter for the QueryValue

        Args:
            arg (_QArg): _description_

        Raises:
            ValueError

        Returns:
            class object 'QueryValue'
        """
        if isinstance(arg, str):
            alias, value, on_condn = '', arg, ''
        elif len(arg) == 3 and 'JOIN' in kwargs['keyword']:
            alias, value, on_condn = arg
        elif len(arg) == 2:
            alias, value = arg
            on_condn = ''
        else:  # pragma: no cover
            raise ValueError(f"invalid arg: {arg!r}")
        return cls(_clean_up(value), _clean_up(alias), _clean_up(on_condn), **kwargs)


class _FlagList(List[_T]):
    flag: str = ''


def _clean_up(thing: str) -> str:
    return textwrap.dedent(thing.rstrip()).strip()


class BaseQuery:

    keywords = [
        'WITH',
        'SELECT',
        'FROM',
        'JOIN',
        'WHERE',
        'GROUP BY',
        'HAVING',
        'ORDER BY',
        'LIMIT',
    ]

    separators: Mapping[str, str] = dict(WHERE='AND', HAVING='AND')
    default_separator = ','

    formats: Tuple[Mapping[str, str], ...] = (
        defaultdict(lambda: '{value}'),
        defaultdict(lambda: '{value} AS {alias}', WITH='{alias} AS {value}'),
    )

    subquery_keywords = {'WITH'}
    fake_keywords = dict(JOIN='FROM')
    flag_keywords = dict(SELECT={'DISTINCT', 'ALL'})

    def __init__(
        self,
        data: Optional[Mapping[str, Iterable[_QArg]]] = None,
        separators: Optional[Mapping[str, str]] = None,
    ) -> None:
        """
        """
        self.data: Mapping[str, _FlagList[QueryValue]] = {}
        if data is None:
            data = dict.fromkeys(self.keywords, ())
        for keyword, args in data.items():
            self.data[keyword] = _FlagList()
            self.add(keyword, *args)

        if separators is not None:
            self.separators = separators

    def add(self: _Q, keyword: str, *args: _QArg) -> _Q:
        """method to add params to the query object

        Args:
            self (_Q): current object of Basequery
            keyword (str): keyword to be added to the query object

        Raises:
            ValueError

        Returns:
            _Q: Basequery
        """
        keyword, fake_keyword = self._resolve_fakes(keyword)
        keyword, flag = self._resolve_flags(keyword)
        target = self.data[keyword]

        if flag:
            if target.flag:  # pragma: no cover
                raise ValueError(f"{keyword} already has flag: {flag!r}")
            target.flag = flag

        kwargs: Dict[str, Any] = {}
        if fake_keyword:
            kwargs.update(keyword=fake_keyword)
        if keyword in self.subquery_keywords:
            kwargs.update(is_subquery=True)

        for arg in args:
            target.append(QueryValue.from_arg(arg, **kwargs))

        return self

    def _resolve_fakes(self, keyword: str) -> Tuple[str, str]:
        for part, real in self.fake_keywords.items():
            if part in keyword:
                return real, keyword
        return keyword, ''

    def _resolve_flags(self, keyword: str) -> Tuple[str, str]:
        prefix, _, flag = keyword.partition(' ')
        if prefix in self.flag_keywords:
            if flag and flag not in self.flag_keywords[prefix]:
                raise ValueError(f"invalid flag for {prefix}: {flag!r}")
            return prefix, flag
        return keyword, ''

    def __getattr__(self: _Q, name: str) -> Callable[..., _Q]:
        # conveniently, avoids shadowing dunder methods (e.g. __deepcopy__)
        if not name.isupper():
            return getattr(super(), name)  # type: ignore
        return functools.partial(self.add, name.replace('_', ' '))

    def __str__(self) -> str:
        return ''.join(self._lines())

    def _lines(self) -> Iterable[str]:
        for keyword, things in self.data.items():
            if not things:
                continue

            if things.flag:
                yield f'{keyword} {things.flag}\n'
            else:
                yield f'{keyword}\n'

            grouped: Tuple[List[QueryValue], ...] = ([], [])
            for thing in things:
                grouped[bool(thing.keyword)].append(thing)
            for group in grouped:
                yield from self._lines_keyword(keyword, group)

    def _lines_keyword(self, keyword: str, things: Sequence[QueryValue]) -> Iterable[str]:
        for i, thing in enumerate(things):
            last = i + 1 == len(things)

            if thing.keyword:
                yield thing.keyword + '\n'

            _format = self.formats[bool(thing.alias)][keyword]
            value = thing.value

            if thing.is_subquery:
                value = f'(\n{textwrap.indent(text=value, prefix="    ")}\n)'

            yield textwrap.indent(text=_format.format(value=value, alias=thing.alias), prefix='   ')

            if thing.on_condn:
                yield '\n ON '+thing.on_condn

            if not last and not thing.keyword:
                try:
                    yield ' ' + self.separators[keyword]
                except KeyError:
                    yield self.default_separator

            yield '\n'


@dataclass()
class RedshiftDialect:
    target_table_conf: List[Dict]
    joins_and_filters_conf: Dict[str, Dict]
    select_sources: List[Dict]

    def get_sql(self) -> None:
        """Method to trrigger Redshift query builder
        """
        logger.info('building Redshift query from the mappings file')

        _query = BaseQuery()
        _query = self.get_select(_query, self.select_sources)
        _query = self.get_join(_query)
        print(str(_query))

    def get_select(self, _query: BaseQuery, select_sources: List[Dict]) -> BaseQuery:
        """method to generate the select sql

        Args:
            _query (BaseQuery)
            select_sources (List[Dict]): mapping file dictionary

        Returns:
            BaseQuery
        """
        try:
            for _select in select_sources:
                _query = _query.SELECT(
                    (_select['column_alias'], _select['transformation']))

            return _query
        except Exception as excep:
            logger.error("Error while generating SELECT sql")
            raise AutoETLException(
                "Error while generating SELECT sql", excep.args)

    def get_join(self, _query: BaseQuery) -> BaseQuery:
        """method to generate the join sql

        Args:
            _query (BaseQuery)

        Returns:
            BaseQuery
        """
        for _index in self.joins_and_filters_conf:

            _map = self.joins_and_filters_conf[_index]

            if int(_index) == 0:

                _query = _query.FROM(
                    (_map['driving_table_alias'], _map['driving_table']))

            if _map['reference_subquery']:
                ref_table = '('+_map['reference_subquery']+')'
            else:
                ref_table = _map['reference_table']

            match _map['join_type']:
                case "left join":
                    _query = _query.LEFT_JOIN(
                        (_map['reference_table_alias'], ref_table, _map['join_condition']))
                case "right join":
                    _query = _query.RIGHT_JOIN(
                        (_map['reference_table_alias'], ref_table, _map['join_condition']))
                case "inner join":
                    _query = _query.INNER_JOIN(
                        (_map['reference_table_alias'], ref_table, _map['join_condition']))
                case "full outer join":
                    _query = _query.FULL_OUTER_JOIN(
                        (_map['reference_table_alias'], ref_table, _map['join_condition']))
                case "cross join":
                    _query = _query.CROSS_JOIN(
                        (_map['reference_table_alias'], ref_table, _map['join_condition']))

            if _map['filter_condition']:
                _query = _query.WHERE(_map['filter_condition'])

        return _query
