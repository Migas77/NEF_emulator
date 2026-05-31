from typing import Self, Iterable

from typing_extensions import override
from app.drivers.analyticsExposure.prom.Query import PromQuery, PromQueryArgs
from app.interfaces.analyticsExposure.QueryBuilderInterface import QueryBuilderInterface


class PromQueryBuilder(QueryBuilderInterface):

    @override
    def build_query(self, query: PromQuery, args: PromQueryArgs) -> str:
        built_query = query.expr.format(
            # Mandatory
            interval=args.interval,
            # Optional
            src_ips=args.src_ips, dst_ips=args.dst_ips,
        )
        if not query.apply_label_replace:
            return built_query
        return PromQueryBuilder.__label_replace(
            expr=built_query, dst_label="type", value=query.type, src_label="", regex=""
        )

    @override
    def build_multi_query(self, queries: Iterable[str]) -> str:
        return ' or '.join(queries)

    @staticmethod
    def __label_replace(expr: str, dst_label: str, value: str, src_label: str, regex: str) -> str:
        return f'label_replace({expr}, "{dst_label}", "{value}", "{src_label}", "{regex}")'

