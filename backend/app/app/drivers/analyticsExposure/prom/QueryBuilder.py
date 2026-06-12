from typing_extensions import Self, override

from app.drivers.analyticsExposure.prom.Query import PromQuery, PromQueryArgs
from app.interfaces.analyticsExposure.QueryBuilderInterface import QueryBuilderInterface


class PromQueryBuilder(QueryBuilderInterface):

    def __init__(self):
        self._queries: dict[str, tuple[PromQuery, PromQueryArgs]] = {}
        self._parts: list[str] = []

    @override
    def add(self, query: PromQuery, args: PromQueryArgs) -> Self:
        built_expr = query.expr.format(
            interval=args.interval,                                             # Mandatory
            app_ips=args.app_ips,                                               # Optional
            ue_ips=args.ue_ips,                                                 # Optional
        )
        if query.apply_label_replace:
            built_expr = self._label_replace(built_expr, "type", query.type, "", "")

        self._queries[query.type] = (query, args)
        self._parts.append(built_expr)
        return self

    @override
    def build(self) -> str:
        return ' or '.join(self._parts)

    @property
    @override
    def queries(self) -> dict[str, tuple[PromQuery, PromQueryArgs]]:
        return self._queries

    @staticmethod
    def _label_replace(expr: str, dst_label: str, value: str, src_label: str, regex: str) -> str:
        return f'label_replace({expr}, "{dst_label}", "{value}", "{src_label}", "{regex}")'

