import re
import random
import logging
from datetime import datetime, timedelta, timezone
from typing import Callable

from app.crud import ue as crud_ue, application as crud_app
from app.interfaces.analyticsExposure.QueryServiceInterface import QueryServiceInterface
from app.schemas.analyticsExposureInternal import (
    QueryResult, VectorQueryResult, MatrixQueryResult,
    VectorEntry, MatrixEntry,
)

logger = logging.getLogger(__name__)


def _parse_sub_query(sub_q: str) -> dict:
    type_match = re.search(r'label_replace\(.*?,\s*"type",\s*"([^"]+)"', sub_q)
    metric_type = type_match.group(1) if type_match else 'UNKNOWN'

    src_ip_match = re.search(r'src_ip=~"([^"]*)"', sub_q)
    src_ips = [ip for ip in src_ip_match.group(1).split('|') if ip] if src_ip_match else []

    dst_ip_match = re.search(r'dst_ip=~"([^"]*)"', sub_q)
    dst_ips = [ip for ip in dst_ip_match.group(1).split('|') if ip] if dst_ip_match else []

    return {'type': metric_type, 'src_ips': src_ips, 'dst_ips': dst_ips}


def _stub_value(metric_type: str) -> float:
    """Return a plausible synthetic value depending on the metric kind.

    THR metrics are throughput in bps.
    VOL metrics are volume in bytes.
    """
    if 'THR' in metric_type:
        return random.uniform(1e6, 100e6)   # 1–100 Mbps in bps
    if 'VOL' in metric_type:
        return random.uniform(1e5, 1e8)     # 100 KB–100 MB in bytes
    return random.uniform(1e5, 1e8)


def _build_metric(mtype: str, src_ip: str | None, dst_ip: str | None,
                  ip_to_mac: dict[str, str]) -> dict[str, str]:
    # code 6 - TCP
    metric: dict[str, str] = {'type': mtype, 'proto': '6'}
    if src_ip:
        metric['src_ip'] = src_ip
        if src_ip in ip_to_mac:
            metric['src_mac'] = ip_to_mac[src_ip]
    if dst_ip:
        metric['dst_ip'] = dst_ip
        if dst_ip in ip_to_mac:
            metric['dst_mac'] = ip_to_mac[dst_ip]
    return metric


def _vector_entries_for(seg: dict, ts: float, ip_to_mac: dict[str, str]) -> list[VectorEntry]:
    mtype, src_ips, dst_ips = seg['type'], seg['src_ips'], seg['dst_ips']
    entries = []
    if src_ips and dst_ips:
        for src_ip in src_ips:
            for dst_ip in dst_ips:
                entries.append(VectorEntry(
                    metric=_build_metric(mtype, src_ip, dst_ip, ip_to_mac),
                    value=(ts, str(_stub_value(mtype))),
                ))
    elif src_ips:
        for src_ip in src_ips:
            entries.append(VectorEntry(
                metric=_build_metric(mtype, src_ip, None, ip_to_mac),
                value=(ts, str(_stub_value(mtype))),
            ))
    elif dst_ips:
        for dst_ip in dst_ips:
            entries.append(VectorEntry(
                metric=_build_metric(mtype, None, dst_ip, ip_to_mac),
                value=(ts, str(_stub_value(mtype))),
            ))
    else:
        entries.append(VectorEntry(
            metric=_build_metric(mtype, None, None, ip_to_mac),
            value=(ts, str(_stub_value(mtype))),
        ))
    return entries


def _matrix_entries_for(seg: dict, timestamps: list[float], ip_to_mac: dict[str, str]) -> list[MatrixEntry]:
    mtype, src_ips, dst_ips = seg['type'], seg['src_ips'], seg['dst_ips']

    def make_values() -> list[tuple[float, str]]:
        return [(ts, str(_stub_value(mtype))) for ts in timestamps]

    entries = []
    if src_ips and dst_ips:
        for src_ip in src_ips:
            for dst_ip in dst_ips:
                entries.append(MatrixEntry(
                    metric=_build_metric(mtype, src_ip, dst_ip, ip_to_mac),
                    values=make_values(),
                ))
    elif src_ips:
        for src_ip in src_ips:
            entries.append(MatrixEntry(
                metric=_build_metric(mtype, src_ip, None, ip_to_mac),
                values=make_values(),
            ))
    elif dst_ips:
        for dst_ip in dst_ips:
            entries.append(MatrixEntry(
                metric=_build_metric(mtype, None, dst_ip, ip_to_mac),
                values=make_values(),
            ))
    else:
        entries.append(MatrixEntry(
            metric=_build_metric(mtype, None, None, ip_to_mac),
            values=make_values(),
        ))
    return entries


class StubQueryService(QueryServiceInterface):

    def __init__(self, session_factory: Callable):
        self._session_factory = session_factory

    def _get_ip_to_mac(self) -> dict[str, str]:
        db = self._session_factory()
        try:
            mapping: dict[str, str] = {}
            for ue in crud_ue.get_all(db):
                if ue.ip_address_v4 and ue.mac_address:
                    mapping[ue.ip_address_v4] = ue.mac_address
            for app in crud_app.get_multi(db):
                if app.ip_address_v4 and app.mac_address:
                    mapping[app.ip_address_v4] = app.mac_address
            return mapping
        finally:
            db.close()

    async def query(self,
        query: str, *, time: datetime | None = None, timeout: timedelta | None = None,
        limit: int | None = None, lookback_delta: float | None = None, stats: str | None = None,
        request_timeout: float | None = 30.0
    ) -> QueryResult | None:
        ip_to_mac = self._get_ip_to_mac()
        entries: list[VectorEntry] = []
        now_ts = (time or datetime.now(timezone.utc)).timestamp()
        for sub_q in query.split(' or '):
            entries.extend(_vector_entries_for(_parse_sub_query(sub_q.strip()), now_ts, ip_to_mac))
        logger.debug("Stub query: returning vector entries %s", entries)
        return VectorQueryResult(status='success', result_type='vector', result=entries)

    async def query_range(self,
        query: str, *, start: datetime, end: datetime, step: timedelta,
        timeout: timedelta | None = None,
        limit: int | None = None, lookback_delta: float | None = None, stats: str | None = None,
        request_timeout: float | None = 30.0
    ) -> QueryResult | None:
        ip_to_mac = self._get_ip_to_mac()
        entries: list[MatrixEntry] = []
        timestamps: list[float] = []

        current = start
        while current <= end:
            timestamps.append(current.timestamp())
            current += step
        if not timestamps:
            timestamps = [end.timestamp()]

        for sub_q in query.split(' or '):
            entries.extend(_matrix_entries_for(_parse_sub_query(sub_q.strip()), timestamps, ip_to_mac))

        logger.debug("Stub query_range: returning matrix entries %s over timestamps %s", entries, timestamps)
        return MatrixQueryResult(status='success', result_type='matrix', result=entries)