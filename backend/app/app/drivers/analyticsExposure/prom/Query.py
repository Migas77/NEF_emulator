from string import Formatter
from pydantic import validator
from typing_extensions import override
from app.drivers.analyticsExposure.prom.QueryService import timedelta_to_prom_duration
from app.interfaces.analyticsExposure.Query import QueryArgs, Query, QueryCatalog


class PromQueryArgs(QueryArgs):

    @staticmethod
    def _ips_regex(ips) -> str:
        return '' if not ips else '|'.join(str(ip) for ip in ips)

    @override
    @property
    def interval(self) -> str:
        return timedelta_to_prom_duration(self.raw_interval)

    @override
    @property
    def src_ips(self) -> str:
        return self._ips_regex(self.raw_src_ips)

    @override
    @property
    def dst_ips(self) -> str:
        return self._ips_regex(self.raw_dst_ips)



class PromQuery(Query):
    # label_replace + OR operator can be used to get multiple queries in one Prometheus API call
    # It can't be used for queries that result in a range vector (set apply_label_replace=False)
    apply_label_replace: bool = True

    @validator("expr")
    def validate_expr_format_placeholders(cls, v):
        valid_fields = set(PromQueryArgs.__dict__.keys())
        used_fields = {
            field_name
            for _, field_name, _, _ in Formatter().parse(v)
            if field_name is not None
        }
        invalid_fields = used_fields - valid_fields
        if invalid_fields:
            raise ValueError(f"Invalid fields in expr: {', '.join(invalid_fields)}. Valid fields are: {', '.join(valid_fields)}")
        return v


class PromQueryCatalog(QueryCatalog):
    # UE_COMMUNICATION
    UE_UL_VOL_PER_FLOW_BYTES_QUERY = PromQuery(
        expr='sum by (src_ip, dst_ip, src_mac, dst_mac, proto) (increase(tap_5g_atnog_throughput_bytes_total{{'
                'src_ip=~"{src_ips}", dst_ip=~"{dst_ips}"'
             '}}[{interval}]))',
        type='UE_UL_VOL_PER_FLOW_BYTES_QUERY'
    )
    UE_DL_VOL_PER_FLOW_BYTES_QUERY = PromQuery(
        expr='sum by (src_ip, dst_ip, src_mac, dst_mac, proto) (increase(tap_5g_atnog_throughput_bytes_total{{'
                'src_ip=~"{src_ips}", dst_ip=~"{dst_ips}"'
             '}}[{interval}]))',
        type='UE_DL_VOL_PER_FLOW_BYTES_QUERY'
    )
    # WLAN_PERFORMANCE
    UE_UL_THR_PER_SRC_IP_BPS_QUERY = PromQuery(
        expr='8 * sum by (src_ip) (rate(tap_5g_atnog_throughput_bytes_total{{'
                'src_ip=~"{src_ips}"'
             '}}[{interval}]))',
        type='UE_UL_THR_PER_SRC_IP_BPS_QUERY'
    )
    UE_DL_THR_PER_DST_IP_BPS_QUERY = PromQuery(
        expr='8 * sum by (dst_ip) (rate(tap_5g_atnog_throughput_bytes_total{{'
                'dst_ip=~"{dst_ips}"'
             '}}[{interval}]))',
        type='UE_DL_THR_PER_DST_IP_BPS_QUERY'
    )
    UE_UL_VOL_PER_SRC_IP_BYTES_QUERY = PromQuery(
        expr='sum by (src_ip) (increase(tap_5g_atnog_throughput_bytes_total{{'
                'src_ip=~"{src_ips}", dst_ip=~"{dst_ips}"'
             '}}[{interval}]))',
        type='UE_UL_VOL_PER_SRC_IP_BYTES_QUERY'
    )
    UE_DL_VOL_PER_DST_IP_BYTES_QUERY = PromQuery(
        expr='sum by (dst_ip) (increase(tap_5g_atnog_throughput_bytes_total{{'
                'src_ip=~"{src_ips}", dst_ip=~"{dst_ips}"'
             '}}[{interval}]))',
        type='UE_DL_VOL_PER_DST_IP_BYTES_QUERY'
    )
    ALL_UE_UL_THR_PER_APP_IP_BPS_QUERY = PromQuery(
        expr='8 * sum(rate(tap_5g_atnog_throughput_bytes_total{{'
                'dst_ip=~"{dst_ips}"'
             '}}[{interval}]))',
        type='ALL_UE_UL_THR_PER_APP_IP_BPS_QUERY'
    )
    ALL_UE_DL_THR_PER_APP_IP_BPS_QUERY = PromQuery(
        expr='8 * sum(rate(tap_5g_atnog_throughput_bytes_total{{'
                'src_ip=~"{src_ips}"'
             '}}[{interval}]))',
        type='ALL_UE_DL_THR_PER_APP_IP_BPS_QUERY'
    )
    ALL_UE_UL_VOL_PER_APP_IP_BYTES_QUERY = PromQuery(
        expr='sum(increase(tap_5g_atnog_throughput_bytes_total{{'
                'dst_ip=~"{dst_ips}"'
             '}}[{interval}]))',
        type='ALL_UE_UL_VOL_PER_APP_IP_BYTES_QUERY'
    )
    ALL_UE_DL_VOL_PER_APP_IP_BYTES_QUERY = PromQuery(
        expr='sum(increase(tap_5g_atnog_throughput_bytes_total{{'
                'src_ip=~"{src_ips}"'
             '}}[{interval}]))',
        type='ALL_UE_DL_VOL_PER_APP_IP_BYTES_QUERY'
    )