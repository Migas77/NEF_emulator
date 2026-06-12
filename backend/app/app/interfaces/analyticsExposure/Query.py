from abc import abstractmethod
from functools import cached_property
from typing import Any

from pydantic import BaseModel, Field, IPvAnyAddress
from datetime import timedelta

from typing_extensions import override


class QueryArgs(BaseModel):
    raw_interval: timedelta
    raw_app_ips: set[IPvAnyAddress] = Field(default_factory=set)
    raw_ue_ips: set[IPvAnyAddress] = Field(default_factory=set)

    @property
    @abstractmethod
    def interval(self) -> Any:
        pass

    @property
    @abstractmethod
    def app_ips(self) -> Any:
        pass

    @property
    @abstractmethod
    def ue_ips(self) -> Any:
        pass

    @override
    def dict(self, *args, **kwargs):
        result = super().dict(**kwargs)
        pairs = [
            ('raw_interval', 'interval'),
            ('raw_app_ips', 'app_ips'),
            ('raw_ue_ips',  'ue_ips'),
        ]
        for old, new in pairs:
            result[new] = result.pop(old)
        return result


class Query(BaseModel):
    expr: str
    type: str

class QueryCatalog:
    # UL: src_ip=ue_ips  →  dst_ip=app_ips
    # DL: src_ip=app_ips →  dst_ip=ue_ips
    
    # WLAN_PERFORMANCE
    UE_UL_THR_PER_SRC_IP_BPS_QUERY: Query = Query(expr='', type='UE_UL_THR_PER_SRC_IP_BPS_QUERY')
    UE_DL_THR_PER_DST_IP_BPS_QUERY: Query = Query(expr='', type='UE_DL_THR_PER_DST_IP_BPS_QUERY')
    UE_UL_VOL_PER_SRC_IP_BYTES_QUERY: Query = Query(expr='', type='UE_UL_VOL_PER_SRC_IP_BYTES_QUERY')
    UE_DL_VOL_PER_DST_IP_BYTES_QUERY: Query = Query(expr='', type='UE_DL_VOL_PER_DST_IP_BYTES_QUERY')
    ALL_UE_UL_THR_PER_APP_IP_BPS_QUERY: Query = Query(expr='', type='ALL_UE_UL_THR_PER_APP_IP_BPS_QUERY')
    ALL_UE_DL_THR_PER_APP_IP_BPS_QUERY: Query = Query(expr='', type='ALL_UE_DL_THR_PER_APP_IP_BPS_QUERY')
    ALL_UE_UL_VOL_PER_APP_IP_BYTES_QUERY: Query = Query(expr='', type='ALL_UE_UL_VOL_PER_APP_IP_BYTES_QUERY')
    ALL_UE_DL_VOL_PER_APP_IP_BYTES_QUERY: Query = Query(expr='', type='ALL_UE_DL_VOL_PER_APP_IP_BYTES_QUERY')
    
    # UE_COMMUNICATION
    UE_UL_VOL_PER_FLOW_BYTES_QUERY: Query = Query(expr='', type='UE_UL_VOL_PER_FLOW_BYTES_QUERY')
    UE_DL_VOL_PER_FLOW_BYTES_QUERY: Query = Query(expr='', type='UE_DL_VOL_PER_FLOW_BYTES_QUERY')

