from abc import abstractmethod
from typing import Any

from pydantic import BaseModel, Field, IPvAnyAddress
from datetime import timedelta

from typing_extensions import override


class QueryArgs(BaseModel):
    raw_interval: timedelta
    raw_src_ips: set[IPvAnyAddress] = Field(default_factory=set)
    raw_dst_ips: set[IPvAnyAddress] = Field(default_factory=set)

    @property
    @abstractmethod
    def interval(self) -> Any:
        pass

    @property
    @abstractmethod
    def src_ips(self) -> Any:
        pass

    @property
    @abstractmethod
    def dst_ips(self) -> Any:
        pass

    @override
    def dict(self, *args, **kwargs):
        result = super().dict(**kwargs)
        pairs = [
            ('raw_interval', 'interval'),
            ('raw_src_ips', 'src_ips'),
            ('raw_dst_ips', 'dst_ips'),
        ]
        for old, new in pairs:
            result[new] = result.pop(old)
        return result


class Query(BaseModel):
    expr: str
    type: str

class QueryCatalog:
    # UL: src_ip=UEs  →  dst_ip=app
    # DL: src_ip=app  →  dst_ip=UEs 
    
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

