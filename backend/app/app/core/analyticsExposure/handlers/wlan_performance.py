import logging
from collections import defaultdict
from datetime import datetime, timezone
from functools import cached_property

from typing_extensions import override

from app.core.analyticsExposure.handlers.base import AnalyticsHandler, FieldMapping, QueryType
from app.core.analyticsExposure.utilities import bps_to_xbps_bitrate
from app.interfaces.analyticsExposure.Query import Query
from app.schemas.analyticsExposure import (
    AnalyticsData,
    AnalyticsEvent,
    AnalyticsEventFilter,
    AnalyticsEventFilterSubsc,
    AnalyticsEventNotif,
    Gpsi,
    MatchingDirection,
    WlanOrderingCriterion,
    WlanPerformInfo,
    WlanPerformanceReq,
    WlanPerUeIdPerformanceInfo,
    WlanPerSsIdPerformanceInfo,
    WlanPerTsPerformanceInfo,
)
from app.schemas.analyticsExposureInternal import VectorResult, MatrixResult, QueryResult, MatrixQueryData, \
    VectorQueryData
from app.schemas.commonData import AnalyticsSubset
from app.schemas.monitoringevent import TrafficInformation

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class WlanPerformanceHandler(AnalyticsHandler):

    wlan_reqs: list[WlanPerformanceReq]
    active_subsets: set[AnalyticsSubset]
    number_of_ues: int | None               # Only set if AnalyticsSubset.numberOfUes is in active_subsets
    app_ip: str

    # Hardwired ssid
    _SSID: str = '5g-atnog'

    _WLAN_ALL_SUBSETS: frozenset[AnalyticsSubset] = frozenset({
        AnalyticsSubset.rssi,
        AnalyticsSubset.rtt,
        AnalyticsSubset.trafficInfo,
        AnalyticsSubset.numberOfUes,
    })

    _WLAN_UNSUPPORTED_SUBSETS: frozenset[AnalyticsSubset] = frozenset({
        AnalyticsSubset.rssi,
        AnalyticsSubset.rtt,
        AnalyticsSubset.numberOfUes,
    })

    _METRIC_FIELD_MAP: dict[str, FieldMapping] = {
        "UE_UL_THR_PER_SRC_IP_BPS_QUERY":       FieldMapping("uplinkRate",     "src_ip", bps_to_xbps_bitrate),
        "UE_DL_THR_PER_DST_IP_BPS_QUERY":       FieldMapping("downlinkRate",   "dst_ip", bps_to_xbps_bitrate),
        "UE_UL_VOL_PER_SRC_IP_BYTES_QUERY":     FieldMapping("uplinkVolume",   "src_ip", lambda fs: int(float(fs))),
        "UE_DL_VOL_PER_DST_IP_BYTES_QUERY":     FieldMapping("downlinkVolume", "dst_ip", lambda fs: int(float(fs))),
        "ALL_UE_UL_THR_PER_APP_IP_BPS_QUERY":   FieldMapping("uplinkRate",           "", bps_to_xbps_bitrate),
        "ALL_UE_DL_THR_PER_APP_IP_BPS_QUERY":   FieldMapping("downlinkRate",         "", bps_to_xbps_bitrate),
        "ALL_UE_UL_VOL_PER_APP_IP_BYTES_QUERY": FieldMapping("uplinkVolume",         "", lambda fs: int(float(fs))),
        "ALL_UE_DL_VOL_PER_APP_IP_BYTES_QUERY": FieldMapping("downlinkVolume",       "", lambda fs: int(float(fs))),
    }

    @override
    def _setup_for_subscription(self) -> "AnalyticsHandler[AnalyticsEventNotif] | None":
        event_filter = self.analytics_event_subsc.analyEventFilter if self.analytics_event_subsc else None
        return self if self._load_from_filter(event_filter) else None

    @override
    def _setup_for_fetch(self) -> "AnalyticsHandler[AnalyticsData] | None":
        event_filter = self.analytics_request.analyEventFilter if self.analytics_request else None
        return self if self._load_from_filter(event_filter) else None

    def _load_from_filter(
        self, event_filter: AnalyticsEventFilter | AnalyticsEventFilterSubsc | None
    ) -> bool:
        if not event_filter or not event_filter.appServerAddrs:
            logger.info("%s: appServerAddrs not provided, skipping", self.analytics_id)
            return False

        # Use as destination the first appServerAddrs ipAddr (ipv4)
        self.app_ip = str(event_filter.appServerAddrs[0].ipAddr.ipv4Addr)
        self.wlan_reqs = event_filter.wlanReqs or []
        self.active_subsets = set(event_filter.listOfAnaSubsets or self._WLAN_ALL_SUBSETS)
        self.number_of_ues = len(self.target_ues) if AnalyticsSubset.numberOfUes in self.active_subsets else None
        self.ues_by_ip = {str(ue.ip_address_v4): ue for ue in self.target_ues if ue.ip_address_v4 is not None}

        if event_filter.listOfAnaSubsets:
            unsupported = self.active_subsets & self._WLAN_UNSUPPORTED_SUBSETS
            if unsupported:
                logger.warning(
                    "%s: the following wlan subsets are not supported in this emulator and will be ignored: %s",
                    self.analytics_id,
                    ", ".join(s.value for s in unsupported),
                )

        allowed_ssids = {ssid for req in self.wlan_reqs if req.ssIds for ssid in req.ssIds}
        if allowed_ssids and self._SSID not in allowed_ssids:
            logger.info("%s: only supported SSID '%s' not in allowed SSIDs, skipping", self.analytics_id, self._SSID)
            return False

        if any(req.bssIds for req in self.wlan_reqs):
            logger.warning("%s: bssIds filtering is not supported in this emulator", self.analytics_id)

        return True

    @override
    def _set_event_queries(self) -> None:
        catalog = self.driver.query_catalog
        if AnalyticsSubset.trafficInfo in self.active_subsets:
            # because all queries are based on same underlying metric, all returned ts will align
            self.queries.extend([
                catalog.UE_UL_THR_PER_SRC_IP_BPS_QUERY, catalog.UE_UL_VOL_PER_SRC_IP_BYTES_QUERY,
                catalog.UE_DL_THR_PER_DST_IP_BPS_QUERY, catalog.UE_DL_VOL_PER_DST_IP_BYTES_QUERY,
                catalog.ALL_UE_UL_THR_PER_APP_IP_BPS_QUERY, catalog.ALL_UE_UL_VOL_PER_APP_IP_BYTES_QUERY,
                catalog.ALL_UE_DL_THR_PER_APP_IP_BPS_QUERY, catalog.ALL_UE_DL_VOL_PER_APP_IP_BYTES_QUERY,
            ])

    @override
    def set_built_queries(self):
        target_ue_ips = set(self.ues_by_ip.keys())
        app_ips = {self.app_ip}

        def select_args(q: Query):
            if "ALL" in q.type:
                return self.driver.QueryArgsCls(raw_interval=self.query_interval, raw_app_ips=app_ips)
            else:
                return self.driver.QueryArgsCls(raw_interval=self.query_interval, raw_app_ips=app_ips, raw_ue_ips=target_ue_ips)

        builder = self.driver.QueryBuilderCls()
        for q in self.queries:
            builder.add(q, select_args(q))

        self._built_queries = [(builder, self.query_type)]

    def _build_wlan_infos(self, results: list[QueryResult | None]) -> list[WlanPerformInfo] | None:
        if results is None:
            logger.info("%s: empty query result", self.analytics_id)
            return None

        # { ue_ip/"ALL" : { ts: WlanPerTsPerformanceInfo } }
        ue_traffic: dict[str, dict[datetime, WlanPerTsPerformanceInfo]] = defaultdict(dict)

        if len(results) != 1:
            logger.warning("%s: expected 1 query result, got %d, skipping", self.analytics_id, len(results))
            return None

        query_result = results[0]
        if query_result is None:
            logger.error("%s: received None query result, skipping", self.analytics_id)
            return None

        data = query_result.data
        if not isinstance(data, (MatrixQueryData, VectorQueryData)):
            logger.error("%s: ignoring unexpected result entry type: %s", self.analytics_id, type(data))
            return None

        if len(data.result) == 0:
            # If no data is found on Prometheus then there was no traffic for the UEs in the whole timerange
            empty_entry_ssid = self._default_ssid_wlan_ts_perf_info
            empty_entry_ue = self._default_ue_wlan_ts_perf_info
            return [
                WlanPerformInfo(
                    wlanPerSsidInfos=[
                        WlanPerSsIdPerformanceInfo(
                            ssId=self._SSID,
                            wlanPerTsInfos=[empty_entry_ssid],
                        )
                    ],
                    wlanPerUeIdInfos=[
                        WlanPerUeIdPerformanceInfo(
                            gpsi=Gpsi(__root__=f"msisdn-{ue.msisdn}"),
                            wlanPerTsInfos=[empty_entry_ue],
                        ) for ue in self.target_ues
                    ],
                )
            ]

        for r in data.result:
            metric_type = r.metric.get('type')
            if not metric_type:
                logger.warning("%s: ignoring result entry without 'type' label: %s", self.analytics_id, r)
                continue

            mapping = self._METRIC_FIELD_MAP.get(metric_type)
            if mapping is None or mapping.field not in TrafficInformation.__fields__:
                continue

            ue_ip, number_of_ues = ("ALL", self.number_of_ues) if "ALL" in metric_type else \
                (r.metric[mapping.extractor], None)

            # Prometheus will also evaluate [self.query_start_ts - interval, self.query_start_ts]
            ts_curr = self.query_start_ts - self.query_interval
            for ts, value in r.values:
                # Prometheus returns r.values ordered by ts (unix)
                # As all queries are based on the same underlying metric, and time parameters, all returned ts will align
                ts_dt = datetime.fromtimestamp(ts, tz=timezone.utc)
                if ts_dt not in ue_traffic[ue_ip]:
                    duration = ts_dt - ts_curr
                    effective_duration = duration if duration > self.query_interval else self.query_interval
                    ue_traffic[ue_ip][ts_dt] = WlanPerTsPerformanceInfo(
                        tsStart=ts_curr,
                        tsDuration=int(effective_duration.total_seconds()),
                        trafficInfo=TrafficInformation(),
                        numberOfUes=number_of_ues
                    )
                    ts_curr = ts_dt

                ts_perf = ue_traffic[ue_ip][ts_dt]
                setattr(ts_perf.trafficInfo, mapping.field, mapping.converter(value))
                if mapping.field in {"uplinkVolume", "downlinkVolume"}:
                    ts_perf.trafficInfo.totalVolume = (
                        (ts_perf.trafficInfo.uplinkVolume or 0) + (ts_perf.trafficInfo.downlinkVolume or 0)
                    )

        all_ts_map = ue_traffic["ALL"]
        wlan_per_ssid: list[WlanPerSsIdPerformanceInfo] = [WlanPerSsIdPerformanceInfo(
            ssId=self._SSID,
            wlanPerTsInfos=list(all_ts_map.values()) if all_ts_map else [self._default_ssid_wlan_ts_perf_info],
        )]

        wlan_per_ue: list[WlanPerUeIdPerformanceInfo] = []
        for ue_ip, ue in self.ues_by_ip.items():
            # For a given ue ip, it will return default value of 0 if the entry is missing
            ts_map = ue_traffic[ue_ip]
            wlan_infos = list(ts_map.values()) if ts_map else [self._default_ue_wlan_ts_perf_info]
            wlan_per_ue.append(WlanPerUeIdPerformanceInfo(
                gpsi=Gpsi(__root__=f"msisdn-{ue.msisdn}"),
                wlanPerTsInfos=wlan_infos
            ))

        wlan_infos = [WlanPerformInfo(
            wlanPerSsidInfos=wlan_per_ssid,
            wlanPerUeIdInfos=wlan_per_ue,
        )]
        sorted_infos = self._sort(wlan_infos)
        logger.info("%s: built %d wlan entries", self.analytics_id, len(sorted_infos))
        return sorted_infos

    @override
    def results_to_analytics_event_notif(self, results: list[QueryResult | None]) -> AnalyticsEventNotif | None:
        wlan_infos = self._build_wlan_infos(results)
        if not wlan_infos:
            return None
        return AnalyticsEventNotif(
            analyEvent=AnalyticsEvent.wlanPerformance,
            timeStamp=self.query_end_ts,
            wlanInfos=wlan_infos,
        )

    @override
    def results_to_analytics_data(self, results: list[QueryResult | None]) -> AnalyticsData | None:
        wlan_infos = self._build_wlan_infos(results)
        if not wlan_infos:
            return None
        return AnalyticsData(wlanInfos=wlan_infos, suppFeat="")

    @cached_property
    def _default_ue_wlan_ts_perf_info(self) -> WlanPerTsPerformanceInfo:
        return WlanPerTsPerformanceInfo(
            tsStart=self.query_start_ts,
            tsDuration=int((self.query_end_ts - self.query_start_ts).total_seconds()),
            trafficInfo=TrafficInformation(
                uplinkRate='0 bps',
                downlinkRate='0 bps',
                uplinkVolume=0,
                downlinkVolume=0,
                totalVolume=0
            )
        )

    @cached_property
    def _default_ssid_wlan_ts_perf_info(self) -> WlanPerTsPerformanceInfo:
        return self._default_ue_wlan_ts_perf_info.copy(
            update={"numberOfUes": self.number_of_ues}
        )

    def _sort(self, wlan_infos: list[WlanPerformInfo]) -> list[WlanPerformInfo]:
        req = self.wlan_reqs[0] if self.wlan_reqs else None
        if req is None or req.wlanOrderCriter is None:
            return wlan_infos

        criter = req.wlanOrderCriter
        reverse = req.order == MatchingDirection.descending

        def sort_key(wlan_info: WlanPerformInfo) -> int | datetime:
            ts_infos = [ts for ssid_info in (wlan_info.wlanPerSsidInfos or []) for ts in ssid_info.wlanPerTsInfos]
            if not ts_infos:
                return 0
            if criter == WlanOrderingCriterion.timeSlotStart:
                return min(ts.tsStart for ts in ts_infos)
            if criter == WlanOrderingCriterion.numberOfUes:
                return sum(ts.numberOfUes or 0 for ts in ts_infos)
            if criter == WlanOrderingCriterion.rssi:
                return sum(ts.rssi or 0 for ts in ts_infos)
            if criter == WlanOrderingCriterion.rtt:
                return sum(ts.rtt or 0 for ts in ts_infos)
            if criter == WlanOrderingCriterion.trafficInfo:
                return sum(
                    (ts.trafficInfo.uplinkVolume or 0) + (ts.trafficInfo.downlinkVolume or 0)
                    for ts in ts_infos if ts.trafficInfo is not None
                )
            return 0

        return sorted(wlan_infos, key=sort_key, reverse=reverse)