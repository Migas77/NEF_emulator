import logging
from collections import defaultdict
from datetime import datetime, timezone

from typing_extensions import override

from app.core.analyticsExposure.handlers.base import AnalyticsHandler, FieldMapping, QueryType
from app.core.analyticsExposure.utilities import bps_to_xbps_bitrate
from app.crud import application as crud_app
from app.interfaces.analyticsExposure.Query import QueryCatalog, Query
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
from app.schemas.analyticsExposureInternal import VectorEntry, MatrixEntry
from app.schemas.commonData import AnalyticsSubset
from app.schemas.monitoringevent import TrafficInformation

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class WlanPerformanceHandler(AnalyticsHandler):

    wlan_reqs: list[WlanPerformanceReq]
    active_subsets: set[AnalyticsSubset]

    # Hardwired values for single WLAN PERFORMANCE INFO
    _SSID: str = '5g-atnog'
    _APP_IP: str = ''           # server IP used for UL/DL direction in ALL_* queries (first registered app IP used)

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
        "ALL_UE_UL_THR_PER_APP_IP_BPS_QUERY":   FieldMapping("uplinkRate",     "src_ip", bps_to_xbps_bitrate),
        "ALL_UE_DL_THR_PER_APP_IP_BPS_QUERY":   FieldMapping("downlinkRate",   "dst_ip", bps_to_xbps_bitrate),
        "ALL_UE_UL_VOL_PER_APP_IP_BYTES_QUERY": FieldMapping("uplinkVolume",   "src_ip", lambda fs: int(float(fs))),
        "ALL_UE_DL_VOL_PER_APP_IP_BYTES_QUERY": FieldMapping("downlinkVolume", "dst_ip", lambda fs: int(float(fs))),
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
        # As appIds is not mentioned in the spec for event WLAN_PERFORMANCE.
        # the first registered app on the db will be used for calculating the WLAN UL/DL metrics
        app = next(iter(crud_app.get_multi(self.db_sql, limit=1)), None)
        if app is None:
            logger.info("%s: no registered app found, skipping", self.analytics_id)
            return False
        self.app_ip = str(app.ip_address_v4)

        self.wlan_reqs, self.active_subsets = (
            event_filter.wlanReqs or [], set(event_filter.listOfAnaSubsets or self._WLAN_ALL_SUBSETS)
            if event_filter else ([], set(self._WLAN_ALL_SUBSETS))
        )

        if event_filter and event_filter.listOfAnaSubsets:
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
    def _get_event_metrics(self, catalog: QueryCatalog) -> list[Query]:
        queries = []
        if AnalyticsSubset.trafficInfo in self.active_subsets:
            queries.extend([
                catalog.UE_UL_THR_PER_SRC_IP_BPS_QUERY, catalog.UE_UL_VOL_PER_SRC_IP_BYTES_QUERY,
                catalog.UE_DL_THR_PER_DST_IP_BPS_QUERY, catalog.UE_DL_VOL_PER_DST_IP_BYTES_QUERY,
                catalog.ALL_UE_UL_THR_PER_APP_IP_BPS_QUERY, catalog.ALL_UE_UL_VOL_PER_APP_IP_BYTES_QUERY,
                catalog.ALL_UE_DL_THR_PER_APP_IP_BPS_QUERY, catalog.ALL_UE_DL_VOL_PER_APP_IP_BYTES_QUERY,
            ])
        return queries

    @override
    def _prepare_queries(self) -> list[tuple[str, QueryType]]:
        metrics = self._get_event_metrics(self.driver.query_catalog)
        if not metrics:
            return []

        target_ue_ips = {ue.ip_address_v4 for ue in self.target_ues}

        def select_args(q: Query):
            if "ALL" in q.type:
                if "UL" in q.type:
                    return self.driver.QueryArgsCls(raw_dst_ips={self.app_ip}, raw_interval=self.query_interval)
                else:
                    return self.driver.QueryArgsCls(raw_src_ips={self.app_ip}, raw_interval=self.query_interval)
            elif "UL" in q.type:
                return self.driver.QueryArgsCls(raw_src_ips=target_ue_ips, raw_interval=self.query_interval)
            else:
                return self.driver.QueryArgsCls(raw_dst_ips=target_ue_ips, raw_interval=self.query_interval)

        return [(
            self.driver.query_builder.build_multi_query(
                self.driver.query_builder.build_query(q, select_args(q))
                for q in metrics
            ),
            self.query_type,
        )]

    def _build_wlan_infos(self, results: list) -> list[WlanPerformInfo] | None:
        if not results:
            logger.info("%s: empty query result", self.analytics_id)
            return None

        ues_by_ip = {str(ue.ip_address_v4): ue for ue in self.target_ues if ue.ip_address_v4 is not None}

        # { ue_ip : { ts: WlanPerTsPerformanceInfo } }
        ue_traffic: defaultdict[str, dict[datetime, WlanPerTsPerformanceInfo]] = defaultdict(dict)
        number_target_ues = len(self.target_ues) if AnalyticsSubset.numberOfUes in self.active_subsets else None

        for r in results:
            if isinstance(r, MatrixEntry):
                pairs = r.values
            elif isinstance(r, VectorEntry):
                pairs = [r.value]
            else:
                logger.error("%s: ignoring unexpected result entry type: %s", self.analytics_id, type(r))
                continue

            metric_type = r.metric.get('type')
            if not metric_type:
                logger.warning("%s: ignoring result entry without 'type' label: %s", self.analytics_id, r)
                continue

            mapping = self._METRIC_FIELD_MAP.get(metric_type)
            if mapping is None or mapping.field not in TrafficInformation.__fields__:
                continue

            ue_ip, number_of_ues = (r.metric[mapping.extractor], None) if "ALL" not in metric_type else ("ALL", number_target_ues)
            for ts, value in pairs:
                ts_dt = datetime.fromtimestamp(ts, tz=timezone.utc)
                ts_perf = ue_traffic[ue_ip].setdefault(ts_dt, WlanPerTsPerformanceInfo(
                    tsStart=ts_dt - self.temporal_gran_size,
                    tsDuration=int(self.temporal_gran_size.total_seconds()),
                    trafficInfo=TrafficInformation(),
                    numberOfUes=number_of_ues
                ))

                setattr(ts_perf.trafficInfo, mapping.field, mapping.converter(value))

        wlan_per_ssid: list[WlanPerSsIdPerformanceInfo] = []
        wlan_per_ue: list[WlanPerUeIdPerformanceInfo] = []

        for ue_ip, ts_map in ue_traffic.items():
            if ue_ip == "ALL":
                wlan_per_ssid.append(WlanPerSsIdPerformanceInfo(
                    ssId=self._SSID,
                    wlanPerTsInfos=list(ts_map.values()),
                ))
            else:
                ts_infos = []
                for info in ts_map.values():
                    if AnalyticsSubset.trafficInfo in self.active_subsets:
                        if (
                            info.trafficInfo is not None and
                            info.trafficInfo.downlinkVolume is not None and info.trafficInfo.uplinkVolume is not None
                        ):
                            info.trafficInfo.totalVolume = (
                                info.trafficInfo.downlinkVolume + info.trafficInfo.uplinkVolume
                            )
                    else:
                        info.trafficInfo = None
                    ts_infos.append(info)
                wlan_per_ue.append(WlanPerUeIdPerformanceInfo(
                    gpsi=Gpsi(__root__=f"msisdn-{ues_by_ip[ue_ip].msisdn}"),
                    wlanPerTsInfos=ts_infos,
                ))

        if not wlan_per_ssid:
            logger.info("%s: empty wlanPerSsidInfos, no data to report", self.analytics_id)
            return None

        wlan_infos = [WlanPerformInfo(
            wlanPerSsidInfos=wlan_per_ssid,
            wlanPerUeIdInfos=wlan_per_ue if wlan_per_ue else None,
        )]
        sorted_infos = self._sort(wlan_infos)
        logger.info("%s: built %d wlan entries", self.analytics_id, len(sorted_infos))
        return sorted_infos

    @override
    def results_to_analytics_event_notif(self, results: list) -> AnalyticsEventNotif | None:
        wlan_infos = self._build_wlan_infos(results)
        if not wlan_infos:
            return None
        return AnalyticsEventNotif(
            analyEvent=AnalyticsEvent.wlanPerformance,
            timeStamp=datetime.now(timezone.utc),
            wlanInfos=wlan_infos,
        )

    @override
    def results_to_analytics_data(self, results: list) -> AnalyticsData | None:
        wlan_infos = self._build_wlan_infos(results)
        if not wlan_infos:
            return None
        return AnalyticsData(wlanInfos=wlan_infos, suppFeat="")

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