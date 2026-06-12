import logging
from collections import defaultdict
from datetime import datetime, timezone, timedelta

from typing_extensions import override

from app.core.analyticsExposure.handlers.base import AnalyticsHandler, QueryType
from app.core.analyticsExposure.utilities import population_variance, compute_mean
from app.crud import application as crud_app
from app.models.application import Application
from app.schemas.analyticsExposure import (
    AnalyticsData,
    AnalyticsEvent,
    AnalyticsEventFilter,
    AnalyticsEventFilterSubsc,
    AnalyticsEventNotif,
    EthFlowDescription,
    FlowDirection,
    IpEthFlowDescription,
    MatchingDirection,
    TrafficCharacterization,
    UeCommunication,
    UeCommOrderCriterion,
    UeCommReq,
)
from app.schemas.analyticsExposureInternal import QueryResult, VectorQueryData
from app.schemas.commonData import AnalyticsSubset

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class UeCommHandler(AnalyticsHandler):

    ue_comm_reqs: list[UeCommReq]
    ip_app_map: dict[str, Application]    # ip_address_v4 → Application instance
    ue_ip_set: set[str]                   # str(ip_address_v4) for each target UE
    app_ip_set: set[str]                  # ip_address_v4 for each registered/filtered app
    active_subsets: set[AnalyticsSubset]

    _UE_COMM_ALL_SUBSETS: frozenset[AnalyticsSubset] = frozenset({
        AnalyticsSubset.appListForUeComm,
        AnalyticsSubset.n4SessInactTimerForUeComm,
    })

    @override
    def _setup_for_subscription(self) -> AnalyticsHandler[AnalyticsEventNotif] | None:
        self.query_type = QueryType.QUERY
        self.query_interval = self.query_end_ts - self.query_start_ts or timedelta(seconds=60)
        self._load_from_filter(self.analytics_event_subsc.analyEventFilter)
        return self

    @override
    def _setup_for_fetch(self) -> AnalyticsHandler[AnalyticsData] | None:
        self.query_type = QueryType.QUERY
        self.query_interval = self.query_end_ts - self.query_start_ts or timedelta(seconds=60)
        self._load_from_filter(self.analytics_request.analyEventFilter)
        return self

    def _load_from_filter(
        self, analy_filter: AnalyticsEventFilter | AnalyticsEventFilterSubsc | None
    ) -> None:
        self.ue_comm_reqs, requested_app_ids, self.active_subsets = (
            analy_filter.ueCommReqs or [], analy_filter.appIds or [], set(analy_filter.listOfAnaSubsets or self._UE_COMM_ALL_SUBSETS)
            if analy_filter else ([], [], set(self._UE_COMM_ALL_SUBSETS))
        )

        apps = (
            crud_app.get_apps_by_app_ids(self.db_sql, app_ids=requested_app_ids)
            if requested_app_ids
            else list(crud_app.get_multi(self.db_sql))
        )
        self.ip_app_map = {app.ip_address_v4: app for app in apps}
        self.ue_ip_set = {str(ue.ip_address_v4) for ue in self.target_ues}
        self.app_ip_set = set(self.ip_app_map.keys())

    @override
    def _set_event_queries(self) -> None:
        catalog = self.driver.query_catalog
        self.queries.extend([catalog.UE_UL_VOL_PER_FLOW_BYTES_QUERY, catalog.UE_DL_VOL_PER_FLOW_BYTES_QUERY])

    @override
    def set_built_queries(self) -> None:
        if not self.ip_app_map:
            logger.info("%s: No registered apps, skipping query preparation", self.analytics_id)
            return

        builder = self.driver.QueryBuilderCls()
        for q in self.queries:
            args = self.driver.QueryArgsCls(
                raw_app_ips=self.app_ip_set,
                raw_ue_ips=self.ue_ip_set,
                raw_interval=self.query_interval,
            )
            builder.add(q, args)

        self._built_queries = [(builder, self.query_type)]

    @override
    def results_to_analytics_event_notif(self, results: list[QueryResult | None]) -> AnalyticsEventNotif | None:
        ue_comm_infos = self._build_ue_comm_infos(results)
        if not ue_comm_infos:
            return None
        return AnalyticsEventNotif(
            analyEvent=AnalyticsEvent.ueComm,
            timeStamp=self.query_end_ts,
            ueCommInfos=ue_comm_infos,
        )

    @override
    def results_to_analytics_data(self, results: list[QueryResult | None]) -> AnalyticsData | None:
        ue_comm_infos = self._build_ue_comm_infos(results)
        if not ue_comm_infos:
            return None
        return AnalyticsData(ueCommInfos=ue_comm_infos, suppFeat="")

    def _build_ue_comm_infos(self, results: list[QueryResult | None]) -> list[UeCommunication] | None:

        # Per-app, per-UE volumes { app_ip: { ue_ip: volume } } for UL and DL
        per_app_ue_ul_vol: defaultdict[str, defaultdict[str, float]] = defaultdict(lambda: defaultdict(float))
        per_app_ue_dl_vol: defaultdict[str, defaultdict[str, float]] = defaultdict(lambda: defaultdict(float))
        # Store the original metric entries for building flow descriptions
        per_app_ue_ul_metrics: defaultdict[str, list[dict]] = defaultdict(list)
        per_app_ue_dl_metrics: defaultdict[str, list[dict]] = defaultdict(list)

        catalog = self.driver.query_catalog

        if len(results) != 1:
            logger.warning("%s: expected 1 query result, got %d, skipping", self.analytics_id, len(results))
            return None

        query_result = results[0]
        if query_result is None:
            logger.error("%s: received None query result, skipping", self.analytics_id)
            return None

        data = query_result.data
        if not isinstance(data, VectorQueryData):
            logger.error("%s: ignoring unexpected result entry type: %s", self.analytics_id, type(data))
            return None

        if len(data.result) == 0:
            # If no data is found on Prometheus then there was no traffic for the UEs / Apps in the whole timerange
            return [
                UeCommunication(
                    commDur=int((self.query_end_ts - self.query_start_ts).total_seconds()),         # Not meaningful
                    commDurVariance=0.0,                                                            # Not meaningful
                    ts=self.query_start_ts,                                                         # Not meaningful
                    trafChar=TrafficCharacterization(
                        appId=app.app_id,
                        ulVol=0,
                        ulVolVariance=0,
                        dlVol=0,
                        dlVolVariance=0
                    ),
                    ratio=100
                ) for app_ip, app in self.ip_app_map.items()
            ]

        for r in data.result:

            src_ip = r.metric.get('src_ip')
            dst_ip = r.metric.get('dst_ip')
            metric_type = r.metric.get('type')
            if not src_ip or not dst_ip or not metric_type:
                logger.warning("%s: missing src_ip, dst_ip or metric_type in metric labels: %s",
                               self.analytics_id, r.metric)
                continue

            _, value_str = r.value
            vol = max(0.0, float(value_str))

            if metric_type == catalog.UE_UL_VOL_PER_FLOW_BYTES_QUERY.type and src_ip in self.ue_ip_set and dst_ip in self.app_ip_set:
                if vol > 0:
                    per_app_ue_ul_vol[dst_ip][src_ip] += vol
                    per_app_ue_ul_metrics[dst_ip].append(r.metric)
            elif metric_type == catalog.UE_DL_VOL_PER_FLOW_BYTES_QUERY.type and src_ip in self.app_ip_set and dst_ip in self.ue_ip_set:
                if vol > 0:
                    per_app_ue_dl_vol[src_ip][dst_ip] += vol
                    per_app_ue_dl_metrics[src_ip].append(r.metric)
            else:
                logger.warning("%s: unexpected IPs in metric type %s", self.analytics_id, r.metric)

        ue_comm_infos: list[UeCommunication] = []
        tgt_ue = (self.analytics_event_subsc or self.analytics_request).tgtUe
        is_group = bool(tgt_ue is not None and (tgt_ue.exterGroupId or tgt_ue.anyUeInd))

        for app_ip, app in self.ip_app_map.items():
            # defaultdicts
            ul_vols = list(per_app_ue_ul_vol[app_ip].values())
            dl_vols = list(per_app_ue_dl_vol[app_ip].values())
            ul_metrics = per_app_ue_ul_metrics[app_ip]
            dl_metrics = per_app_ue_dl_metrics[app_ip]

            ul_vol_res, ul_vol_var_res = self._get_vol_stats(ul_vols)
            dl_vol_res, dl_vol_var_res = self._get_vol_stats(dl_vols)

            ul_desc = self._build_flow_desc(ul_metrics, FlowDirection.uplink) if ul_metrics else None
            dl_desc = self._build_flow_desc(dl_metrics, FlowDirection.downlink) if dl_metrics else None

            traffic_char = TrafficCharacterization(
                appId=app.app_id,
                fDescs=[d for d in [ul_desc, dl_desc] if d] or None,
                ulVol=ul_vol_res,
                dlVol=dl_vol_res,
                ulVolVariance=ul_vol_var_res,
                dlVolVariance=dl_vol_var_res,
            )

            ue_comm = UeCommunication(
                commDur=int((self.query_end_ts - self.query_start_ts).total_seconds()),             # Not meaningful
                commDurVariance=0.0,                                                                # Not meaningful
                ts=self.query_start_ts,                                                             # Not meaningful
                trafChar=traffic_char,
            )

            if is_group:
                active_ue_count = len(per_app_ue_ul_vol[app_ip].keys() | per_app_ue_dl_vol[app_ip].keys())
                ratio = round(active_ue_count / len(self.target_ues) * 100)
                if ratio > 1:
                    ue_comm.ratio = ratio               # Limited to the ratio of UEs that do communicate

            ue_comm_infos.append(ue_comm)

        sorted_infos = self._sort(ue_comm_infos)
        logger.info("%s: built %d UE comm entries", self.analytics_id, len(sorted_infos))
        return sorted_infos or None

    @staticmethod
    def _build_flow_desc(metrics: list[dict[str, str]], direction: FlowDirection) -> IpEthFlowDescription | None:
        if not metrics:
            return None

        protos, src_macs, dst_macs, src_ips, dst_ips = set(), set(), set(), set(), set()
        for m in metrics:
            if v := m.get('proto'):   protos.add(v)
            if v := m.get('src_mac'): src_macs.add(v)
            if v := m.get('dst_mac'): dst_macs.add(v)
            if v := m.get('src_ip'):  src_ips.add(v)
            if v := m.get('dst_ip'):  dst_ips.add(v)

        # Use single value when consistent across all entries; fall back to default otherwise
        src_mac = next(iter(src_macs)) if len(src_macs) == 1 else None
        dst_mac = next(iter(dst_macs)) if len(dst_macs) == 1 else None
        proto_str = next(iter(protos)) if len(protos) == 1 else "ip"
        src_ip = next(iter(src_ips)) if len(src_ips) == 1 else "any"
        dst_ip = next(iter(dst_ips)) if len(dst_ips) == 1 else "any"

        # fDesc direction: "in" for UPLINK, "out" for DOWNLINK
        dir_str = "in" if direction == FlowDirection.uplink else "out"
        ip_filter = f"permit {dir_str} {proto_str} from {src_ip} to {dst_ip}"

        eth_filter = EthFlowDescription(
            ethType="0x0800",
            sourceMacAddr=src_mac.replace(':', '-') if src_mac else None,
            destMacAddr=dst_mac.replace(':', '-') if dst_mac else None,
            fDir=direction,
            fDesc=ip_filter,
        )
        return IpEthFlowDescription(ethTrafficFilter=eth_filter)

    def _sort(self, infos: list[UeCommunication]) -> list[UeCommunication]:
        if not self.ue_comm_reqs:
            return infos
        req = self.ue_comm_reqs[0]
        if req.orderCriterion is None:
            return infos
        reverse = req.orderDirection == MatchingDirection.descending
        if req.orderCriterion == UeCommOrderCriterion.startTime:
            return sorted(infos, key=lambda c: c.ts or 0, reverse=reverse)
        if req.orderCriterion == UeCommOrderCriterion.duration:
            return sorted(infos, key=lambda c: c.commDur or 0, reverse=reverse)
        return infos

    @staticmethod
    def _get_vol_stats(volumes: list[float]) -> tuple[int, float | None]:
        return (int(compute_mean(volumes)), population_variance(volumes)) if volumes else (0, 0.0)


