import logging
from collections import defaultdict
from datetime import datetime, timezone, timedelta

from typing_extensions import override

from app.core.analyticsExposure.handlers.base import AnalyticsHandler, QueryType
from app.core.analyticsExposure.utilities import population_variance, subscription_id_from_link
from app.crud import application as crud_app
from app.models.application import Application
from app.drivers.analyticsExposure import AnalyticsExposureDriver
from app.interfaces.analyticsExposure.Query import QueryCatalog, Query
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
from app.schemas.analyticsExposureInternal import VectorEntry
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
    def _get_event_metrics(self, catalog: QueryCatalog) -> list[Query]:
        return [catalog.UE_UL_VOL_PER_FLOW_BYTES_QUERY, catalog.UE_DL_VOL_PER_FLOW_BYTES_QUERY]

    @override
    def _prepare_queries(self) -> list[tuple[str, QueryType]]:
        if not self.ip_app_map:
            logger.info("%s: No registered apps, skipping query preparation", self.analytics_id)
            return []

        catalog = self.driver.query_catalog

        queries = []
        for q, src_ips, dst_ips in [
            (catalog.UE_UL_VOL_PER_FLOW_BYTES_QUERY, self.ue_ip_set, self.app_ip_set),  # UE → app (UL)
            (catalog.UE_DL_VOL_PER_FLOW_BYTES_QUERY, self.app_ip_set, self.ue_ip_set),  # app → UE (DL)
        ]:
            args = self.driver.QueryArgsCls(
                raw_src_ips=src_ips, raw_dst_ips=dst_ips,
                raw_interval=self.query_interval,
            )
            queries.append((self.driver.query_builder.build_query(q, args), self.query_type))

        return queries

    @override
    def results_to_analytics_event_notif(self, results: list) -> AnalyticsEventNotif | None:
        ue_comm_infos = self._build_ue_comm_infos(results)
        if not ue_comm_infos:
            return None
        return AnalyticsEventNotif(
            analyEvent=AnalyticsEvent.ueComm,
            timeStamp=datetime.now(timezone.utc),
            ueCommInfos=ue_comm_infos,
        )

    @override
    def results_to_analytics_data(self, results: list) -> AnalyticsData | None:
        ue_comm_infos = self._build_ue_comm_infos(results)
        if not ue_comm_infos:
            return None
        return AnalyticsData(ueCommInfos=ue_comm_infos, suppFeat="")

    def _build_ue_comm_infos(self, results: list) -> list[UeCommunication] | None:
        if not results:
            logger.info("%s: empty query result", self.analytics_id)
            return None

        # Per-app, per-UE volumes { app_ip: { ue_ip: volume } } for UL and DL
        per_app_ue_ul_vol: defaultdict[str, defaultdict[str, float]] = defaultdict(lambda: defaultdict(float))
        per_app_ue_dl_vol: defaultdict[str, defaultdict[str, float]] = defaultdict(lambda: defaultdict(float))
        # Store the original metric entries for building flow descriptions
        per_app_ue_ul_metrics: defaultdict[str, list[dict]] = defaultdict(list)
        per_app_ue_dl_metrics: defaultdict[str, list[dict]] = defaultdict(list)
        # Populate to conform with the spec (not meaningful in the implementation)
        comm_dur = int((self.query_end_ts - self.query_start_ts).total_seconds())
        ts_start = self.query_start_ts

        catalog = self.driver.query_catalog

        for r in results:
            # logger.info("r %s", r)
            if not isinstance(r, VectorEntry):
                logger.error("%s: ignoring unexpected result entry type: %s", self.analytics_id, type(r))
                continue

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

        if not per_app_ue_ul_vol and not per_app_ue_dl_vol:
            logger.info("%s: no UE communication data to report", self.analytics_id)
            return None

        ue_comm_infos: list[UeCommunication] = []
        active_app_ips = set(per_app_ue_ul_vol.keys()) | set(per_app_ue_dl_vol.keys())
        tgt_ue = (self.analytics_event_subsc or self.analytics_request).tgtUe
        is_group = bool(tgt_ue is not None and (tgt_ue.exterGroupId or tgt_ue.anyUeInd))
        for app_ip in active_app_ips:
            ul_vols = per_app_ue_ul_vol[app_ip].values()
            dl_vols = per_app_ue_dl_vol[app_ip].values()

            ul_metrics = per_app_ue_ul_metrics.get(app_ip, [])
            dl_metrics = per_app_ue_dl_metrics.get(app_ip, [])

            ul_desc = self._build_flow_desc(ul_metrics, FlowDirection.uplink) if ul_metrics else None
            dl_desc = self._build_flow_desc(dl_metrics, FlowDirection.downlink) if dl_metrics else None

            app = self.ip_app_map.get(app_ip)
            traffic_char = TrafficCharacterization(
                appId=app.app_id if app else "",
                fDescs=[d for d in [ul_desc, dl_desc] if d] or None,
                ulVol=int(sum(ul_vols) / len(ul_vols)) if ul_vols else None,
                dlVol=int(sum(dl_vols) / len(dl_vols)) if dl_vols else None,
                ulVolVariance=population_variance(ul_vols),
                dlVolVariance=population_variance(dl_vols),
            )

            if not any([traffic_char.ulVol, traffic_char.dlVol, traffic_char.ulVolVariance, traffic_char.dlVolVariance]):
                continue

            ue_comm = UeCommunication(
                commDur=comm_dur,                       # Not meaningful in the implementation
                commDurVariance=0.0,                    # Not meaningful in the implementation
                ts=ts_start,                            # Not meaningful in the implementation
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
    def _build_flow_desc(metrics: list[dict], direction: FlowDirection) -> IpEthFlowDescription | None:
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

