import asyncio
from collections import defaultdict
from dataclasses import dataclass
from pydantic import Field, IPvAnyAddress
from ipaddress import IPv4Address
from typing import Any, Callable

import httpx
import logging
from datetime import datetime, timedelta, timezone
from pymongo.synchronous.database import Database
from sqlalchemy.orm import Session

from app import schemas
from app.core.notification_responder import notification_responder
from app.schemas.analyticsExposure import AnalyticsEvent, WlanPerUeIdPerformanceInfo, WlanPerTsPerformanceInfo, \
    WlanPerSsIdPerformanceInfo, WlanPerformInfo, AnalyticsEventNotif
from app.schemas.commonData import BitRate, DurationSec
from app.crud import crud_mongo, ue as crud_ue
from app.schemas.monitoringevent import TrafficInformation


@dataclass
class Query:
    expr: str
    name: str


@dataclass
class FieldMapping:
    field: str
    converter: Callable

UE_UL_THR_QUERY = Query(expr='8 * sum by (src_ip) (rate(tap_5g_atnog_throughput_bytes_total{{src_ip=~"{ip}"}}[{interval}]))', name='UE_UL_THR')
UE_DL_THR_QUERY = Query(expr='8 * sum by (dst_ip) (rate(tap_5g_atnog_throughput_bytes_total{{dst_ip=~"{ip}"}}[{interval}]))', name='UE_DL_THR')
UE_UL_VOL_QUERY = Query(expr='8 * sum by (src_ip) (increase(tap_5g_atnog_throughput_bytes_total{{src_ip=~"{ip}"}}[{interval}]))', name='UE_UL_VOL')
UE_DL_VOL_QUERY = Query(expr='8 * sum by (dst_ip) (increase(tap_5g_atnog_throughput_bytes_total{{dst_ip=~"{ip}"}}[{interval}]))', name='UE_DL_VOL')
ALL_UL_THR_QUERY = Query(expr='8 * sum(rate(tap_5g_atnog_throughput_bytes_total{{src_ip=~"{ip}"}}[{interval}]))', name='ALL_UL_THR')
ALL_DL_THR_QUERY = Query(expr='8 * sum(rate(tap_5g_atnog_throughput_bytes_total{{dst_ip=~"{ip}"}}[{interval}]))', name='ALL_DL_THR')
ALL_UL_VOL_QUERY = Query(expr='8 * sum(increase(tap_5g_atnog_throughput_bytes_total{{src_ip=~"{ip}"}}[{interval}]))', name='ALL_UL_VOL')
ALL_DL_VOL_QUERY = Query(expr='8 * sum(increase(tap_5g_atnog_throughput_bytes_total{{dst_ip=~"{ip}"}}[{interval}]))', name='ALL_DL_VOL')


db_collection = "AnalyticsExposure"

@dataclass
class PollerState:
    loop_start_time: float
    next_poll_time: float
    stop_loop: bool = False

class SubscriptionState(schemas.AnalyticsExposureSubsc):
    queries: list[list[Query]] = Field(default_factory=list)
    user_equipments: list[list[schemas.UE]] = Field(default_factory=list)



class SubscriptionAnalyticsPoller:

    # TODO: IMPORTANT: E2eDataVolTransTimeReq also has timestamp (so what's the extraReportReq for?)
    # TODO: IMPORTANT: add predictions not implemente to endpoints
    # TODO: Mongo Client is Sync
    # TODO: SQLAlchemy Client is Sync
    # TODO: logging package is sync (change to async logging)
    # TODO: check what happens if not reachable (how does asyncio handle exception)
    # TODO: handle multiple UEs
    # TODO: verify repPeriod and such bs
    # TODO: handle offset period 0

    def __init__(self, prometheus_url: str, ):
        self.__httpx_client = httpx.AsyncClient(
            base_url=prometheus_url,
            timeout=httpx.Timeout(10, connect=3.05, read=27)
        )
        self.poll_interval = 10.0
        self.interval = '1m'

    async def poll(self, db_mongo: Database, db_sql: Session, *, subscription_id: str, db_event: asyncio.Event):
        # 27 March: 14:40 - 14:50 (interval=10m, window=10m, step=5s)
        # ip = '10.255.32.22'

        doc = crud_mongo.read_uuid(db_mongo, db_collection, subscription_id)
        if doc is None:
            logging.error(f"Subscription with id {subscription_id} not found in database")
            return

        doc.pop("owner_id")
        subscription = schemas.AnalyticsExposureSubsc(**doc)
        if subscription.analyEventsSubs is None:
            logging.warning("Subscription has no analyEventsSubs, cannot poll")
            return

        subscription_state = SubscriptionState(**subscription.dict())

        subscription_state.queries = []
        subscription_state.user_equipments = []
        for idx, s in enumerate(subscription_state.analyEventsSubs):
            subscription_state.queries.append(await self.__prepare_queries(s))
            subscription_state.user_equipments.append(await self.__prepare_ues(db_sql, s))

        notif_method = subscription_state.analyRepInfo.notifMethod
        send_notif_func = self.__send_onEventDetection_notification
        poll_interval = self.poll_interval
        if notif_method == schemas.NotificationMethod.onEventDetection:
            # TODO: INCOGNITA -> don't know where such thresholds are defined
            pass
        elif notif_method == schemas.NotificationMethod.periodic:
            poll_interval = subscription_state.analyRepInfo.repPeriod or poll_interval
            send_notif_func = self.__send_periodic_notification
        elif notif_method == schemas.NotificationMethod.oneTime:
            send_notif_func = self.__send_oneTime_notification

        loop = asyncio.get_running_loop()
        poller_state = PollerState(loop_start_time=0.0, next_poll_time=loop.time())

        while True:
            poller_state.loop_start_time = loop.time()
            notification = None

            if db_event.is_set():
                doc = crud_mongo.read_uuid(db_mongo, db_collection, subscription_id)
                if doc is None:
                    logging.info(f"Subscription with id {subscription_id} not found in database (it was deleted), stopping")
                    return

                doc.pop("owner_id")
                new_data = schemas.AnalyticsExposureSubsc(**doc)
                subscription_state.__dict__.update(new_data.__dict__)

                subscription_state.queries = []
                subscription_state.user_equipments = []
                for idx, s in enumerate(subscription_state.analyEventsSubs):
                    queries = await self.__prepare_queries(s)
                    ues = await self.__prepare_ues(db_sql, s)
                    subscription_state.queries.append(queries)
                    subscription_state.user_equipments.append(ues)
                    if len(queries) > 0 and len(ues) > 0:
                        notification = await self.__actually_query(s, queries, ues)

                db_event.clear()
            else:
                for idx, s in enumerate(subscription_state.analyEventsSubs):
                    queries = subscription_state.queries[idx]
                    ues = subscription_state.user_equipments[idx]
                    if len(queries) > 0 and len(ues) > 0:
                        notification = await self.__actually_query(s, queries, ues)


            if notification is not None:
                await send_notif_func(subscription_state, poller_state, notification)

            if poller_state.stop_loop:
                return

            poller_state.next_poll_time += poll_interval
            sleep_duration = poller_state.next_poll_time - loop.time()
            if sleep_duration > 0:
                await asyncio.sleep(sleep_duration)
            else:
                # poll took longer than poll_interval — skip ahead
                logging.warning("Poll took longer than poll_interval, skipping to next deadline")
                poller_state.next_poll_time = loop.time() + poll_interval

    async def __prepare_queries(self, s: schemas.AnalyticsEventSubsc) -> list[Query]:
        analy_event = s.analyEvent

        if analy_event == AnalyticsEvent.wlanPerformance:
            return [
                UE_UL_THR_QUERY, UE_UL_VOL_QUERY,
                UE_DL_THR_QUERY, UE_DL_VOL_QUERY,
                ALL_UL_THR_QUERY, ALL_UL_VOL_QUERY,
                ALL_DL_THR_QUERY, ALL_DL_VOL_QUERY,
            ]
        else:
            logging.warning("Unsupported analytics event type: %s", analy_event)
            return []

    async def __prepare_ues(self, db: Session, s: schemas.AnalyticsEventSubsc) -> list[schemas.UE]:

        user_equipments = []
        if s.tgtUe.gpsi:
            tgtUe = s.tgtUe.gpsi
            if tgtUe.startswith("msisdn-"):
                user_equipments.append(crud_ue.get_supi(db, supi=tgtUe.split("msisdn-")[1]))

            elif tgtUe.startswith("extid-"):
                user_equipments.append(crud_ue.get_externalId(
                    db, externalId=tgtUe.split("extid-")[1]
                ))

        elif s.tgtUe.exterGroupId:
            user_equipments = crud_ue.get_by_exterGroupId(db, exterGroupId=s.tgtUe.exterGroupId)

        if len(user_equipments) == 0:
            logging.warning("No UEs found for tgtUe %s", s.tgtUe)

        return user_equipments

    async def __actually_query(self, s: schemas.AnalyticsEventSubsc, queries: list[Query], ues: list[schemas.UE]) -> AnalyticsEventNotif:
        # TODO verify if lists are not empty. If so send it back
        analy_event = s.analyEvent
        analy_filter = s.analyEventFilter
        extra_report_req = analy_filter.extraReportReq or schemas.EventReportingRequirement()
        offsetPeriod_delta = timedelta(seconds=extra_report_req.offsetPeriod) if extra_report_req.offsetPeriod is not None else None
        temporal_gran_size = analy_filter.temporalGranSize if analy_filter.temporalGranSize is not None else 60

        start_ts, end_ts = SubscriptionAnalyticsPoller.__resolve_query_start_end_ts(
            temporal_gran_size=temporal_gran_size,
            offset_period=offsetPeriod_delta,
            start_ts=extra_report_req.startTs,
            end_ts=extra_report_req.endTs
        )

        temporal_gran_size_td = timedelta(seconds=temporal_gran_size)
        interval = SubscriptionAnalyticsPoller.__timedelta_to_prom_duration(temporal_gran_size_td)
        built_query = self.__build_multi_query(
            queries=queries,
            interval=interval,
            ue_ips=ues,
            all_ue_ips=["10.255.32.22"]
        )

        ues_by_ip = {str(ue.ip_address_v4): ue for ue in ues if ue.ip_address_v4 is not None}
        ip_address = "10.255.32.22"
        ues_by_ip[ip_address] = schemas.UE(
            ip_address_v4=ip_address
        )

        # TODO may be this in certain conditions (start_ts = end_ts or offsetPeriod = 0)
        # result = await self.__prom_query(
        #     query=built_query,
        #     time=extra_report_req.endTs
        # )

        result = await self.__prom_query_range(built_query, start=start_ts, end=end_ts, step=interval)

        if result is None:
            logging.warning("Unexpected query result: %s", result)
        elif analy_event == AnalyticsEvent.wlanPerformance:
            # tsStart=(
            #
            # ),
            # tsDuration=(
            #     offsetPeriod_delta if offsetPeriod_delta is not None
            #     else (extra_report_req.endTs - extra_report_req.startTs) if (extra_report_req.startTs and extra_report_req.endTs)
            #     else
            # )
            METRIC_TYPE_TO_FIELD = {
                "UE_UL_THR": FieldMapping("uplinkRate", SubscriptionAnalyticsPoller.__bps_to_xbps_bitrate),
                "UE_DL_THR": FieldMapping("downlinkRate", SubscriptionAnalyticsPoller.__bps_to_xbps_bitrate),
                "UE_UL_VOL": FieldMapping("uplinkVolume", lambda fs: int(float(fs))),
                "UE_DL_VOL": FieldMapping("downlinkVolume", lambda fs: int(float(fs))),
                "ALL_UL_THR": FieldMapping("uplinkRate", SubscriptionAnalyticsPoller.__bps_to_xbps_bitrate),
                "ALL_DL_THR": FieldMapping("downlinkRate", SubscriptionAnalyticsPoller.__bps_to_xbps_bitrate),
                "ALL_UL_VOL": FieldMapping("uplinkVolume", lambda fs: int(float(fs))),
                "ALL_DL_VOL": FieldMapping("downlinkVolume", lambda fs: int(float(fs))),
            }


            # { ue_ip : { ts: WlanPerTsPerformanceInfo } }
            ue_traffic_info: defaultdict[str, dict[datetime, WlanPerTsPerformanceInfo]] = defaultdict(dict)
            print("DEBUG START")

            # TODO: value -> __prom_query
            # TODO: values -> __prom_query_range

            for r in result:
                print("entry r", r)
                if (
                    'values' in r and 'metric' in r
                    and 'type' in r['metric'] and 'ue' in r['metric']
                ):
                    metric_ue_ip = r['metric']['ue']
                    metric_type = r['metric']['type']
                    for ts, value in r['values']:
                        mapping = METRIC_TYPE_TO_FIELD.get(metric_type)
                        if mapping is not None and mapping.field in TrafficInformation.__fields__:
                            ts_dt = datetime.fromtimestamp(ts)
                            wlan_ts_perf = ue_traffic_info[metric_ue_ip].setdefault(ts_dt, WlanPerTsPerformanceInfo(
                                tsStart=ts_dt - temporal_gran_size_td,
                                tsDuration=temporal_gran_size,
                                trafficInfo=TrafficInformation()
                            ))
                            setattr(wlan_ts_perf.trafficInfo, mapping.field, mapping.converter(value))

            wlanPerSsidInfos = []
            wlanPerUeIdInfos = []
            for ue_ip in ue_traffic_info:
                if ue_ip == "all":
                    wlanPerSsidInfos.append(WlanPerSsIdPerformanceInfo(
                        ssId='5g-atnog',
                        wlanPerTsInfos=list(ue_traffic_info[ue_ip].values()),
                    ))
                    continue
                else:
                    local = []
                    for info in ue_traffic_info[ue_ip].values():
                        if info.trafficInfo.downlinkVolume is not None and info.trafficInfo.uplinkVolume is not None:
                            info.trafficInfo.totalVolume = info.trafficInfo.downlinkVolume + info.trafficInfo.uplinkVolume
                        local.append(info)
                    wlanPerUeIdInfos.append(WlanPerUeIdPerformanceInfo(
                        supi=ues_by_ip[ue_ip].supi,
                        wlanPerTsInfos=local
                    ))

            # TODO: numberOfUes
            return AnalyticsEventNotif(
                analyEvent=analy_event,
                timeStamp=datetime.now(),
                wlanInfos=[WlanPerformInfo(
                    wlanPerSsidInfos=wlanPerSsidInfos,
                    wlanPerUeIdInfos=wlanPerUeIdInfos
                )]
            )
        else:
            raise Exception("not implemented")




    async def __prom_query(self,
        query: str,
        *,
        time: datetime | None = None,
        timeout: str | None = None,
        limit: int | None = None,
        lookback_delta: float | None = None,
        stats: str | None = None,
        request_timeout: float | None = 30.0
    ) -> dict | None:

        params = {"query": query}
        params |= {"time": time.timestamp()} if time else {}
        print("Querying Prometheus with params: ", params)
        resp = await self.__httpx_client.get("/api/v1/query", params=params, timeout=request_timeout)

        if resp.status_code != 200:
            logging.critical(f"Error while querying Prometheus (status code: {resp.status_code}): {resp.text}")
            return None

        result = resp.json()["data"]["result"]
        print("result __prom_query: ", result)
        return result

    async def __prom_query_range(self,
        query: str,
        *,
        start: datetime,
        end: datetime,
        step: str,
        timeout: str | None = None,
        limit: int | None = None,
        lookback_delta: float | None = None,
        stats: str | None = None,
        request_timeout: float | None = 30.0,
    ) -> dict | None:
        resp = await self.__httpx_client.get("/api/v1/query_range", params={
            "query": query, "start": start.timestamp(), "end": end.timestamp(), "step": step
        })

        if resp.status_code != 200:
            logging.critical(f"Error while querying Prometheus (status code: {resp.status_code}): {resp.text}")
            return None

        result = resp.json()["data"]["result"]
        print("result __prom_query_range: ", result)
        return result




    async def __send_periodic_notification(self, subscription: schemas.AnalyticsExposureSubsc, poller_state: PollerState, json: Any):
        await SubscriptionAnalyticsPoller.__send_notification(subscription.notifUri, json)

    async def __send_oneTime_notification(self, subscription: schemas.AnalyticsExposureSubsc, poller_state: PollerState, json: Any):
        is_sent = await SubscriptionAnalyticsPoller.__send_notification(subscription.notifUri, json)
        poller_state.stop_loop = is_sent

    async def __send_onEventDetection_notification(self, subscription: schemas.AnalyticsExposureSubsc, poller_state: PollerState, json: Any):
        on_event = True
        if on_event:
            await SubscriptionAnalyticsPoller.__send_notification(subscription.notifUri, json)

    @staticmethod
    async def __send_notification(notif_uri: str, json: Any) -> bool:
        try:
            print("sending json ", json)
            await notification_responder.send_notification(notif_uri, json)
            return True
        except Exception as e:
            logging.error(f"Error while sending notification: {e}")
            return False


    @staticmethod
    def __build_query(query: Query, **kwargs):
        return query.expr.format(**kwargs)

    @staticmethod
    def __resolve_query_start_end_ts(*, temporal_gran_size: DurationSec | None = None, offset_period: timedelta | None = None, start_ts: datetime | None = None, end_ts: datetime | None = None):
        # adjust start_ts based on temporal_gran_size
        if offset_period is not None:
            start, end = (datetime.now() - offset_period, datetime.now())
        elif start_ts is not None:
            start, end = (start_ts, end_ts or datetime.now())
        else:
            raise Exception("start and end time are required, Not implemented")
        if temporal_gran_size is not None and (end - start).total_seconds() > temporal_gran_size:
            start += timedelta(seconds=temporal_gran_size)
        return start, end

    def __build_multi_query(self, queries: list[Query], ue_ips: list[schemas.UE], all_ue_ips: list[str], interval: str, **kwargs):

        ip_regex_per_ue = '|'.join(str(ue.ip_address_v4) for ue in ue_ips)
        ip_regex_per_ue += '|10.255.32.22'
        ip_regex_all = '|'.join(all_ue_ips)
        print("ip_regex_per_ue", ip_regex_per_ue)
        print("ip_regex_all", ip_regex_all)
        def wrap_query(q: Query) -> str:
            direction_ip_label = 'src_ip' if 'UL' in q.name else 'dst_ip'
            if "ALL" in q.name:
                ip_regex = ip_regex_all
                replaced_label = "all"
            else:
                ip_regex = ip_regex_per_ue
                replaced_label = "$1"
            return (
                f'label_replace('
                    f'label_replace({q.expr.format(**kwargs, ip=ip_regex, interval=interval)}, "type", "{q.name}", "", ""), '
                    f'"ue", "{replaced_label}", "{direction_ip_label}", "(.*)"'
                f')'
            )


        return (
                'sum by (type, ue) (' +
                ' or '.join(wrap_query(q) for q in queries) +
                ')'
        )

    def __build_multi_query_all_ues(self, queries: list[Query], interval: str, **kwargs):
        def wrap_query(q: Query) -> str:
            return (
                f'label_replace({q.expr.format(**kwargs, ip=".*", interval=interval)}, "type", "{q.name}", "", "")'
            )

        return (
            'sum by (type) (' +
            ' or '.join(wrap_query(q) for q in queries) +
            ')'
        )




    @staticmethod
    def __timedelta_to_prom_duration(td: timedelta):
        total_ms = (
            td.days * 86_400_000
            + td.seconds * 1_000
            + td.microseconds // 1_000
        )

        if total_ms < 0:
            raise ValueError("Prometheus does not support negative durations")


        units = [
            ("y",  365 * 24 * 60 * 60 * 1000),
            ("w",  7   * 24 * 60 * 60 * 1000),
            ("d",  24  * 60 * 60 * 1000),
            ("h",  60  * 60 * 1000),
            ("m",  60  * 1000),
            ("s",  1000),
            ("ms", 1),
        ]

        parts = []
        for suffix, ms_val in units:
            if total_ms >= ms_val:
                parts.append(f"{total_ms // ms_val}{suffix}")
                total_ms %= ms_val

        return "".join(parts) or '0s'

    @staticmethod
    def __bps_to_xbps_bitrate(bitrate_value: str) -> BitRate:
        units = {"Tbps": 1e12, "Gbps": 1e9, "Mbps": 1e6, "Kbps": 1e3}
        value = float(bitrate_value)
        for unit, threshold in units.items():
            if value >= threshold:
                return f"{value / threshold:.2f} {unit}"
        return f"{value:.2f} bps"

    @staticmethod
    def __bps_to_xbps_float(bitrate_value: str) -> float:
        units = {"Tbps": 1e12, "Gbps": 1e9, "Mbps": 1e6, "Kbps": 1e3}
        value = float(bitrate_value)
        for unit, threshold in units.items():
            if value >= threshold:
                return float(value / threshold)
        return float(value)


subscription_analytics_poller = SubscriptionAnalyticsPoller(
    prometheus_url="http://10.255.28.207:30090"
)
