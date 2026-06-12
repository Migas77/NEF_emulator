from typing import Optional, List, Union
from datetime import datetime, timezone

from app.schemas.monitoringevent import CivicAddress, GeographicArea, TrafficInformation, \
    VelocityEstimate, GeographicalCoordinates
from pydantic import Field, IPvAnyAddress, AnyHttpUrl, constr, root_validator, validator, StrictStr, StrictInt, \
    StrictBool, conint
from enum import Enum
from .commonData import (
    Snssai,
    TimeWindow,
    FlowInfo,
    Ecgi,
    Ncgi,
    GlobalRanNodeId,
    Tai,
    UserLocation,
    RatType,
    QosResourceType,
    AnalyticsSubset,
    PartitioningCriteria,
    NotificationFlag,
    WebsockNotifConfig, SamplingRatio, DurationSec, Uinteger, BitRate, PacketDelBudget, PacketErrRate,
    SupportedFeatures, Volume, IpAddr,
)
from .cpParameterProvisioning import ScheduledCommunicationTime, StationaryIndication, ScheduledCommunicationType
from .utils import ExtraBaseModel


############### TS 29.517 Naf_EventExposure Types ###############


class AddrFqdn(ExtraBaseModel):
    """IP address and/or FQDN."""

    ipAddr: IpAddr = Field(None)
    fqdn: str = Field(None, description="Indicates an FQDN.")


class SvcExperience(ExtraBaseModel):
    """Contains a mean opinion score with the customized range."""

    mos: float
    upperRange: float
    lowerRange: float


###############################################

############### TS 29.508 Nsmf_EventExposure Types ###############


class UpfInformation(ExtraBaseModel):
    """Represents the ID/address/FQDN of the UPF."""

    upfId: str
    upfAddr: AddrFqdn


###############################################

############### TS 29.520 Nnwdaf_EventsSubscription Types ###############


class NotificationMethod(str, Enum):
    periodic = "PERIODIC"
    oneTime = "ONE_TIME"
    onEventDetection = "ON_EVENT_DETECTION"


###############################################

############### TS 29.571 CommonData Types ###############


class BufferedNotificationsAction(str, Enum):
    """Indicates the required action by the event producer NF on the buffered Notifications."""

    sendAll = "SEND_ALL"
    discardAll = "DISCARD_ALL"
    dropOld = "DROP_OLD"

class SubscriptionAction(str, Enum):
    """Indicates the required action by the event producer NF on the event subscription if an exception occurs while the event is muted."""

    close = "CLOSE"
    continueWithMuting = "CONTINUE_WITH_MUTING"
    continueWithoutMuting = "CONTINUE_WITHOUT_MUTING"


class MutingExceptionInstructions(ExtraBaseModel):
    """Indicates to an Event producer NF instructions for the subscription and stored events when an exception (e.g.
    full buffer) occurs at the Event producer NF while the event is muted."""

    bufferedNotifs: Optional[BufferedNotificationsAction] = Field(None)
    subscription: Optional[SubscriptionAction] = Field(None)


class MutingNotificationsSettings(ExtraBaseModel):
    """Indicates the Event producer NF settings to the Event consumer NF"""

    maxNoOfNotif: Optional[int] = Field(None)
    durationBufferedNotif: Optional[int] = Field(None, ge=0)


class TrafficProfile(str, Enum):
    single_trans_ul = "SINGLE_TRANS_UL"
    single_trans_dl = "SINGLE_TRANS_DL"
    dual_trans_ul_first = "DUAL_TRANS_UL_FIRST"
    dual_trans_dl_first = "DUAL_TRANS_DL_FIRST"

class BatteryIndication(ExtraBaseModel):
    """Parameters \"replaceableInd\" and \"rechargeableInd\" are only included if the value of Parameter \"batteryInd\" is true. """

    batteryInd: Optional[StrictBool] = Field(default=None, description="This IE shall indicate whether the UE is battery powered or not. true: the UE is battery powered; false or absent: the UE is not battery powered ")
    replaceableInd: Optional[StrictBool] = Field(default=None, description="This IE shall indicate whether the battery of the UE is replaceable or not. true: the battery of the UE is replaceable; false or absent: the battery of the UE is not replaceable. ")
    rechargeableInd: Optional[StrictBool] = Field(default=None, description="This IE shall indicate whether the battery of the UE is rechargeable or not. true: the battery of UE is rechargeable; false or absent: the battery of the UE is not rechargeable. ")


class Supi(ExtraBaseModel):
    __root__: constr(regex=r"^(imsi-[0-9]{5,15}|nai-.+|gci-.+|gli-.+|.+)$") = Field(
        ...,
        description='String identifying a Supi that shall contain either an IMSI, a network specific identifier,\na Global Cable Identifier (GCI) or a Global Line Identifier (GLI) as specified in clause \n2.2A of 3GPP TS 23.003. It shall be formatted as follows\n - for an IMSI "imsi-<imsi>", where <imsi> shall be formatted according to clause 2.2\n   of 3GPP TS 23.003 that describes an IMSI.\n - for a network specific identifier "nai-<nai>, where <nai> shall be formatted\n   according to clause 28.7.2 of 3GPP TS 23.003 that describes an NAI.\n - for a GCI "gci-<gci>", where <gci> shall be formatted according to clause 28.15.2\n   of 3GPP TS 23.003.\n - for a GLI "gli-<gli>", where <gli> shall be formatted according to clause 28.16.2 of\n   3GPP TS 23.003.To enable that the value is used as part of an URI, the string shall\n   only contain characters allowed according to the "lower-with-hyphen" naming convention\n   defined in 3GPP TS 29.501.\n',
    )


class Gpsi(ExtraBaseModel):
    __root__: constr(regex=r"^(msisdn-[0-9]{5,15}|extid-[^@]+@[^@]+|.+)$") = Field(
        ...,
        description="String identifying a Gpsi shall contain either an External Id or an MSISDN.  It shall be formatted as follows -External Identifier= \"extid-'extid', where 'extid'  shall be formatted according to clause 19.7.2 of 3GPP TS 23.003 that describes an  External Identifier. \n",
    )


class PduSessionType(str, Enum):
    """PduSessionType indicates the type of a PDU session."""

    IPV4 = "IPV4"
    IPV6 = "IPV6"
    IPV4V6 = "IPV4V6"
    UNSTRUCTURED = "UNSTRUCTURED"
    ETHERNET = "ETHERNET"

class SscMode(str, Enum):
    """represents the service and session continuity mode"""

    SSC_MODE_1 = "SSC_MODE_1"
    SSC_MODE_2 = "SSC_MODE_2"
    SSC_MODE_3 = "SSC_MODE_3"

class AccessType(str, Enum):
    """Indicates whether the access is via 3GPP or via non-3GPP"""
    _3GPP_ACCESS = "3GPP_ACCESS"
    NON_3GPP = "NON_3GPP"

class PduSessionInfo(ExtraBaseModel):
    """Represents combination of PDU Session parameter(s) information."""

    pduSessType: Optional[PduSessionType] = Field(None)
    sscMode: Optional[SscMode] = Field(None)
    accessTypes: Optional[List[AccessType]] = Field(None, min_items=1)


###############################################

############### TS 29.523 Npcf_EventExposure Types ###############


class ReportingInformation(ExtraBaseModel):
    """Represents the type of reporting that the subscription requires."""

    immRep: bool = False
    notifMethod: NotificationMethod = NotificationMethod.onEventDetection
    maxReportNbr: int = Field(None, description="", ge=0) # Is None if there is no limit
    monDur: datetime = Field(None, description="")
    repPeriod: int = Field(None, description="", ge=0)
    sampRatio: SamplingRatio = None
    partitionCriteria: List[PartitioningCriteria] = Field(
        None, description="", min_items=1
    )
    grpRepTime: int = Field(None, description="", ge=0)
    notifFlag: NotificationFlag = NotificationFlag.activate
    notifFlagInstruct: Optional[MutingExceptionInstructions] = Field(None)
    mutingSetting: Optional[MutingNotificationsSettings] = Field(None)

    @root_validator(skip_on_failure=True)
    def repPeriod_when_notifMethod_is_periodic(cls, v):
        if v.get("repPeriod") is not None and v.get("notifMethod") != NotificationMethod.periodic:
            raise ValueError('repPeriod may be supplied for notification method "PERIODIC"')
        return v


###############################################

############### TS 29.554 Npcf_BDTPolicyControl Types ###############


class NetworkAreaInfo(ExtraBaseModel):
    """Describes a network area information in which the NF service consumer requests the number of UEs."""

    ecgis: List[Ecgi] = Field(
        None, description="Contains a list of E-UTRA cell identities.", min_items=1
    )
    ncgis: List[Ncgi] = Field(
        None, description="Contains a list of NR cell identities.", min_items=1
    )
    gRanNodeIds: List[GlobalRanNodeId] = Field(
        None, description="Contains a list of NG RAN nodes.", min_items=1
    )
    tais: List[Tai] = Field(
        None, description="Contains a list of tracking area identities.", min_items=1
    )


###############################################

############### TS 29.503 Nudm Types ###############


class SuggestedPacketNumDl(ExtraBaseModel):
    suggestedPacketNumDl: int = Field(None, description="", ge=1)
    validityTime: datetime

class UmtTime(ExtraBaseModel):
    """UmtTime"""

    timeOfDay: StrictStr = Field(description="String with format partial-time or full-time as defined in clause 5.6 of IETF RFC 3339. Examples, 20:15:00, 20:15:00-08:00 (for 8 hours behind UTC).  ", alias="timeOfDay")
    dayOfWeek: int = Field(le=7, strict=True, ge=1, description="integer between and including 1 and 7 denoting a weekday. 1 shall indicate Monday, and the subsequent weekdays shall be indicated with the next higher numbers. 7 shall indicate Sunday. ", alias="dayOfWeek")


###############################################

############### TS 29.512 Npcf_SMPolicyControl Types ###############


class FlowDirection(str, Enum):
    downlink = "DOWNLINK"
    uplink = "UPLINK"
    bidirectional = "BIDIRECTIONAL"
    unspecified = "UNSPECIFIED"


###############################################

############### TS 29.514 Npcf_PolicyAuthorization Types ###############


class EthFlowDescription(ExtraBaseModel):
    """Identifies an Ethernet flow."""

    destMacAddr: Optional[constr(regex=r"^([0-9a-fA-F]{2})((-[0-9a-fA-F]{2}){5})$")] = Field(None)
    ethType: str
    fDesc: Optional[str] = Field(None, description="Defines a packet filter of an IP flow.")
    fDir: Optional[FlowDirection] = Field(None)
    sourceMacAddr: Optional[constr(regex=r"^([0-9a-fA-F]{2})((-[0-9a-fA-F]{2}){5})$")] = Field(None)
    vlanTags: Optional[List[str]] = Field(None, description="", min_items=1, max_items=2)
    srcMacAddrEnd: Optional[constr(regex=r"^([0-9a-fA-F]{2})((-[0-9a-fA-F]{2}){5})$")] = Field(None)
    destMacAddrEnd: Optional[constr(regex=r"^([0-9a-fA-F]{2})((-[0-9a-fA-F]{2}){5})$")] = Field(None)


###############################################

############### TS 29.122 CommonData Types ###############


class LocationArea(ExtraBaseModel):
    """LocationArea"""

    geographicAreas: Optional[List[GeographicArea]] = Field(min_items=0, default=None, description="Identifies a list of geographic area of the user where the UE is located.")
    civicAddresses: Optional[List[CivicAddress]] = Field(min_items=0, default=None, description="Identifies a list of civic addresses of the user where the UE is located.")
    nwAreaInfo: Optional[NetworkAreaInfo] = Field(default=None)
    umtTime: Optional[UmtTime] = Field(default=None)

class LocationArea5G(ExtraBaseModel):
    geographicAreas: Optional[List[GeographicArea]] = Field(
        None,
        description="Identifies a list of geographic area of the user where the UE is located.",
        min_items=0,
    )
    civicAddresses: Optional[List[CivicAddress]] = Field(
        None,
        description="Identifies a list of civic addresses of the user where the UE is located.",
        min_items=0,
    )
    nwAreaInfo: Optional[NetworkAreaInfo] = None


###############################################

############### TS 29.503 Nudm Types ###############


class ExpectedUeBehaviourData(ExtraBaseModel):
    """IP address and/or FQDN."""

    stationaryIndication: Optional[StationaryIndication] = Field(default=None)
    communicationDurationTime: Optional[StrictInt] = Field(default=None, description="indicating a time in seconds.")
    periodicTime: Optional[StrictInt] = Field(default=None, description="indicating a time in seconds.")
    scheduledCommunicationTime: Optional[ScheduledCommunicationTime] = Field(default=None)
    scheduledCommunicationType: Optional[ScheduledCommunicationType] = Field(default=None)
    Identifies: Optional[List[LocationArea]] = Field(default=None, min_items=1, description="Identifies the UE's expected geographical movement. The attribute is only applicable in 5G. ")
    trafficProfile: Optional[TrafficProfile] = Field(default=None)
    batteryIndication: Optional[BatteryIndication] = Field(default=None)
    validityTime: Optional[datetime] = Field(default=None, description="string with format 'date-time' as defined in OpenAPI.")
    confidenceLevel: Optional[str] = Field(default=None, strict=True)
    accuracyLevel: Optional[str] = Field(default=None, strict=True)


###############################################

############### TS 29.520 Nnwdaf_EventsSubscription Types ###############


class Direction(str, Enum):
    north = "NORTH"
    south = "SOUTH"
    east = "EAST"
    west = "WEST"
    northwest = "NORTHWEST"
    northeast = "NORTHEAST"
    southwest = "SOUTHWEST"
    southeast = "SOUTHEAST"


class NetworkPerfType(str, Enum):
    gnbActiveRatio = "GNB_ACTIVE_RATIO"
    gnbComputingUsage = "GNB_COMPUTING_USAGE"
    gnbMemoryUsage = "GNB_MEMORY_USAGE"
    gnbDiskUsage = "GNB_DISK_USAGE"
    gnbRscUsageOverallTraffic = "GNB_RSC_USAGE_OVERALL_TRAFFIC"
    gnbRscUsageGbrTraffic = "GNB_RSC_USAGE_GBR_TRAFFIC"
    gnbRscUsageDelayCritGbrTraffic = "GNB_RSC_USAGE_DELAY_CRIT_GBR_TRAFFIC"
    numOfUE = "NUM_OF_UE"
    sessSuccRatio = "SESS_SUCC_RATIO"
    hoSuccRatio = "HO_SUCC_RATIO"


class ExceptionId(str, Enum):
    unexpectedUELocation = "UNEXPECTED_UE_LOCATION"
    unexpectedLongLiveFlow = "UNEXPECTED_LONG_LIVE_FLOW"
    unexpectedLargeRateFlow = "UNEXPECTED_LARGE_RATE_FLOW"
    unexpectedWakeup = "UNEXPECTED_WAKEUP"
    suspecionOfDdosAttack = "SUSPICION_OF_DDOS_ATTACK"
    wrongDestinationAddress = "WRONG_DESTINATION_ADDRESS"
    tooFrequentServiceAccess = "TOO_FREQUENT_SERVICE_ACCESS"
    unexpectedRatioLinkFailures = "UNEXPECTED_RADIO_LINK_FAILURES"
    pingPongAcrossCells = "PING_PONG_ACROSS_CELLS"


class ExceptionTrend(str, Enum):
    up = "UP"
    down = "DOWN"
    unknow = "UNKNOW"
    stable = "STABLE"


class DispersionClass(str, Enum):
    fixed = "FIXED"
    camper = "CAMPER"
    traveller = "TRAVELLER"
    topHeavy = "TOP_HEAVY"


class CongestionType(str, Enum):
    userPlane = "USER_PLANE"
    controlPlane = "CONTROL_PLANE"
    userAndControlPlane = "USER_AND_CONTROL_PLANE"


class TimeUnit(str, Enum):
    minute = "MINUTE"
    hour = "HOUR"
    day = "DAY"


class DispersionType(str, Enum):
    dvda = "DVDA"
    tda = "TDA"
    dvdaAndTda = "DVDA_AND_TDA"


class ServiceExperienceType(str, Enum):
    voice = "VOICE"
    video = "VIDEO"
    other = "OTHER"


class MatchingDirection(str, Enum):
    ascending = "ASCENDING"
    descending = "DESCENDING"
    crossed = "CROSSED"


class DispersionOrderingCriterion(str, Enum):
    timeSlotStart = "TIME_SLOT_START"
    dispersion = "DISPERSION"
    classification = "CLASSIFICATION"
    ranking = "RANKING"
    percentileRanking = "PERCENTILE_RANKING"


class ExpectedAnalyticsType(str, Enum):
    mobility = "MOBILITY"
    commun = "COMMUN"
    mobilityAndCommun = "MOBILITY_AND_COMMUN"


class DnPerfOrderingCriterion(str, Enum):
    avgTrafficRate = "AVERAGE_TRAFFIC_RATE"
    macTrafficRate = "MAXIMUM_TRAFFIC_RATE"
    avgPacketDelay = "AVERAGE_PACKET_DELAY"
    maxPacketDelay = "MAXIMUM_PACKET_DELAY"
    avgPacketLossRate = "AVERAGE_PACKET_LOSS_RATE"


class Accuracy(str, Enum):
    low = "LOW"
    medium = "MEDIUM"
    high = "HIGH"
    highest = "HIGHEST"


class AnalyticsMetadata(str, Enum):
    numOfSamples = "NUM_OF_SAMPLES"
    dataWindow = "DATA_WINDOW"
    dataStatProps = "DATA_STAT_PROPS"
    strategy = "STRATEGY"
    accuracy = "ACCURACY"


class DatasetStatisticalProperty(str, Enum):
    uniformDistData = "UNIFORM_DIST_DATA"
    noOutliers = "NO_OUTLIERS"


class OutputStrategy(str, Enum):
    binary = "BINARY"
    gradient = "GRADIENT"


class NwdafFailureCode(str, Enum):
    unavailableData = "UNAVAILABLE_DATA"
    bothStatPredNotAllowed = "BOTH_STAT_PRED_NOT_ALLOWED"
    predictionNotAllowed = "PREDICTION_NOT_ALLOWED"
    unsatisfiedRequestedAnalyticsTime = "UNSATISFIED_REQUESTED_ANALYTICS_TIME"
    noRoamingSupport = "NO_ROAMING_SUPPORT"
    other = "OTHER"


class RetainabilityThreshold(ExtraBaseModel):
    """Represents a QoS flow retainability threshold."""

    relFlowNum: Optional[int] = Field(None, description="", ge=0)
    relTimeUnit: Optional[TimeUnit] = Field(None)
    relFlowRatio: Optional[int] = Field(None, description="Expressed in percent", ge=1, le=100)


class ThresholdLevel(ExtraBaseModel):
    """Represents a threshold level."""

    congLevel: Optional[int] = Field(None)
    nfLoadLevel: Optional[int] = Field(None)
    nfCpuUsage: Optional[int] = Field(None)
    nfMemoryUsage: Optional[int] = Field(None)
    nfStorageUsage: Optional[int] = Field(None)
    avgTrafficRate: BitRate = None
    maxTrafficRate: BitRate = None
    minTrafficRate: BitRate = None
    aggTrafficRate: BitRate = None
    var_traffic_rate: Optional[float] = Field(None)
    avgPacketDelay: Optional[int] = Field(None, description="Expressed in milliseconds.", ge=1)
    maxPacketDelay: Optional[int] = Field(None, description="Expressed in milliseconds.", ge=1)
    varPacketDelay: Optional[float] = Field(None)
    avgPacketLossRate: Optional[int] = Field(
        None, description="Expressed in tenth of percent", ge=0, le=1000
    )
    maxPacketLossRate: Optional[int] = Field(
        None, description="Expressed in tenth of percent", ge=0, le=1000
    )
    svcExpLevel: Optional[float] = Field(None)
    speed: Optional[float] = Field(None)


class TopApplication(ExtraBaseModel):
    """Top application that contributes the most to the traffic."""

    appId: str = Field(None, description="String providing an application identifier.")
    ipTrafficFilter: FlowInfo
    ratio: constr(regex=r"^\d+(\.\d+)? (bps|Kbps|Mbps|Gbps|Tbps)$")


class Exception(ExtraBaseModel):
    """Represents the Exception information."""

    excepId: ExceptionId
    excepLevel: int
    excepTrend: ExceptionTrend


class IpEthFlowDescription(ExtraBaseModel):
    """Contains the description of an Uplink and/or Downlink Ethernet flow."""

    ipTrafficFilter: str = Field(
        None, description="Defines a packet filter of an IP flow."
    )
    ethTrafficFilter: EthFlowDescription = Field(None)


class AddressList(ExtraBaseModel):
    """Represents a list of IPv4 and/or IPv6 addresses."""

    ipv4Addrs: List[IPvAnyAddress] = Field(
        None, description="String identifying an Ipv4 address", min_items=1
    )
    ipv6Addrs: List[IPvAnyAddress] = Field(
        None, description="String identifying an Ipv6 address", min_items=1
    )


class CircumstanceDescription(ExtraBaseModel):
    """Contains the description of a circumstance."""

    freq: float
    tm: datetime
    locArea: NetworkAreaInfo
    vol: Volume = None


class AdditionalMeasurement(ExtraBaseModel):
    """Represents additional measurement information."""

    unexpLoc: NetworkAreaInfo
    unexpFlowTeps: List[IpEthFlowDescription] = Field(None, description="", min_items=1)
    unexpWakes: List[datetime] = Field(None, description="", min_items=1)
    ddosAttack: AddressList
    wrgDest: AddressList
    circums: List[CircumstanceDescription] = Field(None, description="", min_items=1)


class TrafficCharacterization(ExtraBaseModel):
    """Identifies the detailed traffic characterization."""

    dnn: str = Field(
        "province1.mnc01.mcc202.gprs",
        description="String identifying the Data Network Name (i.e., Access Point Name in 4G). For more information check clause 9A of 3GPP TS 23.003",
    )
    snssai: Snssai = None
    appId: str = Field("", description="String providing an application identifier.")
    fDescs: Optional[List[IpEthFlowDescription]] = Field(
        None, description="", min_items=1, max_items=2
    )
    ulVol: Volume = None
    ulVolVariance: Optional[float] = Field(None)
    dlVol: Volume = None
    dlVolVariance: Optional[float] = Field(None)


class AppListForUeComm(ExtraBaseModel):
    """Represents the analytics of the application list used by UE."""

    appId: str = Field("", description="String providing an application identifier.")
    startTime: datetime = Field(None)
    appDur: int = Field(None, description="", ge=0)
    occurRatio: int = Field(None, description="Expressed in percent", ge=1, le=100)
    spatialValidity: NetworkAreaInfo = Field(None)


class SessInactTimerForUeComm(ExtraBaseModel):
    """Represents the N4 Session inactivity timer."""

    n4SessId: int = Field(None, description="", ge=0, le=255)
    sessInactiveTimer: int = Field(None, description="", ge=0)


class UeCommunication(ExtraBaseModel):
    """Represents UE communication information."""

    commDur: DurationSec
    commDurVariance: Optional[float] = Field(None)
    perioTime: DurationSec = None
    perioTimeVariance: Optional[float] = Field(None)
    ts: Optional[datetime] = Field(None, description="")
    tsVariance: Optional[float] = Field(None)
    recurringTime: Optional[ScheduledCommunicationTime] = Field(None)
    trafChar: TrafficCharacterization
    ratio: Optional[int] = Field(None, description="", ge=1, le=100)
    perioCommInd: Optional[bool] = False
    confidence: Optional[int] = Field(None, description="", ge=0)
    anaOfAppList: Optional[AppListForUeComm] = Field(None)
    sessInactTimer: Optional[SessInactTimerForUeComm] = Field(None)

    @root_validator(skip_on_failure=True)
    def ts_or_recurringTime_shall_be_provided(cls, v):
        if v.get("ts") is None and v.get("recurringTime") is None:
            raise ValueError('Either "ts" or "recurringTime" shall be provided.')
        return v


class ApplicationVolume(ExtraBaseModel):
    """ApplicationVolume"""

    appId: str = Field(None, description="String providing an application identifier.")
    appVolume: int = Field(None, description="", ge=0)


class DispersionCollection(ExtraBaseModel):
    """Dispersion collection per UE location or per slice."""

    ueLoc: Optional[UserLocation]
    snssai: Snssai = None
    supis: List[Supi] = Field(
        None, description="", min_items=1
    )
    gpsis: List[constr(regex=r"^(msisdn-[0-9]{5,15}|extid-.+@.+|.+)$")] = Field(
        None, description="", min_items=1
    )
    appVolumes: List[ApplicationVolume] = Field(None, description="", min_items=1)
    disperAmount: int = Field(None, description="", ge=0)
    disperClass: Optional[DispersionClass]
    usageRank: int = Field(None, description="", ge=1, le=3)
    percentileRank: int = Field(None, description="", ge=1, le=100)
    ueRatio: int = Field(None, description="", ge=1, le=100)
    confidence: int = Field(None, description="", ge=0)


class DispersionInfo(ExtraBaseModel):
    """Represents the Dispersion information. When subscribed event is "DISPERSION", the
    "disperInfos" attribute shall be included."""

    tsStart: datetime
    tsDuration: DurationSec
    disperCollects: List[DispersionCollection] = Field(description="", min_items=1)
    disperType = DispersionType


class PerfData(ExtraBaseModel):
    """Represents DN performance data."""

    avgTrafficRate: BitRate = None
    maxTrafficRate: BitRate = None
    minTrafficRate: BitRate = None
    aggTrafficRate: BitRate = None
    varTrafficRate: Optional[float] = Field(None)
    trafRateUeIds: Optional[List[Supi]] = Field(None, min_items=1)
    avePacketDelay: PacketDelBudget = None
    maxPacketDelay: PacketDelBudget = None
    varPacketDelay: Optional[float] = Field(None)
    packDelayUeIds: Optional[List[Supi]] = Field(None, min_items=1)
    avgPacketLossRate: Optional[int] = Field(None, le=1000, strict=True, ge=0)
    maxPacketLossRate: Optional[int] = Field(None, le=1000, strict=True, ge=0)
    varPacketLossRate: Optional[float] = Field(None)
    packLossUeIds: Optional[List[Supi]] = Field(None, min_items=1)
    numOfUe: Uinteger = None


class DnPerf(ExtraBaseModel):
    """Represents DN performance for the application."""

    appServerInsAddr: Optional[AddrFqdn]
    upfInfo: Optional[UpfInformation]
    dnai: str = Field(
        None,
        description="DNAI (Data network access identifier), see clause 5.6.7 of 3GPP TS 23.501.",
    )
    perfData: PerfData
    spatialValidCon: Optional[NetworkAreaInfo]
    temporalValidCon: Optional[TimeWindow]


class DnPerfInfo(ExtraBaseModel):
    """Represents DN performance information."""

    appId: str = Field(None, description="String providing an application identifier.")
    dnn: Optional[str] = Field(
        "province1.mnc01.mcc202.gprs",
        description="String identifying the Data Network Name (i.e., Access Point Name in 4G). For more information check clause 9A of 3GPP TS 23.003",
    )
    snssai: Snssai = None
    dnPerf: List[DnPerf] = Field(description="", min_items=1)
    confidence: int = Field(None, description="", ge=0)


class LocationInfo(ExtraBaseModel):
    """Represents UE location information."""

    loc: UserLocation
    ratio: int = Field(None, description="Expressed in percent.", ge=1, le=100)
    confidence: int = Field(None, description="", ge=0)


class RatFreqInformation(ExtraBaseModel):
    """Represents the RAT type and/or Frequency information."""

    allFreq: Optional[bool] = Field(
        False,
        description="Set to true to indicate to handle all the frequencies the NWDAF received, otherwise set to false or omit. The allFreq attribute and the freq attribute are mutually exclusive.",
    )
    allRat: Optional[bool] = Field(
        False,
        description="Set to true to indicate to handle all the RAT Types the NWDAF received, otherwise set to false or omit. The allRat attribute and the ratType attribute are mutually exclusive. ",
    )
    freq: Optional[int] = Field(None, description="", ge=0, le=3279165)
    ratType: Optional[RatType]
    svcExpThreshold: Optional[ThresholdLevel]
    matchingDir: Optional[MatchingDirection]


class ServiceExperienceInfo(ExtraBaseModel):
    """Represents service experience information."""

    svcExprc: SvcExperience
    svcExprcVariance: Optional[float]
    supis: List[Supi] = Field(
        None, description="", min_items=1
    )
    snssai: Snssai = None
    appId: str = Field("", description="String providing an application identifier.")
    srvExpcType: Optional[ServiceExperienceType]
    ueLocs: List[LocationInfo] = Field(None, description="", min_items=1)
    upfInfo: Optional[UpfInformation]
    dnai: str = Field(
        None,
        description="DNAI (Data network access identifier), see clause 5.6.7 of 3GPP TS 23.501.",
    )
    appServerInst: Optional[AddrFqdn]
    confidence: int = Field(None, description="", ge=0)
    dnn: Optional[str] = Field(
        "province1.mnc01.mcc202.gprs",
        description="String identifying the Data Network Name (i.e., Access Point Name in 4G). For more information check clause 9A of 3GPP TS 23.003",
    )
    networkArea: Optional[NetworkAreaInfo]
    nsiId: str = Field(
        None,
        description="Contains the Identifier of the selected Network Slice instance.",
    )
    ratio: int = Field(None, description="Expressed in percent.", ge=1, le=100)
    ratFreq: Optional[RatFreqInformation]
    pduSesInfo: Optional[PduSessionInfo] = Field(None)


class ClassCriterion(ExtraBaseModel):
    """Indicates the dispersion class criterion for fixed, camper and/or traveller UE, and/or the top-heavy UE dispersion class criterion."""

    disperClass: DispersionClass
    classThreshold: SamplingRatio
    thresMatch: MatchingDirection


class RankingCriterion(ExtraBaseModel):
    """Indicates the usage ranking criterion between the high, medium and low usage UE."""

    highBase: SamplingRatio
    lowBase: SamplingRatio


class DispersionRequirement(ExtraBaseModel):
    """Represents the dispersion analytics requirements."""

    disperType: DispersionType
    classCriters: List[ClassCriterion] = Field(None, description="", min_items=1)
    rankCriters: List[RankingCriterion] = Field(None, description="", min_items=1)
    dispOrderCriter: Optional[DispersionOrderingCriterion]
    order: Optional[MatchingDirection]


class NsiIdInfo(ExtraBaseModel):
    """Represents the S-NSSAI and the optionally associated Network Slice Instance(s)."""

    snssai: Snssai
    nsiIds: List[str] = Field(None, description="", min_items=1)


class DeviceType(str, Enum):
    mobilePhone = "MOBILE_PHONE"
    smartPhone = "SMART_PHONE"
    table = "TABLET"
    dongle = "DONGLE"
    modem = "MODEM"
    wlanRouter = "WLAN_ROUTER"
    iotDevice = "IOT_DEVICE"
    wearable = "WEARABLE"
    mobileTestPlatform = "MOBILE_TEST_PLATFORM"
    undefined = "UNDEFINED"


class QosRequirement(ExtraBaseModel):
    """Represents the QoS requirements."""

    fiveqi: Optional[int] = Field(None, description="", ge=0, le=255)
    gfbrUl: BitRate = None
    gfbrDl: BitRate = None
    resType: Optional[QosResourceType] = Field(None, description="")
    pdb: PacketDelBudget = None
    per: PacketErrRate = None
    deviceSpeed: Optional[VelocityEstimate] = Field(default=None)
    deviceType: Optional[DeviceType] = Field(default=None)


class DnPerformanceReq(ExtraBaseModel):
    """Represents other DN performance analytics requirements."""

    dnPerfOrderCriter: Optional[DnPerfOrderingCriterion]
    order: Optional[MatchingDirection]
    reportThresholds: List[ThresholdLevel] = Field(None, description="", min_items=1)


class BwRequirement(ExtraBaseModel):
    """Represents bandwidth requirements."""

    appId: str = Field(None, description="String providing an application identifier.")
    marBwDl: BitRate = None
    marBwUl: BitRate = None
    mirBwDl: BitRate = None
    mirBwUl: BitRate = None


class AnalyticsMetadataIndication(ExtraBaseModel):
    """Contains analytics metadata information requested to be used during analytics generation."""

    dataWindow: Optional[TimeWindow]
    dataStatProps: List[DatasetStatisticalProperty] = Field(
        None, description="", min_items=1
    )
    strategy: Optional[OutputStrategy]
    aggrNwdafIds: List[str] = Field(None, description="", min_items=1)


class EventReportingRequirement(ExtraBaseModel):
    """Represents the type of reporting that the subscription requires."""

    accuracy: Optional[Accuracy] = Field(None)
    accPerSubset: List[Accuracy] = Field(None, description="", min_items=1)
    startTs: Optional[datetime] = Field(None)
    endTs: Optional[datetime] = Field(None)
    offsetPeriod: Optional[int] = Field(None)
    sampRatio: SamplingRatio = None
    maxObjectNbr: Uinteger = None
    maxSupiNbr: Uinteger = None
    timeAnaNeeded: Optional[datetime] = Field(None)
    anaMeta: List[AnalyticsMetadata] = Field(None, description="", min_items=1)
    anaMetaInd: Optional[AnalyticsMetadataIndication] = Field(None)
    histAnaTimePeriod: Optional[TimeWindow] = Field(None)

    @validator("startTs", "endTs")
    def start_end_ts_must_be_in_past(cls, v):
        if v is not None and v > datetime.now(timezone.utc):
            raise ValueError('The "startTs" and "endTs" attributes shall be a value in the past. Predictions not implemented')
        return v

    @validator("offsetPeriod")
    def offset_period_must_be_negative(cls, v):
        if v is not None and v >= 0:
            raise ValueError('The "offsetPeriod" attribute shall be a negative value. Predictions not implemented')
        return v

    @root_validator(skip_on_failure=True)
    def offset_and_startEndTs_mutually_exclusive(cls, v):
        if v.get("offsetPeriod") is not None and any(v.get(ts) is not None for ts in ["startTs", "endTs"]):
            raise ValueError('When the "offsetPeriod" attribute is included, the "startTs" and "endTs" attributes shall not be included.')
        return v

    @root_validator(skip_on_failure=True)
    def validate_endTs_startTs(cls, v):
        # If none of startTs, endTs, or offsetPeriod is provided, the target period starts
        # at the present time with no specified end.
        start, end, offsetPeriod = v.get("startTs"), v.get("endTs"), v.get("offsetPeriod")
        if offsetPeriod:
            return v
        if end is not None:
            if start is None:
                raise ValueError('"endTs" shall not be provided without "startTs". Predictions not implemented')
            elif end < start:
                raise ValueError('"endTs" shall not be less than "startTs".')
        else:
            if start is None:
                now = datetime.now(timezone.utc)
                v["startTs"] = now
        return v

class NetworkPerfOrderCriterion(str, Enum):
    """Represents the ordering criterion for the list of network performance analytics"""

    numberOfUes = "NUMBER_OF_UES"
    communicationPerf = "COMMUNICATION_PERF"
    mobilityPerf = "MOBILITY_PERF"

class TrafficDirection(str, Enum):
    """Represents the traffic direction for the resource usage information."""

    ulAndDl = "UL_AND_DL"
    ul = "UL"
    dl = "DL"

class ValueExpression(ExtraBaseModel):
    """Represents the average or peak value of the resource usage for the network performance type."""

    average = "AVERAGE"
    peak = "PEAK"

class ResourceUsageRequirement(ExtraBaseModel):
    """resource usage requirement."""

    tfcDirc: Optional[TrafficDirection] = Field(None, description="")
    valExp: Optional[ValueExpression] = Field(None, description="")


class NetworkPerfRequirement(ExtraBaseModel):
    """Represents a network performance requirement."""

    nwPerfType: NetworkPerfType
    relativeRatio: Optional[int] = Field(None, description="Expressed in percent.", ge=1, le=100)
    absoluteNum: Optional[int] = Field(None, description="", ge=0)
    orderCriterion: Optional[NetworkPerfOrderCriterion] = Field(None, description="")
    rscUsgReq: Optional[ResourceUsageRequirement] = Field(None, description="")


class E2eDataVolTransTimeCriterion(str, Enum):
    """Represents the ordering criterion for the list of E2E data volume transfer time."""

    e2eDataVolTransTime = "E2E_DATA_VOL_TRANS_TIME"


class DataVolume(ExtraBaseModel):
    """Data Volume including UL/DL."""

    uplinkVolume: Optional[int] = Field(None, description="", ge=0)
    downlinkVolume: Optional[int] = Field(None, description="", ge=0)


class E2eDataVolTransTimeReq(ExtraBaseModel):
    """Represents other E2E data volume transfer time analytics requirements."""

    criterion: Optional[E2eDataVolTransTimeCriterion] = Field(None, description="")
    order: Optional[MatchingDirection] = Field(None, description="")
    highTransTmThr: Uinteger = None
    lowTransTmThr: Uinteger = None
    repeatDataTrans: Uinteger = None
    tsIntervalDataTrans: DurationSec = None
    dataVolume: Optional[DataVolume] = Field(None, description="")
    maxNumberUes: Uinteger = None

class WlanOrderingCriterion(str, Enum):
    """Represents the order criterion for the list of WLAN performance information."""

    timeSlotStart="TIME_SLOT_START"
    numberOfUes="NUMBER_OF_UES"
    rssi="RSSI"
    rtt="RTT"
    trafficInfo="TRAFFIC_INFO"

class WlanPerformanceReq(ExtraBaseModel):
    """Represents other WLAN performance analytics requirements."""

    ssIds: List[StrictStr] = Field(default=None, min_items=1)
    bssIds: List[StrictStr] = Field(default=None, min_items=1)
    wlanOrderCriter: Optional[WlanOrderingCriterion] = Field(default=None)
    order: Optional[MatchingDirection] = None

    @validator("order")
    def order_cannot_be_crossed(cls, v):
        if v == MatchingDirection.crossed:
            raise ValueError('"CROSSED" value in data type "MatchingDirection" is not applicable for the "order" attribute.')
        return v

    @root_validator(skip_on_failure=True)
    def order_may_be_present_if_wlanOrderCriter_is_included(cls, v):
        if v.get("order") is not None and v.get("wlanOrderCriter") is None:
            raise ValueError('The "order" attribute may be present when the "wlanOrderCriter" attribute is included.')
        return v

class UeCommOrderCriterion(str, Enum):
    """
    Represents the ordering criterion for the list of UE communication analytics.
    """
    startTime = "START_TIME"
    duration = "DURATION"

class UeCommReq(ExtraBaseModel):
    """UE communication analytics requirement."""
    orderCriterion: Optional[UeCommOrderCriterion] = Field(default=None)
    orderDirection: Optional[MatchingDirection] = Field(default=None)

    @validator("orderDirection")
    def orderDirection_cannot_be_crossed(cls, v):
        if v == MatchingDirection.crossed:
            raise ValueError('"CROSSED" value in data type "MatchingDirection" is not applicable for the "orderDirection" attribute.')
        return v

    @root_validator(skip_on_failure=True)
    def orderDirection_may_be_present_if_orderCriterion_is_included(cls, v):
        if v.get("orderDirection") is not None and v.get("orderCriterion") is None:
            raise ValueError('The "orderDirection" attribute may be present when the "orderCriterion" attribute is included.')
        return v

class UserDataConOrderCrit(str, Enum):
    """Represents the cause for requesting to terminate an analytics subscription."""

    applicableTimeWindow = "APPLICABLE_TIME_WINDOW"
    networkStatusIndication = "NETWORK_STATUS_INDICATION"

class LocInfoGranularity(str, Enum):
    """Represents the preferred granularity of location information."""

    taLevel = "TA_LEVEL"
    cellLevel = "CELL_LEVEL"
    lonAndLatLevel = "LON_AND_LAT_LEVEL"

class LocationOrientation(str, Enum):
    horizontal = "HORIZONTAL"
    vertical = "VERTICAL"
    horAndVer = "HOR_AND_VER"

class UeMobilityOrderCriterion(str, Enum):
    """Represents the ordering criterion for the list of UE mobility analytics."""

    timeSlot = "TIME_SLOT"

class UeMobilityReq(ExtraBaseModel):
    """UE mobility analytics requirement."""

    orderCriterion: Optional[UeMobilityOrderCriterion] = Field(None)
    orderDirection: Optional[MatchingDirection] = Field(None)
    ueLocOrderInd: Optional[StrictBool] = Field(None)
    distThresholds: Optional[List[conint(ge=0)]] = Field(None, min_items=1)

class ProximityCriterion(str, Enum):
    velocity = "VELOCITY"
    avgSpeed = "AVG_SPEED"
    orientation = "ORIENTATION"
    trajectory = "TRAJECTORY"

class RelProxReq(ExtraBaseModel):
    """Represents the Relative Proximity analytics requirements."""

    direction: Optional[List[Direction]] = Field(None, min_items=1)
    num_of_ue: Uinteger = None
    proximity_crits: Optional[List[ProximityCriterion]] = Field(None, min_items=1)

class MovBehavReq(ExtraBaseModel):
    """Represents the Movement Behaviour analytics requirements."""

    locationGranReq: Optional[LocInfoGranularity] = Field(None)
    reportThresholds: Optional[ThresholdLevel] = Field(None)

class AccuracyReq(ExtraBaseModel):
    """Represents the analytics accuracy requirement information."""

    accuTimeWin: Optional[TimeWindow] = Field(default=None)
    accuPeriod: Optional[StrictInt] = Field(default=None)
    accuDevThr: Uinteger = None
    minNum: Uinteger = None
    updatedAnaFlg: Optional[StrictBool] = Field(default=None)
    correctionInterval: Optional[StrictInt] = Field(default=None)


###############################################

############### TS 29.520 Nnwdaf_AnalyticsInfo Types ###


class UserDataCongestReq(ExtraBaseModel):
    """Represents a user data congesion requirement."""

    orderCriterion: Optional[UserDataConOrderCrit] = Field(None)
    orderDirection: Optional[MatchingDirection] = Field(None)


###############################################

############### TS 29.520 Nnwdaf_EventsSubscription Types ###############


class GeoDistributionInfo1(ExtraBaseModel):
    loc: UserLocation
    supis: List[Supi] = Field(..., min_items=1)
    gpsis: Optional[List[Gpsi]] = Field(None, min_items=1)


class GeoDistributionInfo2(ExtraBaseModel):
    loc: UserLocation
    supis: Optional[List[Supi]] = Field(None, min_items=1)
    gpsis: List[Gpsi] = Field(..., min_items=1)


class GeoDistributionInfo(ExtraBaseModel):
    __root__: Union[GeoDistributionInfo1, GeoDistributionInfo2] = Field(
        ..., description="Represents the geographical distribution of the UEs."
    )


class DirectionInfo(ExtraBaseModel):
    """Represents the UE direction information."""

    supi: Optional[Supi] = Field(None)
    gpsi: Optional[Gpsi] = Field(None)
    numOfUe: Uinteger = None
    avrSpeed: Optional[float] = Field(None)
    ratio: SamplingRatio = None
    direction: Direction


class DataVolumeTransferTime(ExtraBaseModel):
    """Indicates the E2E data volume transfer time and the data volume used to derive the transfer time."""

    uplinkVolume: Volume = None
    avgTransTimeUl: Uinteger = None
    varTransTimeUl: Optional[float] = Field(None)
    downlinkVolume: Volume = None
    avgTransTimeDl: Uinteger = None
    varTransTimeDl: Optional[float] = Field(None)


class E2eDataVolTransTimePerUe(ExtraBaseModel):
    """Represents the E2E data volume transfer time per UE."""
    supi: Optional[Supi] = Field(None)
    gpsi: Optional[Gpsi] = Field(None)
    snssai: Snssai = None
    accessType: Optional[AccessType] = Field(None)
    ratTypes: Optional[List[RatType]] = Field(None, description="The RAT types.", min_items=1)
    appId: Optional[str] = Field(None, description="String providing an application identifier.")
    ueLoc: Optional[UserLocation] = Field(None)
    dnn: Optional[str] = Field(
        "province1.mnc01.mcc202.gprs",
        description="String identifying the Data Network Name (i.e., Access Point Name in 4G). For more information check clause 9A of 3GPP TS 23.003",
    )
    spatialValidity: Optional[NetworkAreaInfo] = Field(None)
    validityPeriod: Optional[TimeWindow] = Field(None)
    dataVolTransTime: Optional[DataVolumeTransferTime] = Field(None)


class E2eDataVolTransTimePerTS(ExtraBaseModel):
    """Represents the E2E data volume transfer time analytics per Time Slot."""

    tsStart: datetime = Field(description="string with format 'date-time' as defined in OpenAPI.")
    tsDuration: DurationSec
    e2eDataVolTransTimePerUe: List[E2eDataVolTransTimePerUe] = Field(description="", min_items=1)


class E2eDataVolTransTimeUeList(ExtraBaseModel):
    """Contains the list of UEs classified based on experience level of E2E Data Volume Transfer Time"""
    highLevel: Optional[List[Supi]] = Field(None, min_items=1)
    mediumLevel: Optional[List[Supi]] = Field(None, min_items=1)
    lowLevel: Optional[List[Supi]] = Field(None, min_items=1)
    lowRatio: SamplingRatio = None
    mediumRatio: SamplingRatio = None
    highRatio: SamplingRatio = None
    spatialValidity: Optional[NetworkAreaInfo] = Field(None)
    validityPeriod: Optional[TimeWindow] = Field(None)


class E2eDataVolTransTimeInfo(ExtraBaseModel):
    """Represents the E2E data volume transfer time analytics information when subscribed event is \"E2E_DATA_VOL_TRANS_TIME\" """

    e2eDataVolTransTimes: List[E2eDataVolTransTimePerTS] = Field(min_items=1)
    e2eDataVolTransTimeUeLists: Optional[List[E2eDataVolTransTimeUeList]] = Field(None, description="", min_items=1)
    geoDistrInfos: Optional[List[GeoDistributionInfo]] = Field(None, description="", min_items=1)
    confidence: Uinteger = None

class SpeedThresholdInfo(ExtraBaseModel):
    """UEs information whose speed is faster than the speed threshold."""

    speedThr: Optional[float] = Field(None)
    numOfUe: Uinteger = None
    ratio: SamplingRatio = None

class MovBehav(ExtraBaseModel):
    """Represents the Movement Behaviour information per time slot."""

    tsStart: datetime
    tsDuration: DurationSec
    numOfUe: Uinteger = None
    ratio: SamplingRatio = None
    avrSpeed: Optional[float] = Field(None)
    speedThresdInfos: Optional[List[SpeedThresholdInfo]] = Field(default=None, min_items=1)
    directionUeInfos: Optional[List[DirectionInfo]] = Field(default=None, min_items=1)

class MovBehavInfo(ExtraBaseModel):
    """Represents the Movement Behaviour information."""

    geoLoc: Optional[GeographicalCoordinates] = Field(None)
    movBehavs: Optional[List[MovBehav]] = Field(None, min_items=1)
    confidence: Uinteger = None


class WlanPerTsPerformanceInfo(ExtraBaseModel):
    """WLAN performance information per Time Slot during the analytics target period."""

    tsStart: datetime
    tsDuration: DurationSec
    rssi: Optional[int] = Field(default=None)
    rtt: Uinteger = None
    trafficInfo: Optional[TrafficInformation] = Field(default=None)
    numberOfUes: Uinteger = None
    confidence: Uinteger = None


class WlanPerSsIdPerformanceInfo(ExtraBaseModel):
    """The WLAN performance per SSID."""
    ssId: str
    wlanPerTsInfos: List[WlanPerTsPerformanceInfo] = Field(min_items=1)


class WlanPerUeIdPerformanceInfo(ExtraBaseModel):
    """The WLAN performance per UE ID."""

    supi: Optional[Supi] = Field(default=None)
    gpsi: Optional[Gpsi] = Field(default=None)
    wlanPerTsInfos: List[WlanPerTsPerformanceInfo] = Field(min_items=1)

    @root_validator(skip_on_failure=True)
    def exactly_one_identifier_present(cls, v):
        if (v.get("supi") is None) == (v.get("gpsi") is None):
            raise ValueError('Exactly one of the "supi" and "gpsi" attributes shall be provided.')
        return v

    @root_validator(skip_on_failure=True)
    def numberOfUes_not_applicable(cls, v):
        wlan_per_ts_infos = v.get("wlanPerTsInfos", [])
        for info in wlan_per_ts_infos:
            if info.numberOfUes is not None:
                raise ValueError('The "numberOfUes" attribute is not applicable for the WlanPerUeIdPerformanceInfo data type')
        return v

class AnalyticsAccuracyIndication(str, Enum):
    """Represents the notification methods for the subscribed events."""

    MEET = "MEET"
    NOT_MEET = "NOT_MEET"

class AccuracyInfo(ExtraBaseModel):
    """The analytics accuracy information."""

    accuracyVal: Uinteger
    accuSampleNbr: Uinteger = None
    anaAccuInd: Optional[AnalyticsAccuracyIndication] = Field(None)


class TimestampedLocation(ExtraBaseModel):
    """The timestamped locations of the trajectory of the UE."""

    ts: datetime
    locInfo: List[LocationInfo]

class UeTrajectory(ExtraBaseModel):
    """Represents timestamped UE positions."""

    supi: Optional[Supi] = Field(None)
    gpsi: Optional[Gpsi] = Field(None)
    timestampedLocs: List[TimestampedLocation] = Field(min_items=1)

class UeProximity(ExtraBaseModel):
    """Represents the Observed or Predicted proximity information."""

    ueDistance: Optional[StrictInt] = Field(None)
    ueVelocity: Optional[VelocityEstimate] = Field(None)
    avrSpeed: Optional[float] = Field(None)
    locOrientation: Optional[LocationOrientation] = Field(None)
    ueTrajectories: Optional[List[UeTrajectory]] = Field(None, min_items=1)
    ratio: SamplingRatio = None

class TimeToCollisionInfo(ExtraBaseModel):
    """Represents Time To Collision (TTC) information."""
    ttc: Optional[datetime] = Field(None)
    accuracy: Uinteger = None
    confidence: Uinteger = None

class RelProxInfo(ExtraBaseModel):
    """Represents the Relative Proximity information."""

    tsStart: datetime
    tsDuration: DurationSec
    supis: Optional[List[Supi]] = Field(None, min_items=1)
    gpsis: Optional[List[Gpsi]] = Field(None, min_items=1)
    ueProximities: List[UeProximity] = Field(min_items=1)
    ttcInfo: Optional[TimeToCollisionInfo] = Field(None, alias="")


class NwdafEvent(str, Enum):
    """Describes the NWDAF Events."""

    SLICE_LOAD_LEVEL = "SLICE_LOAD_LEVEL"
    NETWORK_PERFORMANCE = "NETWORK_PERFORMANCE"
    NF_LOAD = "NF_LOAD"
    SERVICE_EXPERIENCE = "SERVICE_EXPERIENCE"
    UE_MOBILITY = "UE_MOBILITY"
    UE_COMMUNICATION = "UE_COMMUNICATION"
    QOS_SUSTAINABILITY = "QOS_SUSTAINABILITY"
    ABNORMAL_BEHAVIOUR = "ABNORMAL_BEHAVIOUR"
    USER_DATA_CONGESTION = "USER_DATA_CONGESTION"
    NSI_LOAD_LEVEL = "NSI_LOAD_LEVEL"
    DN_PERFORMANCE = "DN_PERFORMANCE"
    DISPERSION = "DISPERSION"
    RED_TRANS_EXP = "RED_TRANS_EXP"
    WLAN_PERFORMANCE = "WLAN_PERFORMANCE"
    SM_CONGESTION = "SM_CONGESTION"
    PFD_DETERMINATION = "PFD_DETERMINATION"
    PDU_SESSION_TRAFFIC = "PDU_SESSION_TRAFFIC"
    E2E_DATA_VOL_TRANS_TIME = "E2E_DATA_VOL_TRANS_TIME"
    MOVEMENT_BEHAVIOUR = "MOVEMENT_BEHAVIOUR"
    LOC_ACCURACY = "LOC_ACCURACY"
    RELATIVE_PROXIMITY = "RELATIVE_PROXIMITY"

class AnalyticsFeedbackInfo(ExtraBaseModel):
    """Analytics feedback information."""

    actionTimes: List[datetime] = Field(None, min_items=1)
    usedAnaTypes: Optional[List[NwdafEvent]] = Field(None, min_items=1)
    impactInd: Optional[StrictBool] = Field(None)


class ResourceUsage(ExtraBaseModel):
    """The current usage of the virtual resources assigned to the NF instances belonging to a particular network slice instance. """

    cpuUsage: Uinteger = None
    memoryUsage: Uinteger = None
    storageUsage: Uinteger = None

class NumberAverage(ExtraBaseModel):
    """Represents average and variance information."""

    number: float
    variance: float
    skewness: Optional[float] = Field(default=None)


class NsiLoadLevelInfo(ExtraBaseModel):
    """Represents the network slice and optionally the associated network slice instance and the load level information. """

    loadLevelInformation: StrictInt = Field(description="Load level information of the network slice and the optionally associated network slice instance. ")
    snssai: Snssai
    nsiId: Optional[StrictStr] = Field(None, description="Contains the Identifier of the selected Network Slice instance")
    resUsage: Optional[ResourceUsage] = Field(None)
    numOfExceedLoadLevelThr: Uinteger = None
    exceedLoadLevelThrInd: Optional[StrictBool] = Field(None, description="Indicates whether the Load Level Threshold is met or exceeded by the statistics value. Set to \"true\" if the Load Level Threshold is met or exceeded, otherwise set to \"false\". Shall be present if one of the element in the \"listOfAnaSubsets\" attribute was set to EXCEED_LOAD_LEVEL_THR_IND. ")
    networkArea: Optional[NetworkAreaInfo] = Field(None)
    timePeriod: Optional[TimeWindow] = Field(None)
    resUsgThrCrossTimePeriod: Optional[List[TimeWindow]] = Field(None, description="Each element indicates the time elapsed between times each threshold is met or exceeded or crossed. The start time and end time are the exact time stamps of the resource usage threshold is reached or exceeded. May be present if the \"listOfAnaSubsets\" attribute is provided and the maximum number of instances shall not exceed the value provided in the \"numOfExceedLoadLevelThr\" attribute. ", min_items=1)
    numOfUes: Optional[NumberAverage] = Field(None)
    numOfPduSess: Optional[NumberAverage] = Field(None)
    confidence: Uinteger = None


class TermCause(str, Enum):
    """Represents the cause for the analytics subscription termination request."""

    USER_CONSENT_REVOKED = "USER_CONSENT_REVOKED"
    NWDAF_OVERLOAD = "NWDAF_OVERLOAD"
    UE_LEFT_AREA = "UE_LEFT_AREA"


###############################################

############### TS 29.522 AMPolicyAuthorization Types ###############


class GeographicalArea(ExtraBaseModel):
    civicAddress: Optional[CivicAddress] = None
    shapes: Optional[GeographicArea] = None


###############################################

############### TS 29.522 AnalyticsExposure Types ###############


class AnalyticsFailureCode(str, Enum):
    unavailableData = "UNAVAILABLE_DATA"
    bothStatPredNotAllowed = "BOTH_STAT_PRED_NOT_ALLOWED"
    unsatifiedRequestAnalyticsTime = "UNSATISFIED_REQUESTED_ANALYTICS_TIME"
    noRoamingSupport = "NO_ROAMING_SUPPORT"
    other = "OTHER"


class AnalyticsEvent(str, Enum):
    ueMobility = "UE_MOBILITY"
    ueComm = "UE_COMM"
    abnormalBehavior = "ABNORMAL_BEHAVIOR"
    congestion = "CONGESTION"
    networkPerformance = "NETWORK_PERFORMANCE"
    qosSustainability = "QOS_SUSTAINABILITY"
    dispersion = "DISPERSION"
    dnPerformance = "DN_PERFORMANCE"
    serviceExperience = "SERVICE_EXPERIENCE"
    e2eDataVolTransTime = "E2E_DATA_VOL_TRANS_TIME"
    movementBehavior = "MOVEMENT_BEHAVIOUR"
    relativeProximity = "RELATIVE_PROXIMITY"
    wlanPerformance = "WLAN_PERFORMANCE"
    nsLoadLevel = "NS_LOAD_LEVEL"


class AnalyticsFailureEventInfo(ExtraBaseModel):
    """Represents an event for which the subscription request was not successful
    and including the associated failure reason."""

    event: AnalyticsEvent
    failureCode: AnalyticsFailureCode


class QosSustainabilityExposure(ExtraBaseModel):
    """Represents a QoS sustainability information."""

    locArea: LocationArea5G
    fineAreaInfos: Optional[List[GeographicalArea]] = Field(None, min_items=1)
    startTs: datetime
    endTs: datetime
    qosFlowRetThd: Optional[RetainabilityThreshold]
    ranUeThrouThd: BitRate = None
    snssai: Snssai = None
    confidence: int = Field(None, description="", ge=0)


class CongestionAnalytics(ExtraBaseModel):
    """Represents data congestion analytics for transfer over the user plane,
    control plane or both."""

    cngType: CongestionType
    tmWdw: TimeWindow
    nsi: ThresholdLevel
    confidence: int = Field(None, description="", ge=0)
    topAppListUl: List[TopApplication] = Field(None, description="", min_items=1)
    topAppListDl: List[TopApplication] = Field(None, description="", min_items=1)


class CongestInfo(ExtraBaseModel):
    """Represents a UE's user data congestion information."""

    locArea: LocationArea5G
    cngAnas: List[CongestionAnalytics] = Field(None, description="", min_items=1)


class AbnormalExposure(ExtraBaseModel):
    """Represents a user's abnormal behavior information."""

    # TODO: test this list
    gpsis: List[constr(regex=r"^(msisdn-[0-9]{5,15}|extid-.+@.+|.+)$")] = Field(
        None, description="", min_items=1
    )
    appId: str = Field(None, description="String providing an application identifier.")
    dnn: Optional[str] = Field(
        "province1.mnc01.mcc202.gprs",
        description="String identifying the Data Network Name (i.e., Access Point Name in 4G). For more information check clause 9A of 3GPP TS 23.003",
    )
    snssai: Snssai = None
    excep: List[Exception]
    ratio: int = Field(None, description="Expressed in percent", ge=1, le=100)
    confidence: int = Field(None, description="", ge=0)
    addtMeasInfo: Optional[AdditionalMeasurement]


class NetworkPerfExposure(ExtraBaseModel):
    """Represents network performance information."""

    locArea: LocationArea5G
    anaPeriod: Optional[TimeWindow] = Field(None)
    nwPerfType: NetworkPerfType
    relativeRatio: int = Field(None, description="Expressed in percent", ge=1, le=100)
    absoluteNum: int = Field(None, description="", ge=0)
    rscUsgReq: Optional[ResourceUsageRequirement] = Field(None)
    confidence: int = Field(None, description="", ge=0)


class UeLocationInfo(ExtraBaseModel):
    loc: LocationArea5G
    geoLoc: Optional[GeographicalArea] = None
    ratio: int = Field(None, description="Expressed in percent", ge=1, le=100)
    confidence: int = Field(None, description="", ge=0)
    geoDistrInfos: Optional[List[GeoDistributionInfo]] = Field(None, min_items=1)


class UeMobilityExposure(ExtraBaseModel):
    """Represents a UE mobility information."""

    ts: Optional[datetime]
    recurringTime: Optional[ScheduledCommunicationTime]
    duration: DurationSec
    durationVariance: Optional[float]
    locInfo: List[UeLocationInfo] = Field(description="", min_items=1)
    directionInfos: List[DirectionInfo] = Field(None, description="", min_items=1)


class WlanPerformInfo(ExtraBaseModel):
    """The WLAN performance related information."""

    locArea: Optional[LocationArea5G] = Field(default=None)
    wlanPerSsidInfos: List[WlanPerSsIdPerformanceInfo] = Field(min_items=1)
    wlanPerUeIdInfos: Optional[List[WlanPerUeIdPerformanceInfo]] = Field(default=None, min_items=1)


class AnalyticsData(ExtraBaseModel):
    """Represents analytics data."""

    start: Optional[datetime]
    expiry: Optional[datetime]
    timeStampGen: Optional[datetime]
    ueMobilityInfos: List[UeMobilityExposure] = Field(None, description="", min_items=1)
    ueCommInfos: List[UeCommunication] = Field(None, description="", min_items=1)
    nwPerfInfos: List[NetworkPerfExposure] = Field(None, description="", min_items=1)
    abnormalInfos: List[AbnormalExposure] = Field(None, description="", min_items=1)
    congestInfos: List[CongestInfo] = Field(None, description="", min_items=1)
    dataVlTrnsTmInfos: List[E2eDataVolTransTimeInfo] = Field(None, description="", min_items=1)
    qosSustainInfos: List[QosSustainabilityExposure] = Field(
        None, description="", min_items=1
    )
    disperInfos: List[DispersionInfo] = Field(None, description="", min_items=1)
    dnPerfInfos: List[DnPerfInfo] = Field(None, description="", min_items=1)
    svcExps: List[ServiceExperienceInfo] = Field(None, description="", min_items=1)
    movBehavInfos: Optional[List[MovBehavInfo]] = Field(default=None, min_items=1)
    wlanInfos: Optional[List[WlanPerformInfo]] = Field(default=None, min_items=1)
    accuInfo: Optional[AccuracyInfo] = Field(default=None)
    cancelAccuInd: Optional[StrictBool] = Field(default=None, description="Indicates cancelled request of the analytics accuracy information. Set to \"true\" indicates the NWDAF cancelled request of analytics accuracy information as the NWDAF does not support the accuracy checking capability. Otherwise set to \"false\". Default value is \"false\" if omitted. ")
    relProxInfos: Optional[List[RelProxInfo]] = Field(default=None, min_items=1)
    suppFeat: constr(regex=r"^[A-Fa-f0-9]*$")


class TargetUeId(ExtraBaseModel):
    """Represents the target UE(s) information."""

    anyUeInd: Optional[bool] = False
    gpsi: Optional[constr(regex=r"^(msisdn-[0-9]{5,15}|extid-[^@]+@[^@]+|.+)$")] = Field(None)
    exterGroupId: Optional[str] = Field(None, description="")

    @root_validator(skip_on_failure=True)
    def only_one_identifier_present(cls, v):
        if sum(bool(v.get(k)) for k in ["anyUeInd", "gpsi", "exterGroupId"]) > 1:
            raise ValueError('Only one of "anyUeInd", "gpsi" or "exterGroupId" attribute may be present.')
        return v


class AnalyticsEventFilter(ExtraBaseModel):
    """Represents analytics event filter information."""

    locArea: Optional[LocationArea5G]
    fineGranAreas: Optional[List[GeographicalArea]] = Field(None, min_items=1)
    temporalGranSize: DurationSec = None
    spatialGranSizeTa: Uinteger = None
    spatialGranSizeCell: Uinteger = None
    dnn: Optional[str] = Field(
        "province1.mnc01.mcc202.gprs",
        description="String identifying the Data Network Name (i.e., Access Point Name in 4G). For more information check clause 9A of 3GPP TS 23.003",
    )
    dnns: Optional[List[StrictStr]] = Field(None, min_items=1)
    dnais: List[str] = Field(
        None,
        description="DNAI (Data network access identifier), see clause 5.6.7 of 3GPP TS 23.501.",
        min_items=1,
    )
    nwPerfTypes: List[NetworkPerfType] = Field(None, description="", min_items=1)
    appIds: List[str] = Field(
        None, description="String providing an application identifier.", min_items=1
    )
    excepIds: List[ExceptionId] = Field(None, description="", min_items=1)
    exptAnaType: Optional[ExpectedAnalyticsType]
    exptUeBehav: Optional[ExpectedUeBehaviourData]
    snssai: Snssai = None
    snssais: Optional[List[Snssai]] = Field(None, min_items=1)
    nsiIdInfos: List[NsiIdInfo] = Field(None, description="", min_items=1)
    qosReq: Optional[QosRequirement]
    listOfAnaSubsets: List[AnalyticsSubset] = Field(None, description="", min_items=1)
    dnPerfReqs: List[DnPerformanceReq] = Field(None, description="", min_items=1)
    dataVlTrnsTmReqs: Optional[List[E2eDataVolTransTimeReq]] = Field(None, min_items=1)
    bwRequs: List[BwRequirement] = Field(None, description="", min_items=1)
    ratFreqs: List[RatFreqInformation] = Field(None, description="", min_items=1)
    appServerAddrs: List[AddrFqdn] = Field(None, description="", min_items=1)
    wlanReqs: Optional[List[WlanPerformanceReq]] = Field(None, min_items=1)
    disperReqs: Optional[List[DispersionRequirement]] = Field(None, min_items=1)
    maxNumOfTopAppUl: int = Field(None, description="", ge=0)
    maxNumOfTopAppDl: int = Field(None, description="", ge=0)
    visitedLocAreas: Optional[List[LocationArea5G]] = Field(None, min_items=1)
    pduSesInfos: Optional[List[PduSessionInfo]] = Field(None, min_items=1)
    ueCommReqs: Optional[List[UeCommReq]] = Field(None, min_items=1)
    userDataConReq: Optional[UserDataCongestReq] = Field(None)
    locGranularity: Optional[LocInfoGranularity] = Field(None)
    locOrientation: Optional[LocationOrientation] = Field(None)
    ueMobilityReqs: Optional[List[UeMobilityReq]] = Field(None, min_items=1)
    movBehavReqs: Optional[List[MovBehavReq]] = Field(None, min_items=1)
    useCaseCxt: Optional[StrictStr] = Field(None, description="Indicates the context of usage of the analytics. The value and format of this parameter are not standardized. ")
    accuReq: Optional[AccuracyReq] = Field(None)
    relProxReqs: Optional[List[RelProxReq]] = Field(None, min_items=1)


class AnalyticsEventFilterSubsc(ExtraBaseModel):
    """Represents an analytics event filter."""

    nwPerfReqs: List[NetworkPerfRequirement] = Field(None, description="", min_items=1)
    locArea: Optional[LocationArea5G] = Field(None, description="")
    fineGranAreas: Optional[List[GeographicalArea]] = Field(None, min_items=1)
    temporalGranSize: DurationSec = None
    spatialGranSizeTa: Uinteger = None
    spatialGranSizeCell: Uinteger = None
    appIds: List[str] = Field(
        None, description="String providing an application identifier.", min_items=1
    )
    dnn: Optional[str] = Field(
        "province1.mnc01.mcc202.gprs",
        description="String identifying the Data Network Name (i.e., Access Point Name in 4G). For more information check clause 9A of 3GPP TS 23.003",
    )
    dnns: Optional[List[str]] = Field(
        None,
        description="Identifies the DNN(s) to which the subscriptions applies.",
        min_items=1,
    )
    dnais: List[str] = Field(
        None,
        description="DNAI (Data network access identifier), see clause 5.6.7 of 3GPP TS 23.501.",
        min_items=1,
    )
    dataVlTrnsTmRqs: List[E2eDataVolTransTimeReq] = Field(None, description="", min_items=1)
    excepRequs: List[Exception] = Field(None, description="", min_items=1)
    exptAnaType: Optional[ExpectedAnalyticsType]
    exptUeBehav: Optional[ExpectedUeBehaviourData]
    matchingDir: Optional[MatchingDirection]
    reptThlds: List[ThresholdLevel] = Field(None, description="", min_items=1)
    snssai: Snssai = None
    snssais: Optional[List[Snssai]] = Field(None, description="", min_items=1)
    nsiIdInfos: List[NsiIdInfo] = Field(None, description="", min_items=1)
    qosReq: Optional[QosRequirement] = Field(None, description="")
    qosFlowRetThds: List[RetainabilityThreshold] = Field(
        None, description="", min_items=1
    )
    ranUeThrouThds: List[constr(regex="^\d+(\.\d+)? (bps|Kbps|Mbps|Gbps|Tbps)$")] = (
        Field(None, description="", min_items=1)
    )
    disperReqs: List[DispersionRequirement] = Field(None, description="", min_items=1)
    listOfAnaSubsets: List[AnalyticsSubset] = Field(None, description="", min_items=1)
    dnPerfReqs: List[DnPerformanceReq] = Field(None, description="", min_items=1)
    bwRequs: List[BwRequirement] = Field(None, description="", min_items=1)
    ratFreqs: List[RatFreqInformation] = Field(None, description="", min_items=1)
    appServerAddrs: List[AddrFqdn] = Field(None, description="", min_items=1)
    wlanReqs: List[WlanPerformanceReq] = Field(default=None, min_items=1)
    extraReportReq: Optional[EventReportingRequirement] = Field(None, description="")
    maxNumOfTopAppUl: Uinteger = None
    maxNumOfTopAppDl: Uinteger = None
    visitedLocAreas: List[LocationArea5G] = Field(None, description="", min_items=1)
    pduSesInfos: Optional[List[PduSessionInfo]] = Field(None, min_items=1)
    ueCommReqs: Optional[List[UeCommReq]] = Field(None, min_items=1)
    userDataConOrderCri: Optional[UserDataConOrderCrit] = Field(None)
    locGranularity: Optional[LocInfoGranularity] = Field(None)
    locOrientation: Optional[LocationOrientation] = Field(None)
    ueMobilityReqs: Optional[List[UeMobilityReq]] = Field(None, min_items=1)
    movBehavReqs: Optional[List[MovBehavReq]] = Field(None, min_items=1)
    relProxReqs: Optional[List[RelProxReq]] = Field(None, min_items=1)
    useCaseCxt: Optional[StrictStr] = Field(None)
    pauseFlg: Optional[StrictBool] = Field(None)
    resumeFlg: Optional[StrictBool] = Field(None)
    accuReq: Optional[AccuracyReq] = Field(None)
    feedback: Optional[AnalyticsFeedbackInfo] = Field(None)

class AnalyticsEventSubsc(ExtraBaseModel):
    """Represents a subscribed analytics event."""

    analyEvent: AnalyticsEvent
    analyEventFilter: Optional[AnalyticsEventFilterSubsc]
    tgtUe: Optional[TargetUeId]


class AnalyticsEventNotif(ExtraBaseModel):
    """Represents an analytics event to be reported."""

    analyEvent: AnalyticsEvent
    expiry: Optional[datetime] = Field(None)
    timeStamp: datetime
    failNotifyCode: Optional[NwdafFailureCode] = Field(None)
    rvWaitTime: DurationSec = None
    ueMobilityInfos: List[UeMobilityExposure] = Field(None, description="", min_items=1)
    ueCommInfos: List[UeCommunication] = Field(None, description="", min_items=1)
    abnormalInfos: List[AbnormalExposure] = Field(None, description="", min_items=1)
    congestInfos: List[CongestInfo] = Field(None, description="", min_items=1)
    dataVlTrnsTmIfs: Optional[List[E2eDataVolTransTimeInfo]] = Field(None, description="", min_items=1)
    nwPerfInfos: List[NetworkPerfExposure] = Field(None, description="", min_items=1)
    qosSustainInfos: List[QosSustainabilityExposure] = Field(
        None, description="", min_items=1
    )
    disperInfos: List[DispersionInfo] = Field(None, description="", min_items=1)
    dnPerfInfos: List[DnPerfInfo] = Field(None, description="", min_items=1)
    svcExps: List[ServiceExperienceInfo] = Field(None, description="", min_items=1)
    movBehavInfos: Optional[List[MovBehavInfo]] = Field(default=None, min_items=1)
    wlanInfos: Optional[List[WlanPerformInfo]] = Field(None, description="", min_items=1)
    relProxInfos: Optional[List[RelProxInfo]] = Field(default=None, min_items=1)
    start: Optional[datetime] = Field(None)
    timeStampGen: Optional[datetime] = Field(None)
    locArea: Optional[LocationArea5G] = Field(None)
    pauseInd: Optional[StrictBool] = Field(None)
    resumeInd: Optional[StrictBool] = Field(None)
    accuInfo: Optional[AccuracyInfo] = Field(None)
    cancelAccuInd: Optional[StrictBool] = Field(None, description="")
    nsiLoadLevelData: Optional[List[NsiLoadLevelInfo]] = Field(None, min_items=1)


class AnalyticsEventNotification(ExtraBaseModel):
    """Represents an analytics event(s) notification."""

    notifId: str
    analyEventNotifs: List[AnalyticsEventNotif] = Field(description="", min_items=1)
    termCause: Optional[TermCause] = Field(None)


class AnalyticsExposureSubsc(ExtraBaseModel):
    """Represents an analytics exposure subscription."""

    analyEventsSubs: List[AnalyticsEventSubsc] = Field(
        None, description="", min_items=1
    )
    # If omitted, the default values within the ReportingInformation data type apply.
    analyRepInfo: ReportingInformation = Field(default_factory=ReportingInformation)
    notifUri: str
    notifId: str
    eventNotifis: List[AnalyticsEventNotif] = Field(None, description="", min_items=1)
    failEventReports: List[AnalyticsFailureEventInfo] = Field(
        None, description="", min_items=1
    )
    suppFeat: SupportedFeatures = None
    self: Optional[AnyHttpUrl] = Field(
        "https://myresource.com",
        description="String identifying a referenced resource. This is also returned as a location header in 201 Created Response",
    )
    requestTestNotification: bool = Field(
        None,
        description="Set to true by the AF to request the NEF to send a test notification as defined in clause 5.2.5.3 of 3GPP TS 29.122. Set to false or omitted otherwise.",
    )
    websockNotifConfig: Optional[WebsockNotifConfig]


    class Config:
        orm_mode = True

class AnalyticsExposureSubscUpsert(AnalyticsExposureSubsc):
    """Represents an analytics exposure subscription to be created/updated."""

    @root_validator(skip_on_failure=True)
    def offset_may_be_present_if_repPeriod_present(cls, values):
        analy_rep_info = values.get("analyRepInfo")
        rep_period_absent = analy_rep_info is None or analy_rep_info.repPeriod is None

        if rep_period_absent:
            for event_subsc in values.get("analyEventsSubs", []):
                analy_filter = event_subsc.analyEventFilter
                if analy_filter is not None and analy_filter.extraReportReq is not None and \
                    analy_filter.extraReportReq.offsetPeriod is not None:
                    raise ValueError('offsetPeriod may be present if the "repPeriod" attribute is included')

        return values


class AnalyticsRequest(ExtraBaseModel):
    """Represents the parameters to request to retrieve analytics information."""

    analyEvent: AnalyticsEvent
    analyEventFilter: Optional[AnalyticsEventFilter]
    analyRep: Optional[EventReportingRequirement]
    tgtUe: Optional[TargetUeId]
    suppFeat: SupportedFeatures

    @validator("analyRep")
    def offset_period_not_allowed(cls, v):
        # As the 'offsetPeriod may be present if the "repPeriod" attribute is included'
        # And '"repPeriod" is supplied for notification Method "periodic"'
        # We assume that for this payload, i.e. immediate reporting at /fetch without periodic reporting,
        # the "offsetPeriod" is not allowed.
        if v and v.offsetPeriod is not None:
            raise ValueError('offsetPeriod is not allowed in analyRep for AnalyticsRequest')
        return v
