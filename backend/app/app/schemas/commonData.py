from enum import Enum
from typing import Optional, List, Annotated
from typing_extensions import Annotated, TypeAlias
from datetime import datetime
from ipaddress import IPv4Address, IPv6Address, IPv6Network
from pydantic import Field, AnyHttpUrl, root_validator

from .utils import ExtraBaseModel


# Defined with other classes, without making a new class type
Link: TypeAlias = AnyHttpUrl
# DateTime - datetime
# ExternalId - str -> example: 123456789@domain.com
ExternalId: TypeAlias = Annotated[
    str,
    Field(
        description='string containing a local identifier followed by "@" and a domain identifier. Both the local identifier and the domain identifier shall be encoded as strings that do not contain any "@" characters. See Clause 4.6.2 of 3GPP TS 23.682 for more information.',
    ),
]
# Msisdn - str -> example: 918369110173
Msisdn: TypeAlias = Annotated[
    str,
    Field(
        description="string formatted according to clause 3.3 of 3GPP TS 23.003 that describes an MSISDN.",
    ),
]
# DurationSec - int = Field(None, description="", ge=0)
# DayOfWeek ???
# TimeOfDay ???
# DurationSecRm ???
# DurationSecRo ???

# TODO:
# 29.571
# Dnai - str --- DNAI (Data network access identifier), see clause 5.6.7 of 3GPP TS 23.501.
# TypeAllocationCode
# ApplicationId - str = Field(None, description="String providing an application identifier.")
# Float - float
# PacketLossRate - int = Field(None, description="Expressed in tenth of percent.", ge=0, le=1000)
# GlobalRanNodeId
# Tai
SamplingRatio: TypeAlias = Annotated[
    int,
    Field(
        ge=1, le=100,
        description='Unsigned integer indicating Sampling Ratio (see clauses 4.15.1 of 3GPP TS 23.502), expressed in percent.'
    ),
]
EutraCellId: TypeAlias = Annotated[
    str,
    Field(
        regex=r"^[A-Fa-f0-9]{7}$",
        description='28-bit string identifying an E-UTRA Cell Id as specified in clause 9.3.1.9 of  3GPP TS 38.413, in hexadecimal representation. Each character in the string shall take a  value of "0" to "9", "a" to "f" or "A" to "F" and shall represent 4 bits. The most  significant character representing the 4 most significant bits of the Cell Id shall appear  first in the string, and the character representing the 4 least significant bit of the  Cell Id shall appear last in the string.',
    ),
]
# Nid - constr(regex=r'^[A-Fa-f0-9]{11}$')
NrCellId: TypeAlias = Annotated[
    str,
    Field(
        regex=r"^[A-Fa-f0-9]{9}$",
        description='36-bit string identifying an NR Cell Id as specified in clause 9.3.1.7 of 3GPP TS 38.413,  in hexadecimal representation. Each character in the string shall take a value of "0" to "9",  "a" to "f" or "A" to "F" and shall represent 4 bits. The most significant character  representing the 4 most significant bits of the Cell Id shall appear first in the string, and  the character representing the 4 least significant bit of the Cell Id shall appear last in the  string.',
    ),
]
N3IwfId: TypeAlias = Annotated[
    str,
    Field(
        regex=r"^[A-Fa-f0-9]+$",
        description='This represents the identifier of the N3IWF ID as specified in clause 9.3.1.57 of  3GPP TS 38.413 in hexadecimal representation. Each character in the string shall take a value  of "0" to "9", "a" to "f" or "A" to "F" and shall represent 4 bits. The most significant  character representing the 4 most significant bits of the N3IWF ID shall appear first in the  string, and the character representing the 4 least significant bit of the N3IWF ID shall  appear last in the string.',
    ),
]
# NgeNbId - constr(regex=r'^(MacroNGeNB-[A-Fa-f0-9]{5}|LMacroNGeNB-[A-Fa-f0-9]{6}|SMacroNGeNB-[A-Fa-f0-9]{5})$')
WAgfId: TypeAlias = Annotated[
    str,
    Field(
        regex=r"^[A-Fa-f0-9]+$",
        description='This represents the identifier of the W-AGF ID as specified in clause 9.3.1.162 of  3GPP TS 38.413 in hexadecimal representation. Each character in the string shall take a value  of "0" to "9", "a" to "f" or "A" to "F" and shall represent 4 bits. The most significant  character representing the 4 most significant bits of the W-AGF ID shall appear first in the  string, and the character representing the 4 least significant bit of the W-AGF ID shall  appear last in the string.',
    ),
]
TngfId: TypeAlias = Annotated[
    str,
    Field(
        regex=r"^[A-Fa-f0-9]+$",
        description='This represents the identifier of the TNGF ID as specified in clause 9.3.1.161 of  3GPP TS 38.413  in hexadecimal representation. Each character in the string shall take a value of "0" to "9", "a"  to "f" or "A" to "F" and shall represent 4 bits. The most significant character representing the  4 most significant bits of the TNGF ID shall appear first in the string, and the character  representing the 4 least significant bit of the TNGF ID shall appear last in the string.',
    ),
]
ENbId: TypeAlias = Annotated[
    str,
    Field(
        regex=r"^(MacroeNB-[A-Fa-f0-9]{5}|LMacroeNB-[A-Fa-f0-9]{6}|SMacroeNB-[A-Fa-f0-9]{5}|HomeeNB-[A-Fa-f0-9]{7})$",
        description='This represents the identifier of the eNB ID as specified in clause 9.2.1.37 of  3GPP TS 36.413. The string shall be formatted with the following pattern  \'^(\'MacroeNB-[A-Fa-f0-9]{5}|LMacroeNB-[A-Fa-f0-9]{6}|SMacroeNB-[A-Fa-f0-9]{5} |HomeeNB-[A-Fa-f0-9]{7})$\'. The value of the eNB ID shall be encoded in hexadecimal representation. Each character in the  string shall take a value of "0" to "9", "a" to "f" or "A" to "F" and shall represent 4 bits.  The padding 0 shall be added to make multiple nibbles, so the most significant character  representing the padding 0 if required together with the 4 most significant bits of the eNB ID  shall appear first in the string, and the character representing the 4 least significant bit  of the eNB ID (to form a nibble) shall appear last in the string.',
    ),
]
Tac = Annotated[
    str,
    Field(
        regex=r"(^[A-Fa-f0-9]{4}$)|(^[A-Fa-f0-9]{6}$)",
        description='2 or 3-octet string identifying a tracking area code as specified in clause 9.3.3.10  of 3GPP TS 38.413, in hexadecimal representation. Each character in the string shall  take a value of "0" to "9", "a" to "f" or "A" to "F" and shall represent 4 bits. The most significant character representing the 4 most significant bits of the TAC shall  appear first in the string, and the character representing the 4 least significant bit  of the TAC shall appear last in the string.',
    ),
]
NgeNbId: TypeAlias = Annotated[
    str,
    Field(
        regex=r"^(MacroNGeNB-[A-Fa-f0-9]{5}|LMacroNGeNB-[A-Fa-f0-9]{6}|SMacroNGeNB-[A-Fa-f0-9]{5})$",
        description='This represents the identifier of the ng-eNB ID as specified in clause 9.3.1.8 of  3GPP TS 38.413. The value of the ng-eNB ID shall be encoded in hexadecimal representation.  Each character in the string shall take a value of "0" to "9", "a" to "f" or "A" to "F" and  shall represent 4 bits. The padding 0 shall be added to make multiple nibbles, so the most  significant character representing the padding 0 if required together with the 4 most  significant bits of the ng-eNB ID shall appear first in the string, and the character  representing the 4 least significant bit of the ng-eNB ID (to form a nibble) shall appear last  in the string.',
        example="SMacroNGeNB-34B89",
    ),
]
# PduSessionId - int -> ge = 0, le = 255
# UserLocation
# Supi - constr(regex=r'^(imsi-[0-9]{5,15}|nai-.+|gci-.+|gli-.+|.+)$')
Bytes: TypeAlias = Annotated[
    str, Field(description="string with format 'bytes' as defined in OpenAPI")
]
HfcNId: TypeAlias = Annotated[
    str,
    Field(
        max_length=6,
        description="This IE represents the identifier of the HFC node Id as specified in CableLabs WR-TR-5WWC-ARCH. It is provisioned by the wireline operator as part of wireline operations and may contain up to six characters.",
    ),
]
Gli: TypeAlias = Bytes
Gci: TypeAlias = Annotated[
    str,
    Field(
        description="Global Cable Identifier uniquely identifying the connection between the 5G-CRG or FN-CRG to the 5GS. See clause 28.15.4 of 3GPP TS 23.003. This shall be encoded as a string per clause 28.15.4 of 3GPP TS 23.003, and compliant with the syntax specified  in clause 2.2  of IETF RFC 7542 for the username part of a NAI. The GCI value is specified in CableLabs WR-TR-5WWC-ARCH.",
    ),
]
# ArfcnValueNR - int = Field(None, description="", ge=0, le=3279165)
# RatType
# 5Qi - int = Field(None, description="", ge=0, le=255)

# TODO:
# 29.122
# Volume -- int -> ge = 0 ---  Unsigned integer identifying a volume in units of bytes.
# LocationArea
# LocationArea5G
# ConfigResult
# FlowInfo
# TimeOfDay - time

# 29.514
# FlowDescription -> str --- defines a packet filter of an IP flow

Port: TypeAlias = Annotated[
    int,
    Field(
        ge=0,
        le=65535,
        description="Unsigned integer with valid values between 0 and 65535.",
    ),
]

# TS 29.122
DurationSec: TypeAlias = Annotated[
    int,
    Field(
        ge=0,
        description="Unsigned integer identifying a period of time in units of seconds.",
    ),
]

DurationMin: TypeAlias = Annotated[
    int,
    Field(
        ge=0,
        description="Unsigned integer identifying a period of time in units of minutes.",
    ),
]


# TS 29.571
class IpAddr(ExtraBaseModel):
    ipv4Addr: Optional[IPv4Address] = None
    ipv6Addr: Optional[IPv6Address] = None
    ipv6Prefix: Optional[IPv6Network] = None

    @root_validator
    def any_of(cls, v):
        if not any(v.values()):
            raise ValueError(
                "Either ipv4Addr, or ipv6Addr, or ipv6Prefix shall be present"
            )

        return v


# TS 29.571
ExtPacketDelBudget: TypeAlias = Annotated[
    int,
    Field(
        ge=1,
        description="Unsigned integer indicating Packet Delay Budget (see clauses\xa05.7.3.4 and 5.7.4 of 3GPP TS 23.501 [8])), expressed in 0.01 milliseconds.",
    ),
]

# TS 29.571
Uint32: TypeAlias = Annotated[
    int,
    Field(
        ge=0,
        le=4294967295,
        description="Integer where the allowed values correspond to the value range of an unsigned 32-bit integer.",
    ),
]


# TS 29.571
SupportedFeatures: TypeAlias = Annotated[
    str,
    Field(
        regex=r"^[A-Fa-f0-9]*$",
        description='A string used to indicate the features supported by an API that is used as defined in clause  6.6 in 3GPP TS 29.500. The string shall contain a bitmask indicating supported features in  hexadecimal representation Each character in the string shall take a value of "0" to "9",  "a" to "f" or "A" to "F" and shall represent the support of 4 features as described in  table\xa05.2.2-3. The most significant character representing the highest-numbered features shall  appear first in the string, and the character representing features 1 to 4 shall appear last  in the string. The list of features and their numbering (starting with 1) are defined  separately for each API. If the string contains a lower number of characters than there are  defined features for an API, all features that would be represented by characters that are not  present in the string are not supported.',
    ),
]


# TS 29.571
Dnn: TypeAlias = Annotated[
    str,
    Field(
        description='String representing a Data Network as defined in clause 9A of 3GPP TS 23.003;  it shall contain either a DNN Network Identifier, or a full DNN with both the Network  Identifier and Operator Identifier, as specified in 3GPP TS 23.003 clause 9.1.1 and 9.1.2. It shall be coded as string in which the labels are separated by dots  (e.g. "Label1.Label2.Label3").',
    ),
]


# TS 29.571
class Snssai(ExtraBaseModel):
    sst: Annotated[
        int,
        Field(
            ge=0,
            le=255,
            description="Unsigned integer, within the range 0 to 255, representing the Slice/Service Type.  It indicates the expected Network Slice behaviour in terms of features and services. Values 0 to 127 correspond to the standardized SST range. Values 128 to 255 correspond  to the Operator-specific range. See clause 28.4.2 of 3GPP TS 23.003. Standardized values are defined in clause 5.15.2.2 of 3GPP TS 23.501.",
        ),
    ]
    sd: Annotated[
        Optional[str],
        Field(
            regex=r"^[A-Fa-f0-9]{6}$",
            description='3-octet string, representing the Slice Differentiator, in hexadecimal representation. Each character in the string shall take a value of "0" to "9", "a" to "f" or "A" to "F" and shall represent 4 bits. The most significant character representing the 4 most significant bits of the SD shall appear first in the string, and the character representing the 4 least significant bit of the SD shall appear last in the string. This is an optional parameter that complements the Slice/Service type(s) to allow to  differentiate amongst multiple Network Slices of the same Slice/Service type. This IE shall be absent if no SD value is associated with the SST.',
        ),
    ] = None


# TS 29.571
Uinteger: TypeAlias = Annotated[
    int,
    Field(
        ge=0,
        description="Unsigned Integer, i.e. only value 0 and integers above 0 are permissible.",
    ),
]

# TS 29.571
BitRate: TypeAlias = Annotated[
    str,
    Field(
        regex=r"^\d+(\.\d+)? (bps|Kbps|Mbps|Gbps|Tbps)$",
        description='String representing a bit rate; the prefixes follow the standard symbols from The International System of Units, and represent x1000 multipliers, with the exception that prefix "K" is used to represent the standard symbol "k".',
    ),
]


# TS 29.571
PacketDelBudget: TypeAlias = Annotated[
    int,
    Field(
        ge=1,
        description="Unsigned integer indicating Packet Delay Budget (see clauses 5.7.3.4 and 5.7.4 of 3GPP TS 23.501), expressed in milliseconds.",
    ),
]


# TS 29.571
PacketErrRate: TypeAlias = Annotated[
    str,
    Field(
        regex=r"^([0-9]E-[0-9])$",
        description='String representing Packet Error Rate (see clause 5.7.3.5 and 5.7.4 of 3GPP TS 23.501, expressed as a "scalar x 10-k" where the scalar and the exponent k are each encoded as one decimal digit.',
    ),
]


# TS 29.122
# NOTE: There is another definition in TS 29.571 that is not compatible with
# this definition.
ExternalGroupId: TypeAlias = Annotated[
    str,
    Field(
        description='string containing a local identifier followed by "@" and a domain identifier. Both the local identifier and the domain identifier shall be encoded as strings that do not contain any "@" characters. See Clauses 4.6.2 and 4.6.3 of 3GPP TS 23.682 for more information.',
    ),
]


# TS 29.571
Gpsi: TypeAlias = Annotated[
    str,
    Field(
        regex=r"^(msisdn-[0-9]{5,15}|extid-[^@]+@[^@]+|.+)$",
        description="String identifying a Gpsi shall contain either an External Id or an MSISDN.  It shall be formatted as follows -External Identifier= \"extid-'extid', where 'extid'  shall be formatted according to clause 19.7.2 of 3GPP TS 23.003 that describes an  External Identifier.",
    ),
]

# TS 29.571
Uri: TypeAlias = Annotated[
    str, Field(description="String providing an URI formatted according to RFC 3986.")
]


# TS 29.571
Nid: TypeAlias = Annotated[
    str,
    Field(
        regex=r"^[A-Fa-f0-9]{11}$",
        description="This represents the Network Identifier, which together with a PLMN ID is used to identify an SNPN (see 3GPP TS 23.003 and 3GPP TS 23.501 clause 5.30.2.1).",
    ),
]


# TS 29.514
TosTrafficClass: TypeAlias = Annotated[
    str,
    Field(
        description="2-octet string, where each octet is encoded in hexadecimal representation. The first octet contains the IPv4 Type-of-Service or the IPv6 Traffic-Class field and the second octet contains the ToS/Traffic Class mask field.",
    ),
]


# TS 29.514
MultiModalId: TypeAlias = Annotated[
    str,
    Field(description="This data type contains a multi-modal service identifier."),
]


# TS 29.514
class UplinkDownlinkSupport(str, Enum):
    UL = "UL"
    DL = "DL"
    UL_DL = "UL_DL"


# TS 29.571
MacAddr48: TypeAlias = Annotated[
    str,
    Field(
        regex=r"^([0-9a-fA-F]{2})((-[0-9a-fA-F]{2}){5})$",
        description="String identifying a MAC address formatted in the hexadecimal notation according to clause 1.1 and clause 2.1 of RFC 7042.",
    ),
]

# TS 29.571
AverWindow: TypeAlias = Annotated[
    int,
    Field(
        ge=1,
        le=4095,
        description="Unsigned integer indicating Averaging Window (see clause 5.7.3.6 and 5.7.4 of 3GPP TS 23.501), expressed in milliseconds.",
    ),
]


# TS 29.514
class AlternativeServiceRequirementsData(ExtraBaseModel):
    altQosParamSetRef: Annotated[
        str,
        Field(description="Reference to this alternative QoS related parameter set."),
    ]
    gbrUl: Optional[BitRate] = None
    gbrDl: Optional[BitRate] = None
    pdb: Optional[PacketDelBudget] = None
    per: Optional[PacketErrRate] = None

    @root_validator
    def any_of(cls, v):
        if (
            (v.get("gbrUl") is None or v.get("gbrDl") is None)
            and v.get("pdb") is None
            and v.get("per") is None
        ):
            raise ValueError(
                "At least one of gbrUl and gbrDl, pdb, or per must be given"
            )

        return v


# TS 29.122
class FlowInfo(ExtraBaseModel):
    """Represents IP flow information."""

    flowId: Annotated[int, Field(description="Indicates the IP flow identifier.")]
    flowDescriptions: Annotated[
        Optional[List[str]],
        Field(
            description="Indicates the packet filters of the IP flow. Refer to clause 5.3.8 of 3GPP TS 29.214 for encoding. It shall contain UL and/or DL IP flow description.",
            min_items=1,
            max_items=2,
        ),
    ] = None
    tosTC: Optional[TosTrafficClass] = None


# TS 29.122
class TimeWindow(ExtraBaseModel):
    """Represents a time window identified by a start time and a stop time."""

    startTime: datetime
    stopTime: datetime


# TS 29.512
FlowDescription: TypeAlias = Annotated[
    str, Field(description="Defines a packet filter of an IP flow.")
]


# TS 29.512
class FlowDirection(str, Enum):
    downlink = "DOWNLINK"
    uplink = "UPLINK"
    bidirectional = "BIDIRECTIONAL"
    unspecified = "UNSPECIFIED"


# TS 29.512
class RequestedQosMonitoringParameter(str, Enum):
    DOWNLINK = "DOWNLINK"
    UPLINK = "UPLINK"
    ROUND_TRIP = "ROUND_TRIP"
    DOWNLINK_DATA_RATE = "DOWNLINK_DATA_RATE"
    UPLINK_DATA_RATE = "UPLINK_DATA_RATE"
    DOWNLINK_CONGESTION = "DOWNLINK_CONGESTION"
    UPLINK_CONGESTION = "UPLINK_CONGESTION"


# TS 29.512
class ReportingFrequency(str, Enum):
    EVENT_TRIGGERED = "EVENT_TRIGGERED"
    PERIODIC = "PERIODIC"


# TS 29.512
class QosMonitoringParamType(str, Enum):
    """Indicates the QoS monitoring parameter type."""

    PACKET_DELAY = "PACKET_DELAY"
    CONGESTION = "CONGESTION"
    DATA_RATE = "DATA_RATE"


# TS 29.122
class QosMonitoringInformation(ExtraBaseModel):
    reqQosMonParams: Annotated[
        List[RequestedQosMonitoringParameter], Field(min_items=1)
    ]
    repFreqs: Annotated[List[ReportingFrequency], Field(min_items=1)]
    repThreshDl: Optional[Uinteger] = None
    repThreshUl: Optional[Uinteger] = None
    repThreshRp: Optional[Uinteger] = None
    conThreshDl: Optional[Uinteger] = None
    conThreshUl: Optional[Uinteger] = None
    waitTime: Optional[DurationSec] = None
    repPeriod: Optional[DurationSec] = None
    repThreshDatRateDl: Optional[BitRate] = None
    repThreshDatRateUl: Optional[BitRate] = None
    consDataRateThrDl: Optional[BitRate] = None
    consDataRateThrUl: Optional[BitRate] = None


# TS 29.122
Volume: TypeAlias = Annotated[
    int,
    Field(description="Unsigned integer identifying a volume in units of bytes.", ge=0),
]


# TS 29.122
class UsageThreshold(ExtraBaseModel):
    duration: Annotated[
        Optional[int],
        Field(
            description="Unsigned integer identifying a period of time in units of seconds.",
            ge=0,
        ),
    ] = None
    totalVolume: Optional[Volume] = None
    downlinkVolume: Optional[Volume] = None
    uplinkVolume: Optional[Volume] = None


# TS 29.514
class EthFlowDescription(ExtraBaseModel):
    destMacAddr: Optional[MacAddr48] = None
    ethType: str
    fDesc: Optional[FlowDescription] = None
    fDir: Optional[FlowDirection] = None
    sourceMacAddr: Optional[MacAddr48] = None
    vlanTags: Annotated[Optional[List[str]], Field(max_items=2, min_items=1)] = None
    srcMacAddrEnd: Optional[MacAddr48] = None
    destMacAddrEnd: Optional[MacAddr48] = None


# TS 29.514
TscPriorityLevel: TypeAlias = Annotated[
    int, Field(ge=1, le=8, description="Represents the priority level of TSC Flows.")
]


# TS 29.514
class PeriodicityRange(ExtraBaseModel):
    lowerBound: Optional[Uinteger] = None
    upperBound: Optional[Uinteger] = None
    periodicVals: Annotated[Optional[List[Uinteger]], Field(min_items=1)] = None
    addPeriodicVals: Annotated[Optional[List[Uinteger]], Field(min_items=1)] = None

    @root_validator
    def bound_or_periodic_vals(cls, v):
        if v.get("periodicVals") is not None:
            return v

        if v.get("addPeriodicVals") is not None:
            return v

        if v.get("lowerBound") is not None and v.get("upperBound") is not None:
            return v

        raise ValueError(
            'Either the "periodicVals" attribute, the "addPeriodicVals" attribute, or both the "lowerBound" attribute and the "upperBound" attribute shall be present.'
        )

    @root_validator
    def periodic_vals_exclusive(cls, v):
        if v.get("periodicVals") is not None and v.get("addPeriodicVals") is not None:
            raise ValueError(
                'The "periodicVals" and "addPeriodicVals" attributes are mutually exclusive.'
            )

        return v


# TS 29.514
class TscaiInputContainer(ExtraBaseModel):
    periodicity: Optional[Uinteger] = None
    burstArrivalTime: Optional[datetime] = None
    surTimeInNumMsg: Optional[Uinteger] = None
    surTimeInTime: Optional[Uinteger] = None
    burstArrivalTimeWnd: Optional[TimeWindow] = None
    periodicityRange: Optional[PeriodicityRange] = None


# TS 29.514
class MediaType(str, Enum):
    AUDIO = "AUDIO"
    VIDEO = "VIDEO"
    DATA = "DATA"
    APPLICATION = "APPLICATION"
    CONTROL = "CONTROL"
    TEXT = "TEXT"
    MESSAGE = "MESSAGE"
    OTHER = "OTHER"


# TS 29.514
class RttFlowReference(ExtraBaseModel):
    flowDir: Optional[FlowDirection] = None
    sharedKey: Uint32


# TS 29.514
DurationMilliSec: TypeAlias = Annotated[
    int, Field(description="Indicates the time interval in units of milliseconds.")
]


# TS 29.514
class AfEvent(str, Enum):
    """Represents an event to notify to the AF."""

    ACCESS_TYPE_CHANGE = "ACCESS_TYPE_CHANGE"
    ANI_REPORT = "ANI_REPORT"
    APP_DETECTION = "APP_DETECTION"
    CHARGING_CORRELATION = "CHARGING_CORRELATION"
    EPS_FALLBACK = "EPS_FALLBACK"
    EXTRA_UE_ADDR = "EXTRA_UE_ADDR"
    FAILED_QOS_UPDATE = "FAILED_QOS_UPDATE"
    FAILED_RESOURCES_ALLOCATION = "FAILED_RESOURCES_ALLOCATION"
    OUT_OF_CREDIT = "OUT_OF_CREDIT"
    PDU_SESSION_STATUS = "PDU_SESSION_STATUS"
    PLMN_CHG = "PLMN_CHG"
    QOS_MONITORING = "QOS_MONITORING"
    QOS_NOTIF = "QOS_NOTIF"
    RAN_NAS_CAUSE = "RAN_NAS_CAUSE"
    REALLOCATION_OF_CREDIT = "REALLOCATION_OF_CREDIT"
    SAT_CATEGORY_CHG = "SAT_CATEGORY_CHG"
    SUCCESSFUL_QOS_UPDATE = "SUCCESSFUL_QOS_UPDATE"
    SUCCESSFUL_RESOURCES_ALLOCATION = "SUCCESSFUL_RESOURCES_ALLOCATION"
    TSN_BRIDGE_INFO = "TSN_BRIDGE_INFO"
    UP_PATH_CHG_FAILURE = "UP_PATH_CHG_FAILURE"
    USAGE_REPORT = "USAGE_REPORT"
    UE_REACH_STATUS_CH = "UE_REACH_STATUS_CH"
    BAT_OFFSET_INFO = "BAT_OFFSET_INFO"
    URSP_ENF_INFO = "URSP_ENF_INFO"
    PACK_DEL_VAR = "PACK_DEL_VAR"
    L4S_SUPP = "L4S_SUPP"
    RT_DELAY_TWO_QOS_FLOWS = "RT_DELAY_TWO_QOS_FLOWS"


# TS 29.514
class AfNotifMethod(str, Enum):
    """Represents the notification methods that can be subscribed for an event."""

    EVENT_DETECTION = "EVENT_DETECTION"
    ONE_TIME = "ONE_TIME"
    PERIODIC = "PERIODIC"


# TS 29.514
class AfEventSubscription(ExtraBaseModel):
    event: AfEvent
    notifMethod: Optional[AfNotifMethod] = None
    repPeriod: Optional[DurationSec] = None
    waitTime: Optional[DurationSec] = None
    qosMonParamType: Optional[QosMonitoringParamType] = None


# TS 29.514
class RequiredAccessInfo(str, Enum):
    """Indicates the access network information required for an AF session."""

    USER_LOCATION = "USER_LOCATION"
    UE_TIME_ZONE = "UE_TIME_ZONE"


# TS 29.514
AfAppId: TypeAlias = Annotated[
    str, Field(description="Contains an AF application identifier.")
]


# TS 29.514
class EventsSubscReqData(ExtraBaseModel):
    events: Annotated[List[AfEventSubscription], Field(min_items=1)]
    notifUri: Optional[Uri] = None
    reqQosMonParams: Optional[
        Annotated[List[RequestedQosMonitoringParameter], Field(min_items=1)]
    ] = None
    qosMon: Optional[QosMonitoringInformation] = None
    qosMonDatRate: Optional[QosMonitoringInformation] = None
    pdvReqMonParams: Optional[
        Annotated[List[RequestedQosMonitoringParameter], Field(min_items=1)]
    ] = None
    pdvMon: Optional[QosMonitoringInformation] = None
    congestMon: Optional[QosMonitoringInformation] = None
    rttMon: Optional[QosMonitoringInformation] = None
    rttFlowRef: Optional[RttFlowReference] = None
    reqAnis: Optional[Annotated[List[RequiredAccessInfo], Field(min_items=1)]] = None
    usgThres: Optional[UsageThreshold] = None
    notifCorreId: Optional[str] = None
    afAppIds: Optional[Annotated[List[AfAppId], Field(min_items=1)]] = None
    directNotifInd: Annotated[
        Optional[bool],
        Field(
            description="Indicates whether the direct event notification is requested (true) or not (false) for the provided QoS monitoring parameters. Default value is false.",
        ),
    ] = None
    avrgWndw: Optional[AverWindow] = None


# TS 29.514
class ServAuthInfo(str, Enum):
    """Indicates the result of the Policy Authorization service request from the AF."""

    TP_NOT_KNOWN = "TP_NOT_KNOWN"
    TP_EXPIRED = "TP_EXPIRED"
    TP_NOT_YET_OCURRED = "TP_NOT_YET_OCURRED"
    ROUT_REQ_NOT_AUTHORIZED = "ROUT_REQ_NOT_AUTHORIZED"
    DIRECT_NOTIF_NOT_POSSIBLE = "DIRECT_NOTIF_NOT_POSSIBLE"


# TS 29.514
ContentVersion: TypeAlias = Annotated[
    int, Field(description="Represents the content version of some content.")
]


# TS 29.514
class Flows(ExtraBaseModel):
    contVers: Annotated[Optional[List[ContentVersion]], Field(min_items=1)] = None
    fNums: Annotated[Optional[List[int]], Field(min_items=1)] = None
    medCompN: int


# TS 29.514
class PdvMonitoringReport(ExtraBaseModel):
    flows: Annotated[
        Optional[List[Flows]],
        Field(description="Identification of the flows.", min_items=1),
    ] = None
    ulPdv: Annotated[
        Optional[int],
        Field(description="Uplink packet delay variation in units of milliseconds."),
    ] = None
    dlPdv: Annotated[
        Optional[int],
        Field(description="Downlink packet delay variation in units of milliseconds."),
    ] = None
    rtPdv: Annotated[
        Optional[int],
        Field(
            description="Round trip packet delay variation in units of milliseconds.",
        ),
    ] = None


# TS 29.514
class BatOffsetInfo(ExtraBaseModel):
    ranBatOffsetNotif: Annotated[
        int,
        Field(
            description="Indicates the BAT offset of the arrival time of the data burst in units of milliseconds.",
        ),
    ]
    adjPeriod: Optional[Uinteger] = None
    flows: Annotated[
        Optional[List[Flows]],
        Field(
            description="Identification of the flows. If no flows are provided, the BAT offset applies for all flows of the AF session.",
            min_items=1,
        ),
    ] = None


# TS 29.571
class MediaTransportProto(str, Enum):
    RTP = "RTP"
    SRTP = "SRTP"


# TS 29.571
class RtpHeaderExtType(str, Enum):
    PDU_SET_MARKING = "PDU_SET_MARKING"


# TS 29.571
class RtpHeaderExtInfo(ExtraBaseModel):
    rtpHeaderExtType: Optional[RtpHeaderExtType] = None
    rtpHeaderExtId: Annotated[Optional[int], Field(ge=1, le=255)] = None
    longFormat: Optional[bool] = None
    pduSetSizeActive: Optional[bool] = None


# TS 29.571
class RtpPayloadFormat(str, Enum):
    H264 = "H264"
    H265 = "H265"


# TS 29.571
class RtpPayloadInfo(ExtraBaseModel):
    rtpPayloadTypeList: Annotated[
        Optional[List[Annotated[int, Field(ge=1, le=127)]]], Field(min_items=1)
    ] = None
    rtpPayloadFormat: Optional[RtpPayloadFormat] = None


# TS 29.571
class ProtocolDescription(ExtraBaseModel):
    transportProto: Optional[MediaTransportProto] = None
    rtpHeaderExtInfo: Optional[RtpHeaderExtInfo] = None
    rtpPayloadInfoList: Annotated[
        Optional[List[RtpPayloadInfo]], Field(min_items=1)
    ] = None


# TS 29.571
class PlmnId(ExtraBaseModel):
    mcc: int
    mnc: int


# TS 29.571
class Ecgi(ExtraBaseModel):
    """Contains the ECGI (E-UTRAN Cell Global Identity), as described in 3GPP 23.003"""

    plmnId: PlmnId
    eutraCellId: Annotated[str, Field(regex=r"^[A-Fa-f0-9]{7}$")]
    nid: Optional[Annotated[str, Field(regex=r"^[A-Fa-f0-9]{11}$")]] = None


# TS 29.571
class Ncgi(ExtraBaseModel):
    """Contains the NCGI (NR Cell Global Identity), as described in 3GPP 23.003"""

    plmnId: PlmnId
    nrCellId: Annotated[str, Field(regex=r"^[A-Fa-f0-9]{9}$")]
    nid: Optional[Annotated[str, Field(regex=r"^[A-Fa-f0-9]{11}$")]] = None


# TS 29.571
class GNbId(ExtraBaseModel):
    """Provides the G-NB identifier."""

    bitLength: Annotated[
        Optional[int],
        Field(
            ge=22,
            le=32,
            description="Unsigned integer representing the bit length of the gNB ID as defined in clause 9.3.1.6 of 3GPP TS 38.413 [11], within the range 22 to 32.",
        ),
    ] = None
    gNBValue: Annotated[
        str,
        Field(
            regex=r"^[A-Fa-f0-9]{6,8}$",
            description='This represents the identifier of the gNB. The value of the gNB ID shall be encoded in hexadecimal representation. Each character in the string shall take a value of "0" to "9", "a" to "f" or "A" to "F" and shall represent 4 bits. The padding 0 shall be added to make multiple nibbles,  the most significant character representing the padding 0 if required together with the 4 most significant bits of the gNB ID shall appear first in the string, and the character representing the 4 least significant bit of the gNB ID shall appear last in the string.',
        ),
    ]


# TS 29.571
class GlobalRanNodeId(ExtraBaseModel):
    """One of the six attributes n3IwfId, gNbIdm, ngeNbId, wagfId, tngfId, eNbId shall be present."""

    plmnId: PlmnId
    n3IwfId: Optional[Annotated[str, Field(regex=r"^[A-Fa-f0-9]+$")]] = None
    gNbId: Optional[GNbId] = None
    ngeNbId: Optional[
        Annotated[
            str,
            Field(
                regex=r"^(MacroNGeNB-[A-Fa-f0-9]{5}|LMacroNGeNB-[A-Fa-f0-9]{6}|SMacroNGeNB-[A-Fa-f0-9]{5})$"
            ),
        ]
    ] = None
    wagfId: Optional[Annotated[str, Field(regex=r"^[A-Fa-f0-9]+$")]] = None
    tngfId: Optional[Annotated[str, Field(regex=r"^[A-Fa-f0-9]+$")]] = None
    nid: Annotated[str, Field(regex=r"^[A-Fa-f0-9]{11}$")]
    eNbId: Optional[
        Annotated[
            str,
            Field(
                regex=r"^(MacroeNB-[A-Fa-f0-9]{5}|LMacroeNB-[A-Fa-f0-9]{6}|SMacroeNB-[A-Fa-f0-9]{5}|HomeeNB-[A-Fa-f0-9]{7})$"
            ),
        ]
    ] = None


# TS 29.571
class Tai(ExtraBaseModel):
    """Contains the tracking area identity as described in 3GPP 23.003"""

    plmnId: PlmnId
    tac: Annotated[str, Field(regex=r"(^[A-Fa-f0-9]{4}$)|(^[A-Fa-f0-9]{6}$)")]
    nid: Optional[Annotated[str, Field(regex=r"^[A-Fa-f0-9]{11}$")]] = None


# TS 29.571
class EutraLocation(ExtraBaseModel):
    tai: Tai
    ignoreTai: Optional[bool] = False
    ecgi: Ecgi
    ignoreEcgi: Annotated[
        bool,
        Field(
            description="This flag when present shall indicate that the Ecgi shall be ignored When present, it shall be set as follows: - true: ecgi shall be ignored. - false (default): ecgi shall not be ignored.",
        ),
    ] = False
    ageOfLocationInformation: Annotated[
        Optional[int],
        Field(
            ge=0,
            le=32767,
            description='The value represents the elapsed time in minutes since the last network contact of the mobile station.  Value "0" indicates that the location information was obtained after a successful paging procedure for Active Location Retrieval when the UE is in idle mode or after a successful NG-RAN location reporting procedure with the eNB when the UE is in connected mode.  Any other value than "0" indicates that the location information is the last known one.  See 3GPP TS 29.002 clause 17.7.8.',
        ),
    ] = None
    ueLocationTimestamp: Optional[datetime] = None
    geographicalInformation: Annotated[
        Optional[str],
        Field(
            regex=r"^[0-9A-F]{16}$",
            description="Refer to geographical Information. See 3GPP TS 23.032 clause 7.3.2. Only the description of an ellipsoid point with uncertainty circle is allowed to be used.\n",
        ),
    ] = None
    geodeticInformation: Annotated[
        Optional[str],
        Field(
            regex=r"^[0-9A-F]{20}$",
            description="Refers to Calling Geodetic Location. See ITU-T Recommendation Q.763 (1999) [24] clause 3.88.2. Only the description of an ellipsoid point with uncertainty circle is allowed to be used.",
        ),
    ] = None
    globalNgenbId: Optional[GlobalRanNodeId] = None
    globalENbId: Optional[GlobalRanNodeId] = None


# TS 29.571
class PlmnIdNid(ExtraBaseModel):
    mcc: int
    mnc: int
    nid: Optional[Nid] = None


class NtnTaiInfo(ExtraBaseModel):
    plmnId: PlmnIdNid
    tacList: Annotated[List[Tac], Field(min_items=1)]
    derivedTac: Optional[Tac] = None


class NrLocation(ExtraBaseModel):
    """Contains the NR user location."""

    tai: Tai
    ignoreTai: bool = False
    ncgi: Ncgi
    ignoreNcgi: bool = False
    ageOfLocationInformation: Annotated[
        Optional[int],
        Field(
            ge=0,
            le=32767,
            description='The value represents the elapsed time in minutes since the last network contact of the mobile station. Value "0" indicates that the location information was obtained after a successful paging procedure for Active Location Retrieval when the UE is in idle mode or after a successful  NG-RAN location reporting procedure with the eNB when the UE is in connected mode. Any other value than "0" indicates that the location information is the last known one. See 3GPP TS 29.002 clause 17.7.8.',
        ),
    ] = None
    ueLocationTimestamp: datetime
    geographicalInformation: Annotated[
        Optional[str], Field(regex=r"^[0-9A-F]{16}$")
    ] = None
    geodeticInformation: Annotated[Optional[str], Field(regex=r"^[0-9A-F]{20}$")] = None
    globalGnbId: Optional[GlobalRanNodeId] = None
    ntnTaiInfo: Optional[NtnTaiInfo] = None


# TS 29.571
class CellGlobalId(ExtraBaseModel):
    """Contains a Cell Global Identification as defined in 3GPP TS 23.003, clause 4.3.1."""

    plmnId: PlmnId
    lac: Annotated[str, Field(regex=r"^[A-Fa-f0-9]{4}$")]
    cellId: Annotated[str, Field(regex=r"^[A-Fa-f0-9]{4}$")]


# TS 29.571
class ServiceAreaId(ExtraBaseModel):
    """Contains a Service Area Identifier as defined in 3GPP TS 23.003, clause 12.5."""

    plmnId: PlmnId
    lac: Annotated[
        str, Field(regex=r"^[A-Fa-f0-9]{4}$", description="Location Area Code.")
    ]
    sac: Annotated[
        str, Field(regex=r"^[A-Fa-f0-9]{4}$", description="Service Area Code.")
    ]


# TS 29.571
class LocationAreaId(ExtraBaseModel):
    """Contains a Location area identification as defined in 3GPP TS 23.003, clause 4.1."""

    plmnId: PlmnId
    lac: Annotated[
        str, Field(regex=r"^[A-Fa-f0-9]{4}$", description="Location Area Code.")
    ]


# TS 29.571
class RoutingAreaId(ExtraBaseModel):
    """Contains a Routing Area Identification as defined in 3GPP TS 23.003, clause 4.2."""

    plmnId: PlmnId
    lac: Annotated[
        str, Field(regex=r"^[A-Fa-f0-9]{4}$", description="Location Area Code.")
    ]
    rac: Annotated[
        str, Field(regex=r"^[A-Fa-f0-9]{2}$", description="Routing Area Code.")
    ]


# TS 29.571
class UtraLocation(ExtraBaseModel):
    """Exactly one of cgi, sai or lai shall be present."""

    cgi: CellGlobalId
    sai: ServiceAreaId
    lai: LocationAreaId
    rai: RoutingAreaId
    ageOfLocationInformation: Annotated[
        Optional[int],
        Field(
            ge=0,
            le=32767,
            description='The value represents the elapsed time in minutes since the last network contact of the mobile station.  Value "0" indicates that the location information was obtained after a successful paging procedure for  Active Location Retrieval when the UE is in idle mode\n or after a successful location reporting procedure  the UE is in connected mode. Any\nother value than "0" indicates that the location information is the last known one.  See 3GPP TS 29.002 clause 17.7.8.',
        ),
    ] = None
    ueLocationTimestamp: Optional[datetime] = None
    geographicalInformation: Annotated[
        Optional[str],
        Field(
            regex=r"^[0-9A-F]{16}$",
            description="Refer to geographical Information.See 3GPP TS 23.032 clause 7.3.2. Only the description of an ellipsoid point with uncertainty circle is allowed to be used.",
        ),
    ] = None
    geodeticInformation: Annotated[
        Optional[str],
        Field(
            regex=r"^[0-9A-F]{20}$",
            description="Refers to Calling Geodetic Location. See ITU-T\xa0Recommendation Q.763 (1999) clause 3.88.2. Only the description of an ellipsoid point with uncertainty circle is allowed to be used.",
        ),
    ]


# TS 29.571
class TransportProtocol(str, Enum):
    udp = "UDP"
    tcp = "TCP"


# TS 29.571
class LineType(str, Enum):
    dsl = "DSL"
    pon = "PON"


# TS 29.571
class TnapId(ExtraBaseModel):
    """Contain the TNAP Identifier see clause5.6.2 of 3GPP TS 23.501."""

    ssId: Annotated[
        Optional[str],
        Field(
            description="This IE shall be present if the UE is accessing the 5GC via a trusted WLAN access network.When present, it shall contain the SSID of the access point to which the UE is attached, that is received over NGAP,  see IEEE Std 802.11-2012.",
        ),
    ] = None
    bssId: Annotated[
        Optional[str],
        Field(
            description="When present, it shall contain the BSSID of the access point to which the UE is attached, that is received over NGAP, see IEEE Std 802.11-2012.",
        ),
    ] = None
    civicAddress: Optional[Bytes] = None


# TS 29.571
class TwapId(ExtraBaseModel):
    """Contain the TWAP Identifier as defined in clause 4.2.8.5.3 of 3GPP TS 23.501 or the WLAN location information as defined in clause 4.5.7.2.8 of 3GPP TS 23.402."""

    ssId: Annotated[
        Optional[str],
        Field(
            description="This IE shall contain the SSID of the access point to which the UE is attached, that is received over NGAP, see IEEE Std 802.11-2012.",
        ),
    ] = None
    bssId: Annotated[
        Optional[str],
        Field(
            description="When present, it shall contain the BSSID of the access point to which the UE is attached, for trusted WLAN access, see IEEE Std 802.11-2012.",
        ),
    ] = None
    civicAddress: Optional[Bytes] = None


# TS 29.571
class HfcNodeId(ExtraBaseModel):
    hfcNId: HfcNId


class N3gaLocation(ExtraBaseModel):
    n3gppTai: Optional[Tai] = None
    n3IwfId: Annotated[
        Optional[str],
        Field(
            regex=r"^[A-Fa-f0-9]+$",
            description='This IE shall contain the N3IWF identifier received over NGAP and shall be encoded as a  string of hexadecimal characters. Each character in the string shall take a value of "0"  to "9", "a" to "f" or "A" to "F" and shall represent 4 bits. The most significant  character representing the 4 most significant bits of the N3IWF ID shall appear first in  the string, and the character representing the 4 least significant bit of the N3IWF ID  shall appear last in the string.',
        ),
    ] = None
    ueIpv4Addr: Optional[IPv4Address] = None
    ueIpv6Addr: Optional[IPv6Address] = None
    portNumber: Optional[Uinteger] = None
    protocol: Optional[TransportProtocol] = None
    tnapId: Optional[TnapId] = None
    twapId: Optional[TwapId] = None
    hfcNodeId: Optional[HfcNodeId] = None
    gli: Optional[Gli] = None
    w5gbanLineType: Optional[LineType] = None
    gci: Optional[Gci] = None


# TS 29.571
class GeraLocation(ExtraBaseModel):
    """Contains the Non-3GPP access user location."""

    locationNumber: Annotated[
        Optional[str],
        Field(
            description="Location number within the PLMN. See 3GPP TS 23.003, clause 4.5.",
        ),
    ] = None
    cgi: Optional[CellGlobalId] = None
    rai: Optional[RoutingAreaId] = None
    sai: Optional[ServiceAreaId] = None
    lai: Optional[LocationAreaId] = None
    vlrNumber: Annotated[
        Optional[str], Field(description="VLR number. See 3GPP TS 23.003 clause 5.1.")
    ] = None
    mscNumber: Annotated[
        Optional[str], Field(description="MSC number. See 3GPP TS 23.003 clause 5.1. ")
    ] = None
    ageOfLocationInformation: Annotated[
        Optional[int],
        Field(
            ge=0,
            le=32767,
            description='The value represents the elapsed time in minutes since the last network contact of the mobile station. Value "0" indicates that the location information was obtained after a successful paging procedure for  Active Location Retrieval when the UE is in idle mode or after a successful location reporting procedure the UE is in connected mode. Any other value than "0" indicates that the location information is the last known one. See 3GPP TS 29.002 clause 17.7.8.',
        ),
    ] = None
    ueLocationTimestamp: Optional[datetime] = None
    geographicalInformation: Annotated[
        Optional[str],
        Field(
            regex=r"^[0-9A-F]{16}$",
            description="Refer to geographical Information.See 3GPP TS 23.032 clause 7.3.2. Only the description of an ellipsoid point with uncertainty circle is allowed to be used.",
        ),
    ] = None
    geodeticInformation: Annotated[
        Optional[str],
        Field(
            regex=r"^[0-9A-F]{20}$",
            description="Refers to Calling Geodetic Location.See ITU-T Recommendation Q.763 (1999) clause 3.88.2.  Only the description of an ellipsoid point with uncertainty circle is allowed to be used.",
        ),
    ] = None


# TS 29.571
class UserLocation(ExtraBaseModel):
    """At least one of eutraLocation, nrLocation and n3gaLocation shall be present. Several
    of them may be present."""

    eutraLocation: EutraLocation
    nrLocation: NrLocation
    n3gaLocation: N3gaLocation
    utraLocation: UtraLocation
    geraLocation: GeraLocation


# TS 29.122
class DayOfWeek(ExtraBaseModel):
    day: Annotated[Optional[int], Field(description="", ge=1, le=7)] = None


# TS 29.571
class RatType(str, Enum):
    """Indicates the radio access used."""

    NR = "NR"
    EUTRA = "EUTRA"
    WLAN = "WLAN"
    VIRTUAL = "VIRTUAL"
    NBIOT = "NBIOT"
    WIRELINE = "WIRELINE"
    WIRELINE_CABLE = "WIRELINE_CABLE"
    WIRELINE_BBF = "WIRELINE_BBF"
    LTE_M = "LTE-M"
    NR_U = "NR_U"
    EUTRA_U = "EUTRA_U"
    TRUSTED_N3GA = "TRUSTED_N3GA"
    TRUSTED_WLAN = "TRUSTED_WLAN"
    UTRA = "UTRA"
    GERA = "GERA"
    NR_LEO = "NR_LEO"
    NR_MEO = "NR_MEO"
    NR_GEO = "NR_GEO"
    NR_OTHER_SAT = "NR_OTHER_SAT"
    NR_REDCAP = "NR_REDCAP"
    WB_E_UTRAN_LEO = "WB_E_UTRAN_LEO"
    WB_E_UTRAN_MEO = "WB_E_UTRAN_MEO"
    WB_E_UTRAN_GEO = "WB_E_UTRAN_GEO"
    WB_E_UTRAN_OTHERSAT = "WB_E_UTRAN_OTHERSAT"
    NB_IOT_LEO = "NB_IOT_LEO"
    NB_IOT_MEO = "NB_IOT_MEO"
    NB_IOT_GEO = "NB_IOT_GEO"
    NB_IOT_OTHERSAT = "NB_IOT_OTHERSAT"
    LTE_M_LEO = "LTE_M_LEO"
    LTE_M_MEO = "LTE_M_MEO"
    LTE_M_GEO = "LTE_M_GEO"
    LTE_M_OTHERSAT = "LTE_M_OTHERSAT"
    NR_EREDCAP = "NR_EREDCAP"


# TS 29.571
class QosResourceType(str, Enum):
    nonGbr = "NON_GBR"
    nonCriticalGbr = "NON_CRITICAL_GBR"
    criticalGbr = "CRITICAL_GBR"


# TS 29.571
class AnalyticsSubset(str, Enum):
    numOfUeReg = "NUM_OF_UE_REG"
    numOfPduSessEstbl = "NUM_OF_PDU_SESS_ESTBL"
    resUsage = "RES_USAGE"
    numOfExceedResUsageLoadLevelThr = "NUM_OF_EXCEED_RES_USAGE_LOAD_LEVEL_THR"
    periodOfExceedResUsageLoadLevelThr = "PERIOD_OF_EXCEED_RES_USAGE_LOAD_LEVEL_THR"
    exceedLoadLevelThrInd = "EXCEED_LOAD_LEVEL_THR_IND"
    listOfTopAppUl = "LIST_OF_TOP_APP_UL"
    listOfTopAppDl = "LIST_OF_TOP_APP_DL"
    nfStatus = "NF_STATUS"
    nfResourceUsage = "NF_RESOURCE_USAGE"
    nfLoad = "NF_LOAD"
    nfPeakLoad = "NF_PEAK_LOAD"
    nfLoadAvgInAoi = "NF_LOAD_AVG_IN_AOI"
    disperAmount = "DISPER_AMOUNT"
    disperClass = "DISPER_CLASS"
    ranking = "RANKING"
    percentileRanking = "PERCENTILE_RANKING"
    rssi = "RSSI"
    rtt = "RTT"
    trafficInfo = "TRAFFIC_INFO"
    numberOfUes = "NUMBER_OF_UES"
    appListForUeComm = "APP_LIST_FOR_UE_COMM"
    n4SessInactTimerForEuComm = "N4_SESS_INACT_TIMER_FOR_UE_COMM"
    avgTrafficRate = "AVG_TRAFFIC_RATE"
    maxTrafficRate = "MAX_TRAFFIC_RATE"
    avgPacketDelay = "AVG_PACKET_DELAY"
    maxPacketDelay = "MAX_PACKET_DELAY"
    avgPacketLossRate = "AVG_PACKET_LOSS_RATE"
    ueLocation = "UE_LOCATION"
    listOfHighExpUe = "LIST_OF_HIGH_EXP_UE"
    listOfMediumExpUe = "LIST_OF_MEDIUM_EXP_UE"
    lsitOfLowExpUe = "LIST_OF_LOW_EXP_UE"
    avgUlPktDropRate = "AVG_UL_PKT_DROP_RATE"
    varUlPktDropRate = "VAR_UL_PKT_DROP_RATE"
    avgDlPktDropRate = "AVG_DL_PKT_DROP_RATE"
    varDlPktDropRate = "VAR_DL_PKT_DROP_RATE"
    avgUlPktDelay = "AVG_UL_PKT_DELAY"
    varUlPktDelay = "VAR_UL_PKT_DELAY"
    avgDlPktDelay = "AVG_DL_PKT_DELAY"
    varDlPktDelay = "VAR_DL_PKT_DELAY"


# TS 29.571
class PartitioningCriteria(str, Enum):
    tac = "TAC"
    subPlmn = "SUBPLMN"
    geoArea = "GEOAREA"
    snssai = "SNSSAI"
    dnn = "DNN"


# TS 29.571
class NotificationFlag(str, Enum):
    activate = "ACTIVATE"
    deactivate = "DEACTIVATE"
    retrieval = "RETRIEVAL"


# TS 29.571
ExtMaxDataBurstVol: TypeAlias = Annotated[
    int,
    Field(
        ge=4096,
        le=2000000,
        description="Unsigned integer indicating Maximum Data Burst Volume (see clauses 5.7.3.7 and 5.7.4 of 3GPP TS 23.501), expressed in Bytes.",
    ),
]


# TS 29.571
class PduSetHandlingInfo(str, Enum):
    ALL_PDUS_NEEDED = "ALL_PDUS_NEEDED"
    ALL_PDUS_NOT_NEEDED = "ALL_PDUS_NOT_NEEDED"


# TS 29.571
class PduSetQosPara(ExtraBaseModel):
    pduSetDelayBudget: Optional[ExtPacketDelBudget] = None
    pduSetErrRate: Optional[PacketErrRate] = None
    pduSetHandlingInfo: Optional[PduSetHandlingInfo] = None

    @root_validator
    def any_of(cls, v):
        if v.get("pduSetHandlingInfo") is not None:
            return v

        if (
            v.get("pduSetDelayBudget") is not None
            and v.get("pduSetErrRate") is not None
        ):
            return v

        raise ValueError(
            "At least one of the following shall be present: 1) pduSetHandlingInfo and/or 2) both pduSetDelayBudget and pduSetErrRate."
        )


# TS 29.122
class WebsockNotifConfig(ExtraBaseModel):
    """Represents the configuration information for the delivery of notifications over Websockets."""

    websocketUri: Optional[Link] = None
    requestWebsocketUri: Annotated[
        Optional[bool],
        Field(
            description=" Set by the SCS/AS to indicate that the Websocket delivery is requested.",
        ),
    ] = None


# TS 29.122
class SponsorInformation(ExtraBaseModel):
    sponsorId: Annotated[str, Field(description="It indicates Sponsor ID.")]
    aspId: Annotated[
        str, Field(description="It indicates Application Service Provider ID.")
    ]


# TS 29.122
class TscQosRequirement(ExtraBaseModel):
    reqGbrDl: Optional[BitRate] = None
    reqGbrUl: Optional[BitRate] = None
    reqMbrDl: Optional[BitRate] = None
    reqMbrUl: Optional[BitRate] = None
    maxTscBurstSize: Optional[ExtMaxDataBurstVol] = None
    req5Gsdelay: Optional[PacketDelBudget] = None
    reqPer: Optional[PacketErrRate] = None
    priority: Optional[TscPriorityLevel] = None
    tscaiTimeDom: Optional[Uinteger] = None
    tscaiInputDl: Optional[TscaiInputContainer] = None
    tscaiInputUl: Optional[TscaiInputContainer] = None
    capBatAdaptation: Annotated[
        Optional[bool],
        Field(
            description='Indicates the capability for AF to adjust the burst sending time, when it is supported and set to "true". The default value is "false" if omitted.',
        ),
    ] = None


# TS 29.122
class UserPlaneEvent(str, Enum):
    SESSION_TERMINATION = "SESSION_TERMINATION"
    LOSS_OF_BEARER = "LOSS_OF_BEARER"  # 4G Only
    RECOVERY_OF_BEARER = "RECOVERY_OF_BEARER"  # 4G Only
    RELEASE_OF_BEARER = "RELEASE_OF_BEARER"  # 4G Only
    USAGE_REPORT = "USAGE_REPORT"
    FAILED_RESOURCES_ALLOCATION = "FAILED_RESOURCES_ALLOCATION"
    QOS_GUARANTEED = "QOS_GUARANTEED"
    QOS_NOT_GUARANTEED = "QOS_NOT_GUARANTEED"
    QOS_MONITORING = "QOS_MONITORING"
    SUCCESSFUL_RESOURCES_ALLOCATION = "SUCCESSFUL_RESOURCES_ALLOCATION"
    ACCESS_TYPE_CHANGE = "ACCESS_TYPE_CHANGE"
    PLMN_CHG = "PLMN_CHG"
    L4S_NOT_AVAILABLE = "L4S_NOT_AVAILABLE"
    L4S_AVAILABLE = "L4S_AVAILABLE"
    BAT_OFFSET_INFO = "BAT_OFFSET_INFO"
    RT_DELAY_TWO_QOS_FLOWS = "RT_DELAY_TWO_QOS_FLOWS"
    PACK_DELAY_VAR = "PACK_DELAY_VAR"


# TS 29.122
class AccumulatedUsage(ExtraBaseModel):
    duration: Optional[DurationSec] = None
    totalVolume: Optional[Volume] = None
    downlinkVolume: Optional[Volume] = None
    uplinkVolume: Optional[Volume] = None


# TS 29.565
class TemporalInValidity(ExtraBaseModel):
    startTime: datetime
    stopTime: datetime


# TS 29.122
# TODO: locationArea5g
# class LocationArea5G(ExtraBaseModel):
#     """Represents a user location area when the UE is attached to 5G. """
#     geographicAreas: List[GeographicArea] = Field(None, description="Identifies a list of geographic area of the user where the UE is located.", min_items=0)
#     civicAddresses: List[CivicAddress] = Field(None, description="Identifies a list of civic addresses of the user where the UE is located.", min_items=0)
#     nwAreaInfo: NetworkAreaInfo
