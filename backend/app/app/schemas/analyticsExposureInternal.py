"""
Internal schemas for the Analytics Exposure API.

This module contains two categories of types that are not part of the 3GPP standard:

  1. CUSTOM INTERNAL TYPES (for runtime state)
  2. PROMETHEUS API RESULT MODELS
"""

from typing import Literal, Union

from pydantic import BaseModel, Field

from app.schemas import AnalyticsExposureSubsc


# =============================================================================
# CUSTOM INTERNAL TYPES
# State about subscription documents    (stored in MongoDB alongside the subsc)
# =============================================================================

class AnalyticsState(BaseModel):
    """Internal runtime state stored alongside an analytics subscription in MongoDB."""
    report_count: int = 0
    is_active: bool = True


class AnalyticsExposureSubscWithState(AnalyticsExposureSubsc):
    """Internal schema used by the poller to deserialize the full MongoDB document, including runtime state."""
    state: AnalyticsState = Field(default_factory=AnalyticsState)


# =============================================================================
# PROMETHEUS API RESULT MODELS
# Typed representations of the four Prometheus resultType schemas:
#   "vector"  → list[VectorEntry]   (instant-vector query)
#   "matrix"  → list[MatrixEntry]   (range-vector query)
#   "scalar"  → tuple[float, str]   (scalar expression)
#   "string"  → tuple[float, str]   (string literal)
# =============================================================================

class VectorEntry(BaseModel):
    """Single entry in a Prometheus instant-vector result (resultType: "vector")."""
    metric: dict[str, str]
    value: tuple[float, str]          # [unix_time, "sample_value"]


class MatrixEntry(BaseModel):
    """Single entry in a Prometheus range-vector result (resultType: "matrix")."""
    metric: dict[str, str]
    values: list[tuple[float, str]]   # [[unix_time, "sample_value"], ...]


class _QueryResultBase(BaseModel):
    """Base class for all Prometheus query results, containing common fields."""
    status: str

    @property
    def is_success(self) -> bool:
        return self.status == "success"


class VectorQueryResult(_QueryResultBase):
    """Result of a Prometheus instant-vector query (resultType: "vector")."""
    result_type: Literal["vector"]
    result: list[VectorEntry]


class MatrixQueryResult(_QueryResultBase):
    """Result of a Prometheus range-vector query (resultType: "matrix")."""
    result_type: Literal["matrix"]
    result: list[MatrixEntry]


class ScalarQueryResult(_QueryResultBase):
    """Result of a Prometheus scalar expression query (resultType: "scalar")."""
    result_type: Literal["scalar"]
    result: tuple[float, str]


class StringQueryResult(_QueryResultBase):
    """Result of a Prometheus string literal query (resultType: "string")."""
    result_type: Literal["string"]
    result: tuple[float, str]


QueryResult = Union[VectorQueryResult, MatrixQueryResult, ScalarQueryResult, StringQueryResult]

