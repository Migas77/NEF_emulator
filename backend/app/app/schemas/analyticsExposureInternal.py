"""
Internal schemas for the Analytics Exposure API.

This module contains two categories of types that are not part of the 3GPP standard:

  1. CUSTOM INTERNAL TYPES (for runtime state)
  2. PROMETHEUS API RESULT MODELS
"""
from typing import Literal, Union, Annotated

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

class VectorResult(BaseModel):
    """Single entry in a Prometheus instant-vector result (resultType: "vector")."""
    metric: dict[str, str]                      # { label_name: label_value, ... }
    value: tuple[float, str]                    # ( unix_time, "sample_value" )

    @property
    def values(self) -> list[tuple[float, str]]:
        return [self.value]


class MatrixResult(BaseModel):
    """Single entry in a Prometheus range-vector result (resultType: "matrix")."""
    metric: dict[str, str]                      # { label_name: label_value, ... }
    values: list[tuple[float, str]]             # [ ( unix_time, "sample_value" ), ...]


class BaseQueryData(BaseModel):
    """Base class for the 'data' field in Prometheus query results, containing common fields."""
    resultType: Literal["vector", "matrix", "scalar", "string"]
    result: list[VectorResult] | list[MatrixResult] | tuple[float, str]


class VectorQueryData(BaseQueryData):
    """Result of a Prometheus instant-vector query (resultType: "vector")."""
    resultType: Literal["vector"]
    result: list[VectorResult]


class MatrixQueryData(BaseQueryData):
    """Result of a Prometheus range-vector query (resultType: "matrix")."""
    resultType: Literal["matrix"]
    result: list[MatrixResult]


class ScalarQueryData(BaseQueryData):
    """Result of a Prometheus scalar expression query (resultType: "scalar")."""
    resultType: Literal["scalar"]
    result: tuple[float, str]


class StringQueryData(BaseQueryData):
    """Result of a Prometheus string literal query (resultType: "string")."""
    resultType: Literal["string"]
    result: tuple[float, str]


QueryData = Annotated[
    Union[VectorQueryData, MatrixQueryData, ScalarQueryData, StringQueryData],
    Field(discriminator="resultType")
]


class QueryResult(BaseModel):
    """Class for all Prometheus query results."""
    status: str
    data: QueryData

