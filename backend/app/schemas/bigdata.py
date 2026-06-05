from __future__ import annotations

from datetime import datetime
from typing import Any, Literal, Optional

from pydantic import BaseModel, ConfigDict, Field


class BigDataDatasetCreate(BaseModel):
    name: str = Field(..., min_length=1, max_length=255)
    raw_path: str = Field(..., min_length=1, max_length=500)
    processed_path: Optional[str] = Field(default=None, max_length=500)
    format: str = Field(default="csv", min_length=1, max_length=32)
    status: str = Field(default="uploaded", min_length=1, max_length=32)
    is_private: bool = True


class BigDataDatasetUpdate(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    name: Optional[str] = Field(default=None, min_length=1, max_length=255)
    raw_path: Optional[str] = Field(default=None, min_length=1, max_length=500)
    processed_path: Optional[str] = Field(default=None, max_length=500)
    format: Optional[str] = Field(default=None, min_length=1, max_length=32)
    status: Optional[str] = Field(default=None, min_length=1, max_length=32)
    is_private: Optional[bool] = None
    row_count: Optional[int] = Field(default=None, ge=0)
    column_count: Optional[int] = Field(default=None, ge=0)
    schema_: Optional[dict[str, Any]] = Field(
        default=None,
        validation_alias="schema_json",
        serialization_alias="schema_json",
    )
    profile_json: Optional[dict[str, Any]] = None
    error: Optional[str] = None


class BigDataDatasetOut(BaseModel):
    model_config = ConfigDict(from_attributes=True, populate_by_name=True)

    id: int
    user_id: int
    name: str
    raw_path: str
    processed_path: Optional[str] = None
    format: str
    status: str
    is_private: bool = True
    row_count: Optional[int] = None
    column_count: Optional[int] = None
    schema_: Optional[dict[str, Any]] = Field(
        default=None,
        validation_alias="schema_json",
        serialization_alias="schema_json",
    )
    profile_json: Optional[dict[str, Any]] = None
    error: Optional[str] = None
    created_at: datetime


class BigDataDatasetSummary(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    name: str
    status: str
    is_private: bool = True
    row_count: Optional[int] = None
    column_count: Optional[int] = None
    created_at: datetime


class BigDataDatasetList(BaseModel):
    datasets: list[BigDataDatasetSummary]


class BigDataChatSampleFilter(BaseModel):
    column: str = Field(..., min_length=1, max_length=255)
    operator: Literal["=", "!=", ">", ">=", "<", "<=", "contains"]
    value: str = Field(..., max_length=1000)


class BigDataChatSampleCreate(BaseModel):
    name: str = Field(..., min_length=1, max_length=255)
    row_limit: int = Field(default=5000, ge=1, le=50000)
    sampling_mode: Literal["first", "random"] = "first"
    columns: list[str] = Field(default_factory=list, max_length=200)
    filters: list[BigDataChatSampleFilter] = Field(default_factory=list, max_length=20)
