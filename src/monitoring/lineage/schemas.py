"""Lineage event schemas — P6 output contracts."""

from datetime import datetime
from typing import Any

from pydantic import BaseModel


class ArtifactRef(BaseModel):
    """Reference to a named + versioned artifact."""

    name: str
    version: str


class LineageEvent(BaseModel):
    """Base lineage event emitted by P6."""

    event_type: str
    event_time: datetime
    inputs: dict[str, ArtifactRef] | None = None
    outputs: dict[str, ArtifactRef] | None = None
    metadata: dict[str, Any] = {}
