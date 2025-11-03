"""Shared data model definitions."""

from __future__ import annotations

from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field, field_validator


class FacilityMetadata(BaseModel):
    """Facility metadata model."""
    
    code: str
    name: str
    network_id: str
    network_region: str
    description: str
    npi_id: str
    lat: float = Field(alias="latitude")
    lng: float = Field(alias="longitude")
    
    class Config:
        """Pydantic config."""
        populate_by_name = True  # Pydantic v2 uses populate_by_name


class MQTTMessage(BaseModel):
    """MQTT message model."""
    
    event_time: str
    facility_id: str
    power: float
    emissions: float
    longitude: Optional[float] = None
    latitude: Optional[float] = None
    
    @field_validator("event_time")
    @classmethod
    def validate_event_time(cls, v: str) -> str:
        """Validate event time format."""
        try:
            datetime.fromisoformat(v.replace("Z", "+00:00"))
        except ValueError as e:
            raise ValueError(f"Invalid event_time format: {e}")
        return v


class FacilityUpdate(BaseModel):
    """Facility update message model (sent to WebSocket client)."""
    
    type: str = "facility_update"
    data: dict
    
    class Config:
        """Pydantic config."""
        json_schema_extra = {
            "example": {
                "type": "facility_update",
                "data": {
                    "facility_id": "0MREH",
                    "event_time": "2025-10-01T00:00:00+10:00",
                    "power": 125.5,
                    "emissions": 0.0,
                    "longitude": 144.726302,
                    "latitude": -37.661274,
                    "metadata": {
                        "name": "Facility Name",
                        "type": "Solar",
                        "network_region": "VIC1",
                        "fuel_technology": "Solar"
                    }
                }
            }
        }


class ClientSubscribeMessage(BaseModel):
    """Client subscribe message model."""
    
    type: str = "subscribe"
    filter: Optional[dict] = None


class MarketMetricsMessage(BaseModel):
    """Market metrics MQTT message model."""
    
    network_code: str
    metric: str  # "price", "demand", etc.
    event_time: str
    value: float
    
    @field_validator("event_time")
    @classmethod
    def validate_event_time(cls, v: str) -> str:
        """Validate event time format."""
        try:
            datetime.fromisoformat(v.replace("Z", "+00:00"))
        except ValueError as e:
            raise ValueError(f"Invalid event_time format: {e}")
        return v


class MarketUpdate(BaseModel):
    """Market update message model (sent to WebSocket client)."""
    
    type: str = "market_update"
    data: dict
    
    class Config:
        """Pydantic config."""
        json_schema_extra = {
            "example": {
                "type": "market_update",
                "data": {
                    "network_code": "NEM",
                    "metric": "price",
                    "event_time": "2025-10-01T00:00:00+10:00",
                    "value": 25.796
                }
            }
        }

