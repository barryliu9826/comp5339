"""Monolithic dashboard application - merged frontend, backend, and schemas.

This file combines:
- Data models (schemas.py)
- FastAPI backend (backend.py)
- Dash frontend (frontend.py)

Architecture:
- FastAPI runs in a background thread (daemon)
- Dash runs in the main process
- Communication via WebSocket
"""

from __future__ import annotations

# ========== Section 1: Imports & Configuration ==========
import asyncio
import json
import logging
import math
import threading
import time
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Callable, Dict, Optional, Set

import dash
import dash_leaflet as dl
import pandas as pd
import plotly.graph_objects as go
import paho.mqtt.client as mqtt
import websockets
from dash import dcc, html, no_update
from dash.dependencies import Input, Output, State
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from pydantic import BaseModel, Field, field_validator
from starlette.websockets import WebSocketState
from websockets.exceptions import WebSocketException

# Logging configuration
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# ========== Section 1.1: Constants ==========
# NEM network regions
NEM_REGIONS = [
    "QLD1",
    "NSW1",
    "VIC1",
    "SA1",
    "TAS1",
]

# Region display name mapping (used for dropdown, remove "1" suffix)
REGION_DISPLAY_MAP = {
    "QLD1": "QLD",  # Queensland
    "NSW1": "NSW",  # New South Wales
    "VIC1": "VIC",  # Victoria
    "SA1": "SA",    # South Australia
    "TAS1": "TAS",  # Tasmania
}

# Common fuel technology types
FUEL_TYPES = [
    "Solar",
    "Wind",
    "Gas",
    "Coal",
    "Hydro",
    "Battery",
    "Diesel",
    "Unknown",
]

# Australia NEM region center coordinates
AUSTRALIA_CENTER = [-25.2744, 133.7751]  # Australia center
NEM_REGION_CENTER = [-28.0, 145.0]  # NEM region center (more towards the east)

# ========== Section 2: Data Models (Pydantic) ==========
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


# Charts payload models
class LinePoint(BaseModel):
    """Single point for line series."""
    
    t: str
    v: float


class LineSeries(BaseModel):
    """Line series definition."""
    
    name: str
    points: list[LinePoint]


class DonutSlice(BaseModel):
    """Donut slice by fuel technology."""
    
    fuel: str
    power: float


class RegionBarItem(BaseModel):
    """Bar item aggregated by network region."""
    
    region: str
    power: float
    emissions: float


class FacilityBarItem(BaseModel):
    """Bar item for top facilities by power/emissions."""
    
    facility_id: str
    name: str
    power: float
    emissions: float


class ChartsPayload(BaseModel):
    """Composite charts payload for facilities charts."""
    
    generated_at: str
    window_minutes: int
    filters_applied: dict | None = None
    line: list[LineSeries]
    donut: list[DonutSlice]
    bar_by_region: list[RegionBarItem]
    bar_top_facilities: list[FacilityBarItem]


class ChartsUpdate(BaseModel):
    """Charts update envelope for WebSocket push."""
    
    type: str = "charts_update"
    data: ChartsPayload


class ChartsInitial(BaseModel):
    """Initial charts snapshot envelope for WS/REST."""
    
    type: str = "initial_charts"
    data: ChartsPayload

# ========== Section 3: Backend Core Components ==========
class StateManager:
    """Manage facility state (in memory)."""
    
    def __init__(self) -> None:
        """Initialize state manager."""
        self._state: Dict[str, dict] = {}
    
    def update_facility(
        self,
        facility_id: str,
        mqtt_message: MQTTMessage,
        metadata: Optional[dict] = None,
    ) -> dict:
        """
        Update facility state.
        
        Args:
            facility_id: Facility ID
            mqtt_message: MQTT message
            metadata: Facility metadata (optional)
        
        Returns:
            Updated full facility data
        """
        # Build latest data
        latest_data = {
            "event_time": mqtt_message.event_time,
            "power": mqtt_message.power,
            "emissions": mqtt_message.emissions,
            "longitude": mqtt_message.longitude or (metadata.get("longitude") if metadata else None),
            "latitude": mqtt_message.latitude or (metadata.get("latitude") if metadata else None),
        }
        
        # Update state
        if facility_id not in self._state:
            self._state[facility_id] = {
                "latest": latest_data,
                "metadata": metadata or {},
            }
        else:
            self._state[facility_id]["latest"] = latest_data
            if metadata:
                self._state[facility_id]["metadata"].update(metadata)
        
        # Return full data (for WebSocket push)
        return {
            "facility_id": facility_id,
            **latest_data,
            "metadata": self._state[facility_id]["metadata"],
        }
    
    def get_facility(self, facility_id: str) -> Optional[dict]:
        """
        Get the status of a specified facility.
        
        Args:
            facility_id: Facility ID
        
        Returns:
            Facility data dictionary, if not found return None
        """
        facility_state = self._state.get(facility_id)
        if not facility_state:
            return None
        
        return {
            "facility_id": facility_id,
            **facility_state["latest"],
            "metadata": facility_state["metadata"],
        }
    
    def get_all_facilities(self) -> Dict[str, dict]:
        """
        Get the status of all facilities.
        
        Returns:
            All facilities data dictionary
        """
        return {
            facility_id: {
                "facility_id": facility_id,
                **state["latest"],
                "metadata": state["metadata"],
            }
            for facility_id, state in self._state.items()
        }
    
    def clear(self) -> None:
        """Clear all facility state data.
        
        Removes all stored facility data from memory. Useful for resetting
        the state manager or during testing.
        """
        self._state.clear()
        logger.info("State cleared")


class BackendMarketStateManager:
    """Manage market metrics state (in memory) - backend version."""
    
    def __init__(self) -> None:
        """Initialize market state manager."""
        # Storage format: {network_code: {metric: {latest_value, latest_time, history}}}
        self._state: Dict[str, Dict[str, dict]] = {}
        # History data retention count (optional)
        self._max_history: int = 100
    
    def update_metric(
        self,
        network_code: str,
        metric: str,
        event_time: str,
        value: float,
    ) -> dict:
        """
        Update market metrics.
        
        Args:
            network_code: Network code
            metric: Metric type (e.g. "price", "demand")
            event_time: Event time
            value: Metric value
        
        Returns:
            Updated metric data
        """
        if network_code not in self._state:
            self._state[network_code] = {}
        
        # Initialize or update metric
        if metric not in self._state[network_code]:
            self._state[network_code][metric] = {
                "latest_value": value,
                "latest_time": event_time,
                "history": [],
            }
        
        metric_state = self._state[network_code][metric]
        metric_state["latest_value"] = value
        metric_state["latest_time"] = event_time
        
        # Add to history (limit count)
        history = metric_state["history"]
        history.append({"event_time": event_time, "value": value})
        if len(history) > self._max_history:
            history.pop(0)
        
        # Return latest data
        return {
            "network_code": network_code,
            "metric": metric,
            "event_time": event_time,
            "value": value,
        }
    
    def get_metric(self, network_code: str, metric: str) -> Optional[dict]:
        """
        Get specified market metrics.
        
        Args:
            network_code: Network code
            metric: Metric type
        
        Returns:
            Metric data dictionary, if not found return None
        """
        if network_code not in self._state:
            return None
        if metric not in self._state[network_code]:
            return None
        
        metric_data = self._state[network_code][metric]
        return {
            "network_code": network_code,
            "metric": metric,
            "event_time": metric_data["latest_time"],
            "value": metric_data["latest_value"],
        }
    
    def get_all_metrics(self) -> Dict[str, Dict[str, list]]:
        """
        Get all market metrics historical data (for frontend charts).
        
        Returns:
            All market metrics historical data dictionary {network_code: {metric: [{"event_time": str, "value": float}]}}
        """
        result = {}
        for network_code, metrics in self._state.items():
            result[network_code] = {}
            for metric, data in metrics.items():
                history = data.get("history", [])
                if history:
                    result[network_code][metric] = history.copy()
                elif "latest_time" in data and "latest_value" in data:
                    # If no history data but latest value, create list with latest value
                    result[network_code][metric] = [{
                                "event_time": data["latest_time"],
                                "value": data["latest_value"],
                    }]
        return result
    
    def clear(self) -> None:
        """Clear all market metrics state data.
        
        Removes all stored market metrics data from memory, including
        historical data. Useful for resetting the state manager or during testing.
        """
        self._state.clear()
        logger.info("Market state cleared")


class MarketDataProcessor:
    """Process market MQTT messages and update state."""
    
    def __init__(
        self,
        market_state_manager: BackendMarketStateManager,
    ) -> None:
        """
        Initialize market data processor.
        
        Args:
            market_state_manager: Market state manager
        """
        self.market_state_manager = market_state_manager
    
    def process_message(self, mqtt_message: MarketMetricsMessage) -> Optional[dict]:
        """
        Process market MQTT messages.
        
        Args:
            mqtt_message: Market MQTT message
        
        Returns:
            Processed market metrics data (including latest value), if processing fails return None
        """
        if not mqtt_message.network_code or not mqtt_message.metric:
                return None
            
        try:
            return self.market_state_manager.update_metric(
                mqtt_message.network_code,
                mqtt_message.metric,
                mqtt_message.event_time,
                mqtt_message.value,
            )
        except Exception as e:
            logger.error(f"Failed to process market MQTT message: {e}", exc_info=True)
            return None


class MetadataLoader:
    """Load facility metadata (from facilities.csv)."""
    
    def __init__(self, csv_path: str | Path) -> None:
        """
        Initialize metadata loader.
        
        Args:
            csv_path: facilities.csv file path
        """
        self.csv_path = Path(csv_path)
        self._metadata: Dict[str, dict] = {}
        self._load_metadata()
    
    def _load_metadata(self) -> None:
        """Load metadata from CSV file."""
        if not self.csv_path.exists():
            logger.warning(f"Facilities CSV not found: {self.csv_path}")
            return
        
        try:
            df = pd.read_csv(self.csv_path)
            logger.info(f"Loading metadata from {self.csv_path}, found {len(df)} facilities")
            
            # Support code or facility_id columns
            if "facility_id" not in df.columns and "code" in df.columns:
                df["facility_id"] = df["code"]
            
            for _, row in df.iterrows():
                facility_id = str(row.get("facility_id") or row.get("code", ""))
                if not facility_id:
                    continue
                
                try:
                    metadata_dict = row.to_dict()
                    # Handle lat/lng and latitude/longitude compatibility
                    lat_value = metadata_dict.get("latitude") or metadata_dict.get("lat", 0.0)
                    lng_value = metadata_dict.get("longitude") or metadata_dict.get("lng", 0.0)
                    
                    self._metadata[facility_id] = {
                        "name": str(metadata_dict.get("name", facility_id)),
                        "type": str(metadata_dict.get("type", "Unknown")),
                        "network_region": str(metadata_dict.get("network_region", "Unknown")),
                        "fuel_technology": self._extract_fuel_technology(metadata_dict),
                        "latitude": float(lat_value),
                        "longitude": float(lng_value),
                        "network_id": str(metadata_dict.get("network_id", "NEM")),
                        "description": str(metadata_dict.get("description", "")),
                    }
                except Exception as e:
                    logger.warning(f"Failed to load metadata for facility {facility_id}: {e}")
            
            logger.info(f"Loaded metadata for {len(self._metadata)} facilities")
        except Exception as e:
            logger.error(f"Failed to load metadata from {self.csv_path}: {e}")
            raise
    
    def _extract_fuel_technology(self, metadata_dict: dict) -> str:
        """Extract fuel technology type from description or type."""
        text = (str(metadata_dict.get("description", "")) + " " + 
                str(metadata_dict.get("name", ""))).lower()
        
        fuel_keywords = {
            "solar": ["solar", "photovoltaic", "pv"],
            "wind": ["wind", "wind farm"],
            "gas": ["gas", "natural gas", "lng"],
            "coal": ["coal", "thermal"],
            "hydro": ["hydro", "water"],
            "battery": ["battery", "storage"],
            "diesel": ["diesel"],
        }
        
        for fuel, keywords in fuel_keywords.items():
            if any(keyword in text for keyword in keywords):
                return fuel.capitalize()
        
        return "Unknown"
    
    def get_metadata(self, facility_id: str) -> Optional[dict]:
        """
        Get the metadata of a specified facility.
        
        Args:
            facility_id: Facility ID
        
        Returns:
            Metadata dictionary, if not found return None
        """
        return self._metadata.get(facility_id)
    
    def get_all_metadata(self) -> Dict[str, dict]:
        """
        Get all facility metadata.
        
        Returns:
            All facility metadata dictionary
        """
        return self._metadata.copy()


class DataProcessor:
    """Process MQTT messages and merge metadata."""
    
    def __init__(
        self,
        state_manager: StateManager,
        metadata_loader: MetadataLoader,
    ) -> None:
        """
        Initialize data processor.
        
        Args:
            state_manager: State manager
            metadata_loader: Metadata loader
        """
        self.state_manager = state_manager
        self.metadata_loader = metadata_loader
    
    def process_message(self, mqtt_message: MQTTMessage) -> Optional[dict]:
        """
        Process MQTT messages.
        
        Args:
            mqtt_message: MQTT message
        
        Returns:
            Processed facility data (including metadata), if processing fails return None
        """
        if not mqtt_message.facility_id:
            return None
        
        try:
            # Get metadata, if not found use default values
            metadata = self.metadata_loader.get_metadata(mqtt_message.facility_id)
            if not metadata:
                metadata = {
                    "name": mqtt_message.facility_id,
                    "type": "Unknown",
                    "network_region": "Unknown",
                    "fuel_technology": "Unknown",
                    "latitude": mqtt_message.latitude or 0.0,
                    "longitude": mqtt_message.longitude or 0.0,
                    "network_id": "NEM",
                    "description": "",
                }
            
            return self.state_manager.update_facility(
                mqtt_message.facility_id,
                mqtt_message,
                metadata,
            )
        except Exception as e:
            logger.error(f"Failed to process MQTT message: {e}", exc_info=True)
            return None

# ========== Section 3.1: MQTT Subscriber Service ==========
class MQTTSubscriber:
    """MQTT subscriber service."""
    
    def __init__(
        self,
        broker_host: str = "broker.hivemq.com",
        broker_port: int = 1883,
        topic: str = "a02/facility_metrics/v1/stream",
        qos: int = 0,
        message_callback: Optional[Callable[[MQTTMessage], None]] = None,
    ) -> None:
        """
        Initialize MQTT subscriber service.
        
        Args:
            broker_host: MQTT broker host
            broker_port: MQTT broker port
            topic: Subscription topic
            qos: QoS level
            message_callback: Message callback function
        """
        self.broker_host = broker_host
        self.broker_port = broker_port
        self.topic = topic
        self.qos = qos
        self.message_callback = message_callback
        self.client: Optional[mqtt.Client] = None
        self._connected = False
    
    def _on_connect(self, client: mqtt.Client, userdata: dict, flags: dict, rc: int) -> None:
        """MQTT connection callback."""
        if rc == 0:
            logger.info(f"Connected to MQTT broker {self.broker_host}:{self.broker_port}")
            self._connected = True
            # Subscribe to topic
            client.subscribe(self.topic, self.qos)
            logger.info(f"Subscribed to topic: {self.topic}")
        else:
            logger.error(f"Failed to connect to MQTT broker, rc={rc}")
            self._connected = False
    
    def _on_disconnect(self, client: mqtt.Client, userdata: dict, rc: int) -> None:
        """MQTT disconnect callback."""
        logger.warning(f"Disconnected from MQTT broker, rc={rc}")
        self._connected = False
    
    def _on_message(self, client: mqtt.Client, userdata: dict, msg: mqtt.MQTTMessage) -> None:
        """MQTT message receive callback."""
        try:
            payload_dict = json.loads(msg.payload.decode("utf-8"))
            mqtt_message = MQTTMessage(**payload_dict)
            if self.message_callback:
                self.message_callback(mqtt_message)
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse MQTT message JSON: {e}")
        except Exception as e:
            logger.error(f"Error processing MQTT message: {e}", exc_info=True)
    
    def start(self) -> None:
        """Start MQTT subscriber service."""
        if self.client is not None:
            logger.warning("MQTT client already started")
            return
        
        # Create MQTT client
        self.client = mqtt.Client(
            client_id=f"dashboard_subscriber_{id(self)}",
            protocol=mqtt.MQTTv311,
        )
        
        # Set callbacks
        self.client.on_connect = self._on_connect
        self.client.on_disconnect = self._on_disconnect
        self.client.on_message = self._on_message
        
        # Connect to broker
        try:
            rc = self.client.connect(self.broker_host, self.broker_port, keepalive=60)
            if rc != 0:
                raise RuntimeError(f"MQTT connect failed, rc={rc}")
            
            # Start network loop (non-blocking)
            self.client.loop_start()
            logger.info("MQTT subscriber started")
        except Exception as e:
            logger.error(f"Failed to start MQTT subscriber: {e}")
            raise
    
    def stop(self) -> None:
        """Stop MQTT subscriber service."""
        if self.client is None:
            return
        
        self.client.loop_stop()
        self.client.disconnect()
        self._connected = False
        self.client = None
        logger.info("MQTT subscriber stopped")
    
    def is_connected(self) -> bool:
        """Check if MQTT client is connected to broker.
        
        Returns:
            True if client exists and is connected, False otherwise
        """
        return self._connected and self.client is not None and self.client.is_connected()

# ========== Section 3.2: WebSocket Manager ==========
class WebSocketManager:
    """Manage WebSocket connections and broadcast messages."""
    
    def __init__(self) -> None:
        """Initialize WebSocket manager."""
        self.active_connections: Set[WebSocket] = set()
    
    async def connect(self, websocket: WebSocket) -> None:
        """
        Accept WebSocket connection.
        
        Args:
            websocket: WebSocket connection object
        """
        await websocket.accept()
        self.active_connections.add(websocket)
        logger.info(f"WebSocket client connected. Total connections: {len(self.active_connections)}")
    
    def disconnect(self, websocket: WebSocket) -> None:
        """
        Remove WebSocket connection.
        
        Args:
            websocket: WebSocket connection object
        """
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)
            logger.info(f"WebSocket client disconnected. Total connections: {len(self.active_connections)}")
    
    async def send_personal_message(self, message: dict, websocket: WebSocket) -> None:
        """
        Send message to a single client.
        
        Args:
            message: Message dictionary
            websocket: WebSocket connection object
        """
        try:
            if websocket.client_state == WebSocketState.CONNECTED:
                await websocket.send_json(message)
            else:
                logger.warning("WebSocket not connected, cannot send message")
                self.disconnect(websocket)
        except Exception as e:
            logger.error(f"Failed to send personal message: {e}")
            self.disconnect(websocket)
    
    async def broadcast(self, message: dict) -> None:
        """
        Broadcast message to all connected clients.
        
        Args:
            message: Message dictionary
        """
        if not self.active_connections:
            return
        
        disconnected: Set[WebSocket] = set()
        for connection in self.active_connections:
            try:
                if connection.client_state == WebSocketState.CONNECTED:
                    await connection.send_json(message)
                else:
                    disconnected.add(connection)
            except Exception as e:
                logger.warning(f"Failed to broadcast to client: {e}")
                disconnected.add(connection)
        
        for connection in disconnected:
            self.disconnect(connection)

# ========== Section 3.3: Charts Aggregator ==========
class ChartsAggregator:
    """Aggregate facilities data to chart-ready payloads."""
    
    def __init__(self, state: StateManager, window_minutes: int = 30) -> None:
        """Initialize aggregator with ring buffers for time series.
        
        Args:
            state: Facilities state manager
            window_minutes: Sliding window length for line charts (default: 30 minutes)
        """
        self._state = state
        self._window_minutes = window_minutes  # Time window for time series data
        self._line_total_power: list[tuple[str, float]] = []  # Ring buffer for total power time series
        self._line_total_emissions: list[tuple[str, float]] = []  # Ring buffer for total emissions time series
        self._top_n: int = 10  # Number of top facilities to display in bar chart
    
    def _trim(self, series: list[tuple[str, float]]) -> None:
        """Trim time series to the configured time window.
        
        Removes data points older than the configured window_minutes from
        the time series buffer. This implements a sliding window approach
        to keep only recent data in memory.
        
        Args:
            series: Time series list of (timestamp, value) tuples to trim in-place
        """
        if not series:
            return
        try:
            # Calculate cutoff timestamp (current time - window duration)
            cutoff = datetime.utcnow().timestamp() - self._window_minutes * 60
            kept: list[tuple[str, float]] = []
            for t, v in series:
                try:
                    # Parse ISO format timestamp to Unix timestamp
                    ts = datetime.fromisoformat(t.replace("Z", "+00:00")).timestamp()
                except Exception:
                    # If parsing fails, use cutoff to exclude invalid timestamps
                    ts = cutoff
                # Keep only data points within the time window
                if ts >= cutoff:
                    kept.append((t, v))
            # Replace series content with trimmed data
            series[:] = kept
        except Exception:
            # On any error, clear the series to prevent corrupted data
            series.clear()
    
    def build_payload(self, filters: Optional[dict] = None) -> dict:
        """Build composite charts payload based on current facilities snapshot.
        
        Aggregates facility data into chart-ready format including:
        - Time series line charts (total power and emissions)
        - Donut chart (power by fuel technology)
        - Bar charts (by region and top facilities)
        
        Args:
            filters: Optional filter dictionary with keys:
                - regions: List of network regions to include
                - fuel_types: List of fuel technology types to include
        
        Returns:
            Dictionary containing chart data payload
        """
        facilities = self._state.get_all_facilities()
        # Apply filters if provided
        if filters:
            regions = filters.get("regions") or []
            fuels = filters.get("fuel_types") or []
            if regions or fuels:
                filtered: Dict[str, dict] = {}
                for fid, f in facilities.items():
                    md = f.get("metadata", {})
                    # Filter by network region
                    if regions and md.get("network_region") not in regions:
                        continue
                    # Filter by fuel technology
                    if fuels and md.get("fuel_technology") not in fuels:
                        continue
                    filtered[fid] = f
                facilities = filtered
        # Initialize aggregation variables
        total_power = 0.0
        total_emissions = 0.0
        by_fuel: Dict[str, float] = {}  # Aggregate power by fuel technology
        by_region: Dict[str, dict] = {}  # Aggregate power and emissions by region
        # Collect facilities for Top-N ranking (id, name, power, emissions)
        facilities_list: list[tuple[str, str, float, float]] = []
        now_iso = datetime.utcnow().isoformat() + "Z"  # Current timestamp in ISO format
        
        # Iterate through all facilities and aggregate data
        for fid, f in facilities.items():
            # Extract power and emissions values (handle None/NaN)
            p = float(f.get("power", 0.0) or 0.0)
            e = float(f.get("emissions", 0.0) or 0.0)
            md = f.get("metadata", {})
            fuel = str(md.get("fuel_technology", "Unknown"))
            region = str(md.get("network_region", "Unknown"))
            name = str(md.get("name", fid))
            
            # Aggregate totals
            total_power += p
            total_emissions += e
            
            # Aggregate by fuel technology (for donut chart)
            by_fuel[fuel] = by_fuel.get(fuel, 0.0) + p
            
            # Aggregate by region (for region bar chart)
            if region not in by_region:
                by_region[region] = {"power": 0.0, "emissions": 0.0}
            by_region[region]["power"] += p
            by_region[region]["emissions"] += e
            
            # Collect facility data for Top-N ranking
            facilities_list.append((fid, name, p, e))
        
        # Append current totals to time series buffers and trim old data
        self._line_total_power.append((now_iso, total_power))
        self._line_total_emissions.append((now_iso, total_emissions))
        self._trim(self._line_total_power)
        self._trim(self._line_total_emissions)
        # Build line series for time series charts (power and emissions over time)
        line = [
            {
                "name": "total_power",
                "points": [{"t": t, "v": v} for (t, v) in self._line_total_power],
            },
            {
                "name": "total_emissions",
                "points": [{"t": t, "v": v} for (t, v) in self._line_total_emissions],
            },
        ]
        
        # Build donut chart data (power distribution by fuel technology)
        donut = [{"fuel": k, "power": v} for k, v in sorted(by_fuel.items())]
        
        # Build bar chart data (power and emissions aggregated by region)
        bar_by_region = [
            {"region": r, "power": vals["power"], "emissions": vals["emissions"]}
            for r, vals in sorted(by_region.items())
        ]
        
        # Build Top-N facilities bar chart (sorted by power descending)
        facilities_list.sort(key=lambda x: x[2], reverse=True)  # Sort by power (index 2)
        top = facilities_list[: self._top_n]  # Get top N facilities
        bar_top_facilities = [
            {"facility_id": fid, "name": name, "power": p, "emissions": e}
            for fid, name, p, e in top
        ]
        # Compose payload
        payload = {
            "generated_at": now_iso,
            "window_minutes": self._window_minutes,
            "filters_applied": filters or {},
            "line": line,
            "donut": donut,
            "bar_by_region": bar_by_region,
            "bar_top_facilities": bar_top_facilities,
        }
        return payload

# ========== Section 4: FastAPI Application ==========
# Global objects for backend
backend_state_manager = StateManager()
backend_market_state_manager = BackendMarketStateManager()
metadata_loader: MetadataLoader | None = None
data_processor: DataProcessor | None = None
market_data_processor: MarketDataProcessor | None = None
websocket_manager = WebSocketManager()
mqtt_subscriber: MQTTSubscriber | None = None
market_mqtt_subscriber: MQTTSubscriber | None = None
charts_aggregator: ChartsAggregator | None = None
_global_event_loop: asyncio.AbstractEventLoop | None = None
_charts_task: asyncio.Task | None = None
_charts_filters: dict | None = None


# ========== Section 4.1: Backend Initialization Functions ==========
def setup_metadata_loader() -> MetadataLoader:
    """Setup metadata loader."""
    project_root = Path(__file__).parent.parent
    default_csv_path = project_root / "data" / "csv_output" / "facilities.csv"
    
    if not default_csv_path.exists():
        logger.warning(f"Facilities CSV not found at {default_csv_path}")
        # Try relative path (from current working directory)
        alt_path = Path("data/csv_output/facilities.csv")
        if alt_path.exists():
            default_csv_path = alt_path
            logger.info(f"Using facilities CSV from relative path: {default_csv_path}")
        else:
            logger.error("Cannot find facilities.csv, metadata loading may fail")
    
    return MetadataLoader(default_csv_path)


def setup_mqtt_subscriber() -> MQTTSubscriber:
    """Setup MQTT subscriber service (facility data)."""
    
    def on_message_callback(mqtt_message: MQTTMessage) -> None:
        """MQTT message callback."""
        global _global_event_loop
        
        if data_processor is None:
            logger.error("Data processor not initialized")
            return
        
        # Process message
        facility_data = data_processor.process_message(mqtt_message)
        
        if facility_data:
            # Build WebSocket message
            ws_message: FacilityUpdate = FacilityUpdate(
                type="facility_update",
                data=facility_data,
            )
            
            # Call asynchronous function from synchronous context (MQTT callback executed in paho-mqtt thread)
            if _global_event_loop is not None and _global_event_loop.is_running():
                try:
                    asyncio.run_coroutine_threadsafe(
                        websocket_manager.broadcast(ws_message.model_dump()),
                        _global_event_loop
                    )
                except Exception as e:
                    logger.error(f"Failed to schedule broadcast task: {e}")
    
    # Create MQTT subscriber
    subscriber = MQTTSubscriber(
        broker_host="broker.hivemq.com",
        broker_port=1883,
        topic="a02/facility_metrics/v1/stream",
        qos=0,
        message_callback=on_message_callback,
    )
    
    return subscriber


def setup_market_mqtt_subscriber() -> MQTTSubscriber:
    """Setup market data MQTT subscriber service."""
    
    def on_market_message_callback(message_dict: dict) -> None:
        """Market MQTT message callback."""
        global _global_event_loop
        
        if market_data_processor is None:
            logger.error("Market data processor not initialized")
            return
        
        try:
            # Parse to MarketMetricsMessage
            mqtt_message = MarketMetricsMessage(**message_dict)
            
            # Process message
            metric_data = market_data_processor.process_message(mqtt_message)
            
            if metric_data:
                # Build WebSocket message
                ws_message: MarketUpdate = MarketUpdate(
                    type="market_update",
                    data=metric_data,
                )
                
                # Call asynchronous function from synchronous context
                if _global_event_loop is not None and _global_event_loop.is_running():
                    try:
                        asyncio.run_coroutine_threadsafe(
                            websocket_manager.broadcast(ws_message.model_dump()),
                            _global_event_loop
                        )
                    except Exception as e:
                        logger.error(f"Failed to schedule market broadcast task: {e}")
        except Exception as e:
            logger.error(f"Failed to process market message: {e}", exc_info=True)
    
    # Create market MQTT subscriber (reuse MQTTSubscriber, but use different message format processing)
    # Need to customize _on_message to process market message format
    class MarketMQTTSubscriber(MQTTSubscriber):
        """Market MQTT subscriber (inherit and override message processing)."""
        
        def _on_message(self, client: mqtt.Client, userdata: dict, msg: mqtt.MQTTMessage) -> None:
            """MQTT message receive callback (market data format)."""
            try:
                payload_dict = json.loads(msg.payload.decode("utf-8"))
                if self.message_callback:
                    self.message_callback(payload_dict)
            except json.JSONDecodeError as e:
                logger.error(f"Failed to parse market MQTT message JSON: {e}")
            except Exception as e:
                logger.error(f"Error processing market MQTT message: {e}", exc_info=True)
    
    subscriber = MarketMQTTSubscriber(
        broker_host="broker.hivemq.com",
        broker_port=1883,
        topic="a02/market_metrics/v1/stream",
        qos=0,
        message_callback=on_market_message_callback,
    )
    
    return subscriber


# ========== Section 4.2: FastAPI Application Lifecycle ==========
@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifecycle management."""
    global metadata_loader, data_processor, market_data_processor, mqtt_subscriber, market_mqtt_subscriber, _global_event_loop
    global charts_aggregator, _charts_task
    
    # Initialize when starting
    logger.info("Starting FastAPI application...")
    
    _global_event_loop = asyncio.get_running_loop()
    metadata_loader = setup_metadata_loader()
    data_processor = DataProcessor(backend_state_manager, metadata_loader)
    market_data_processor = MarketDataProcessor(backend_market_state_manager)
    
    # Start MQTT subscriber service
    mqtt_subscriber = setup_mqtt_subscriber()
    mqtt_subscriber.start()
    market_mqtt_subscriber = setup_market_mqtt_subscriber()
    market_mqtt_subscriber.start()
    
    # Start charts aggregator task
    charts_aggregator = ChartsAggregator(backend_state_manager, window_minutes=30)
    async def charts_loop() -> None:
        """Periodic aggregation and WebSocket broadcast for charts.
        
        Continuously aggregates facility data into chart payloads and
        broadcasts updates to all connected WebSocket clients every second.
        This ensures real-time chart updates in the frontend.
        """
        while True:
            # Build chart payload with current filters
            payload = charts_aggregator.build_payload(filters=_charts_filters or {})
            message = {"type": "charts_update", "data": payload}
            # Broadcast to all connected WebSocket clients
            await websocket_manager.broadcast(message)
            # Wait 1 second before next update
            await asyncio.sleep(1)
    _charts_task = asyncio.create_task(charts_loop())
    
    logger.info("FastAPI application started")
    
    yield
    
    # Clean up when closing
    logger.info("Shutting down FastAPI application...")
    if market_mqtt_subscriber:
        market_mqtt_subscriber.stop()
    if mqtt_subscriber:
        mqtt_subscriber.stop()
    if _charts_task:
        _charts_task.cancel()
        try:
            await _charts_task
        except asyncio.CancelledError:
            # CancelledError is expected when cancelling a task, ignore it
            pass
        except Exception as e:
            # Log any other unexpected errors during task cancellation
            logger.warning(f"Error while cancelling charts task: {e}")
    _global_event_loop = None


# ========== Section 4.3: FastAPI App Instance ==========
fastapi_app = FastAPI(
    title="Electricity Dashboard Backend",
    description="FastAPI backend for real-time electricity facility monitoring",
    version="1.0.0",
    lifespan=lifespan,
)


# ========== Section 4.4: FastAPI Routes ==========
@fastapi_app.get("/")
async def root() -> dict:
    """Root endpoint for API health check.
    
    Returns:
        Dictionary containing API status and WebSocket endpoint information
    """
    return {
        "message": "Electricity Dashboard Backend API",
        "status": "running",
        "websocket_endpoint": "/ws/realtime",
    }


@fastapi_app.get("/api/facilities")
async def get_all_facilities() -> dict:
    """
    Get all facilities current status.
    
    Returns:
        All facilities status dictionary
    """
    facilities = backend_state_manager.get_all_facilities()
    return {
        "facilities": facilities,
        "count": len(facilities),
    }


@fastapi_app.get("/api/facilities/{facility_id}")
async def get_facility(facility_id: str) -> dict:
    """
    Get the status of a specified facility.
    
    Args:
        facility_id: Facility ID
    
    Returns:
        Facility status dictionary
    """
    facility = backend_state_manager.get_facility(facility_id)
    if not facility:
        return {"error": "Facility not found"}
    return facility


@fastapi_app.get("/api/metadata")
async def get_metadata() -> dict:
    """
    Get all facility metadata.
    
    Returns:
        All facility metadata dictionary
    """
    if metadata_loader is None:
        return {"error": "Metadata loader not initialized"}
    
    metadata = metadata_loader.get_all_metadata()
    return {
        "metadata": metadata,
        "count": len(metadata),
    }


@fastapi_app.get("/api/market/latest")
async def get_latest_market_metrics() -> dict:
    """
    Get latest market metrics.
    
    Returns:
        All market metrics latest values
    """
    metrics = backend_market_state_manager.get_all_metrics()
    return {
        "metrics": metrics,
        "count": sum(len(network_metrics) for network_metrics in metrics.values()),
    }


@fastapi_app.get("/api/market/{network_code}/{metric}")
async def get_market_metric(network_code: str, metric: str) -> dict:
    """
    Get latest value of a specified network and metric.
    
    Args:
        network_code: Network code (e.g. "NEM")
        metric: Metric type (e.g. "price", "demand")
    
    Returns:
        Metric data dictionary
    """
    metric_data = backend_market_state_manager.get_metric(network_code, metric)
    if not metric_data:
        return {"error": f"Metric not found: {network_code}/{metric}"}
    return metric_data


@fastapi_app.get("/api/charts/snapshot")
async def get_charts_snapshot() -> dict:
    """Return current charts snapshot for initialization."""
    if charts_aggregator is None:
        return {"error": "Charts aggregator not initialized"}
    payload = charts_aggregator.build_payload(filters=_charts_filters or {})
    return {"type": "initial_charts", "data": payload}


@fastapi_app.websocket("/ws/realtime")
async def websocket_endpoint(websocket: WebSocket) -> None:
    """
    WebSocket endpoint, for real-time data push.
    
    Args:
        websocket: WebSocket connection object
    """
    await websocket_manager.connect(websocket)
    
    try:
        # Send initial data (all facilities current status)
        facilities = backend_state_manager.get_all_facilities()
        initial_message: FacilityUpdate = FacilityUpdate(
            type="initial_data",
            data={"facilities": facilities},
        )
        await websocket_manager.send_personal_message(initial_message.model_dump(), websocket)
        
        # Send initial market data
        market_metrics = backend_market_state_manager.get_all_metrics()
        market_initial_message: MarketUpdate = MarketUpdate(
            type="initial_market_data",
            data={"metrics": market_metrics},
        )
        await websocket_manager.send_personal_message(market_initial_message.model_dump(), websocket)
        
        # Send initial charts snapshot
        if charts_aggregator is not None:
            charts_payload = charts_aggregator.build_payload(filters=_charts_filters or {})
            await websocket_manager.send_personal_message({"type": "initial_charts", "data": charts_payload}, websocket)
        
        # Keep connected and process client messages
        while True:
            try:
                await websocket.receive_json()
            except WebSocketDisconnect:
                break
            except Exception as e:
                logger.error(f"Error handling WebSocket message: {e}")
                break
    
    except WebSocketDisconnect:
        pass
    except Exception as e:
        logger.error(f"WebSocket error: {e}")
    finally:
        websocket_manager.disconnect(websocket)


# ========== Section 5: Frontend Core Components ==========
class FrontendStateManager:
    """Manage frontend state (facility data, filters, display mode)."""
    
    def __init__(self) -> None:
        """Initialize frontend state manager."""
        self.facilities: Dict[str, dict] = {}
        self.filters: Dict[str, list] = {
            "regions": [],
            "fuel_types": [],
        }
        self.selected_facility: Optional[str] = None
    
    def get_all_facilities(self) -> Dict[str, dict]:
        """
        Get all facilities data (apply filters).
        
        Returns:
            Filtered facilities data dictionary
        """
        if not self.filters["regions"] and not self.filters["fuel_types"]:
            return self.facilities.copy()
        
        filtered = {}
        for facility_id, facility_data in self.facilities.items():
            metadata = facility_data.get("metadata", {})
            
            # Region filter
            if self.filters["regions"]:
                region = metadata.get("network_region", "")
                if region not in self.filters["regions"]:
                    continue
            
            # Fuel type filter
            if self.filters["fuel_types"]:
                fuel_type = metadata.get("fuel_technology", "")
                if fuel_type not in self.filters["fuel_types"]:
                    continue
            
            filtered[facility_id] = facility_data
        
        return filtered
    
    def set_filters(self, regions: Optional[list] = None, fuel_types: Optional[list] = None) -> None:
        """
        Set filters.
        
        Args:
            regions: Network region list
            fuel_types: Fuel type list
        """
        if regions is not None:
            self.filters["regions"] = regions
        if fuel_types is not None:
            self.filters["fuel_types"] = fuel_types
    
    def set_selected_facility(self, facility_id: Optional[str]) -> None:
        """
        Set currently selected facility.
        
        Args:
            facility_id: Facility ID
        """
        self.selected_facility = facility_id
    
    def update_facility(self, facility_data: dict) -> None:
        """
        Update single facility data.
        
        Args:
            facility_data: Facility data dictionary, must contain facility_id
        """
        facility_id = facility_data.get("facility_id")
        if not facility_id:
            logger.warning("Missing facility_id in facility_data")
            return
        
        self.facilities[facility_id] = facility_data
    
    def update_facilities_batch(self, facilities: Dict[str, dict]) -> None:
        """
        Batch update facility data.
        
        Args:
            facilities: Facility data dictionary {facility_id: facility_data}
        """
        self.facilities.update(facilities)
    
    def get_facility(self, facility_id: str) -> Optional[dict]:
        """
        Get data for specified facility.
        
        Args:
            facility_id: Facility ID
        
        Returns:
            Facility data dictionary, None if not found
        """
        return self.facilities.get(facility_id)


class FrontendMarketStateManager:
    """Manage frontend market metrics state (supports infinite scrolling time series)."""
    
    def __init__(self, max_history: Optional[int] = None) -> None:
        """
        Initialize market state manager.
        
        Args:
            max_history: Maximum history data count, None means infinite scrolling (recommended for real-time charts)
        """
        # Storage format: {network_code: {metric: [{"event_time": str, "value": float}]}}
        self._metrics: Dict[str, Dict[str, list]] = {}
        # Keep recent history data count, None means no limit (infinite scrolling)
        self._max_history: Optional[int] = max_history
    
    def update_metric(self, network_code: str, metric: str, event_time: str, value: float) -> None:
        """
        Update market metrics (supports infinite scrolling append).
        
        Args:
            network_code: Network code
            metric: Metric type (e.g. "price", "demand")
            event_time: Event time
            value: Metric value
        """
        if network_code not in self._metrics:
            self._metrics[network_code] = {}
        
        if metric not in self._metrics[network_code]:
            self._metrics[network_code][metric] = []
        
        # Add to history (real-time append)
        history = self._metrics[network_code][metric]
        history.append({"event_time": event_time, "value": value})
        
        # Only limit history data count when max_history is not None
        if self._max_history is not None and len(history) > self._max_history:
            history.pop(0)
    
    def update_metrics_batch(self, metrics: Dict[str, Dict[str, dict | list]]) -> None:
        """
        Batch update market metrics.
        
        Args:
            metrics: Market metrics dictionary {network_code: {metric: {event_time, value}} or [{"event_time", "value"}]}
            If metric_data is a dictionary, update as a single data point
            If metric_data is a list, update as historical data batch
        """
        if not metrics:
            logger.warning("update_metrics_batch received empty metrics")
            return
        
        count = 0
        for network_code, network_metrics in metrics.items():
            if not network_metrics:
                continue
            for metric, metric_data in network_metrics.items():
                if isinstance(metric_data, list):
                    # If list, batch update historical data
                    if metric_data:
                        for data_point in metric_data:
                            if isinstance(data_point, dict):
                                event_time = data_point.get("event_time")
                                value = data_point.get("value")
                                if event_time and value is not None:
                                    self.update_metric(network_code, metric, event_time, value)
                                    count += 1
                elif isinstance(metric_data, dict):
                    # If dictionary, update as a single data point
                    event_time = metric_data.get("event_time")
                    value = metric_data.get("value")
                    if event_time and value is not None:
                        self.update_metric(network_code, metric, event_time, value)
                        count += 1
                else:
                    logger.warning(f"Invalid metric_data format for {network_code}/{metric}: {type(metric_data)}")
        
        if count == 0:
            logger.warning(f"update_metrics_batch processed {len(metrics)} networks but updated 0 metrics")
    
    def get_metric_history(self, network_code: str, metric: str) -> list:
        """
        Get historical data for specified metric.
        
        Args:
            network_code: Network code
            metric: Metric type
        
        Returns:
            Historical data list
        """
        return self._metrics.get(network_code, {}).get(metric, []).copy()
    
    def get_latest_value(self, network_code: str, metric: str) -> Optional[float]:
        """
        Get latest value for specified metric.
        
        Args:
            network_code: Network code
            metric: Metric type
        
        Returns:
            Latest value, None if not found
        """
        history = self.get_metric_history(network_code, metric)
        if not history:
            return None
        return history[-1]["value"]
    
    def get_all_metrics(self) -> Dict[str, Dict[str, list]]:
        """
        Get all market metrics historical data.
        
        Returns:
            All market metrics historical data
        """
        return self._metrics.copy()


class WebSocketClient:
    """WebSocket client, connect to FastAPI backend."""
    
    def __init__(
        self,
        uri: str = "ws://localhost:8000/ws/realtime",
        message_callback: Optional[Callable[[dict], None]] = None,
    ) -> None:
        """
        Initialize WebSocket client.
        
        Args:
            uri: WebSocket server URI
            message_callback: Message callback function
        """
        self.uri = uri
        self.message_callback = message_callback
        self.websocket: Optional[websockets.WebSocketClientProtocol] = None
        self._running = False
        self._loop: Optional[asyncio.AbstractEventLoop] = None
    
    async def connect(self) -> None:
        """Connect to WebSocket server."""
        try:
            self.websocket = await websockets.connect(self.uri)
            logger.info(f"Connected to WebSocket server: {self.uri}")
        except Exception as e:
            logger.error(f"Failed to connect to WebSocket server: {e}")
            raise
    
    async def disconnect(self) -> None:
        """Disconnect from WebSocket server."""
        if self.websocket:
            await self.websocket.close()
            logger.info("Disconnected from WebSocket server")
    
    async def send_message(self, message: dict) -> None:
        """
        Send message to server.
        
        Args:
            message: Message dictionary
        """
        if not self.websocket:
            logger.warning("WebSocket not connected")
            return
        
        try:
            await self.websocket.send(json.dumps(message))
        except Exception as e:
            logger.error(f"Failed to send message: {e}")
    
    async def listen(self) -> None:
        """Listen to WebSocket messages."""
        if not self.websocket:
            logger.error("WebSocket not connected")
            return
        
        self._running = True
        try:
            while self._running:
                try:
                    message = await self.websocket.recv()
                    data = json.loads(message)
                    if self.message_callback:
                        self.message_callback(data)
                except WebSocketException as e:
                    logger.error(f"WebSocket error: {e}")
                    break
                except Exception as e:
                    logger.error(f"Error processing message: {e}")
                    break
        finally:
            self._running = False
    
    def stop(self) -> None:
        """Stop WebSocket client listening loop.
        
        Sets the running flag to False, which will cause the listen loop
        to exit gracefully on the next iteration.
        """
        self._running = False

# ========== Section 6: Frontend UI Components ==========
def create_market_chart(network_code: str = "NEM", metric: str = "price", market_state_manager: FrontendMarketStateManager = None) -> dcc.Graph:
    """
    Create real-time scrolling time series chart component (fixed window: latest 50 data points).
    
    Args:
        network_code: Network code
        metric: Metric type (e.g. "price", "demand")
        market_state_manager: Frontend market state manager instance
    
    Returns:
        Dash Graph component (fixed window display latest 50 data points)
    """
    if market_state_manager is None:
        # Fallback: return empty chart
        fig = go.Figure()
        fig.add_annotation(
            text="Market state manager not initialized",
            xref="paper",
            yref="paper",
            x=0.5,
            y=0.5,
            showarrow=False,
            font=dict(size=12),
        )
        fig.update_layout(
            title=f"{metric.capitalize()} - {network_code}",
            xaxis_title="Time",
            yaxis_title=f"{metric.capitalize()}",
            height=400,
        )
        return dcc.Graph(id=f"market-chart-{network_code}-{metric}", figure=fig)
    
    history = market_state_manager.get_metric_history(network_code, metric)
    
    if not history:
        # If no data, display empty chart (wait for data)
        fig = go.Figure()
        fig.add_annotation(
            text="Waiting for data...<br><br>Please ensure:<br>1. MQTT publisher is running<br>2. Backend is connected to MQTT broker<br>3. Market data is being published",
            xref="paper",
            yref="paper",
            x=0.5,
            y=0.5,
            showarrow=False,
            font=dict(size=12),
        )
        fig.update_layout(
            title=f"{metric.capitalize()} - {network_code}",
            xaxis_title="Time",
            yaxis_title=f"{metric.capitalize()}",
            height=400,
            xaxis=dict(type="date", autorange=True),
            yaxis=dict(autorange=True),
        )
        return dcc.Graph(id=f"market-chart-{network_code}-{metric}", figure=fig)
    
    # Fixed window mode: only display latest 50 data points for performance
    # This prevents the chart from becoming too cluttered with historical data
    max_display_points = 50
    display_history = history[-max_display_points:] if len(history) > max_display_points else history
    
    # Extract time and value (only display latest 50 points)
    # Filter out invalid values (NaN, None, etc.) to prevent chart rendering errors
    valid_data = [
        (item["event_time"], item["value"])
        for item in display_history
        if item.get("value") is not None 
        and not (isinstance(item["value"], float) and math.isnan(item["value"]))
    ]
    
    # De-duplicate and aggregate by timestamp to avoid multiple traces caused by repeated points
    # If multiple values exist for the same timestamp, average them
    if valid_data:
        aggregated: dict[str, list[float]] = {}
        for ts, val in valid_data:
            aggregated.setdefault(ts, []).append(float(val))
        # Average values per timestamp and sort by time for proper time series display
        valid_data = sorted(
            [(ts, sum(vals) / len(vals)) for ts, vals in aggregated.items()],
            key=lambda x: x[0],
        )
    
    if not valid_data:
        fig = go.Figure()
        fig.add_annotation(
            text="Data contains invalid values<br>Please check data source",
            xref="paper",
            yref="paper",
            x=0.5,
            y=0.5,
            showarrow=False,
            font=dict(size=14),
        )
        fig.update_layout(
            title=f"{metric.capitalize()} - {network_code} (Data Error)",
            xaxis_title="Time",
            yaxis_title=f"{metric.capitalize()}",
            height=400,
        )
        return dcc.Graph(id=f"market-chart-{network_code}-{metric}", figure=fig)
    
    times = [item[0] for item in valid_data]
    values = [item[1] for item in valid_data]
    
    # Create time series line chart
    # Format time string to ISO format (Plotly requirement for date axis)
    formatted_times = []
    for time_str in times:
        try:
            # Convert ISO format with Z to +00:00 for datetime parsing
            dt = datetime.fromisoformat(time_str.replace("Z", "+00:00"))
            formatted_times.append(dt.isoformat())
        except Exception:
            # Fallback to original string if parsing fails
            formatted_times.append(time_str)
    
    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=formatted_times,  # Use formatted time string
            y=values,
            mode="lines+markers",
            name=metric,
            line=dict(color="#1f77b4", width=2),
            marker=dict(size=4, opacity=0.7),
            hovertemplate="<b>%{fullData.name}</b><br>" +
                         "Time: %{x}<br>" +
                         "Value: %{y:.2f}<extra></extra>",
        )
    )
    
    # Update layout with appropriate units based on metric type
    unit = "($/MWh)" if metric == "price" else "(MW)"
    
    # X axis configuration (automatic range, Plotly will automatically adapt to data range)
    # fixedrange=False allows user to pan/zoom the chart
    xaxis_config = dict(
        type="date",  # Use date axis for time series
        autorange=True,  # Automatically adjust range to fit data
        automargin=True,  # Automatically adjust margins
        rangeslider=dict(visible=False),  # Hide range slider for cleaner UI
        showspikes=True,  # Show crosshair spikes on hover
        spikecolor="gray",
        spikethickness=1,
        fixedrange=False,  # Allow user interaction (pan/zoom)
    )
    
    fig.update_layout(
        title=f"{metric.capitalize()} - {network_code} {unit}",
        xaxis_title="Time",
        yaxis_title=f"{metric.capitalize()} {unit}",
        height=400,
        margin=dict(l=50, r=20, t=50, b=50),
        hovermode="x unified",
        template="plotly_white",
        xaxis=xaxis_config,
        # Use latest data point timestamp as uirevision to maintain user's zoom/pan
        # when new data is added, ensuring view focuses on latest data
        uirevision=display_history[-1].get("event_time", "") if display_history else "",
        yaxis=dict(
            autorange=True,
            automargin=True,
            showspikes=True,
            spikecolor="gray",
            spikethickness=1,
        ),
        dragmode="pan",
    )
    
    return dcc.Graph(
        id=f"market-chart-{network_code}-{metric}",
        figure=fig,
        config={
            "displayModeBar": True,  # Show toolbar
            "displaylogo": False,  # Hide Plotly logo
            "scrollZoom": True,  # Enable scroll zoom (for viewing historical data)
            "doubleClick": "reset",  # Double click to reset view
            "modeBarButtonsToRemove": ["lasso2d", "select2d"],  # Remove unnecessary tools
        },
    )


def create_filter_panel() -> html.Div:
    """
    Create filter panel component.
    
    Returns:
        Dash Div component, contains all filter controls
    """
    return html.Div(
        [
            html.H4("Filters", style={"marginBottom": "20px"}),
            
            # Network region filter
            html.Div(
                [
                    html.Label("Network Region:", style={"fontWeight": "bold"}),
                    dcc.Dropdown(
                        id="filter-region",
                        options=[
                            {"label": REGION_DISPLAY_MAP.get(region, region), "value": region}
                            for region in NEM_REGIONS
                        ],
                        multi=True,
                        placeholder="Select regions...",
                        style={"marginBottom": "20px"},
                    ),
                ]
            ),
            
            # Fuel technology filter
            html.Div(
                [
                    html.Label("Fuel Technology:", style={"fontWeight": "bold"}),
                    dcc.Dropdown(
                        id="filter-fuel-type",
                        options=[{"label": fuel, "value": fuel} for fuel in FUEL_TYPES],
                        multi=True,
                        placeholder="Select fuel types...",
                        style={"marginBottom": "20px"},
                    ),
                ]
            ),
            
            # Status display
            html.Div(
                [
                    html.Hr(),
                    html.P(id="status-text", children="Ready", style={"fontSize": "12px", "color": "gray"}),
                ]
            ),
            
            # Statistics container
            html.Div(
                id="statistics-container",
                children=[],
                style={
                    "marginTop": "20px",
                    "maxHeight": "calc(100vh - 400px)",
                    "overflowY": "auto",
                },
            ),
        ],
        style={
            "width": "250px",
            "padding": "20px",
            "backgroundColor": "#f5f5f5",
            "height": "100vh",
            "overflowY": "hidden",
        },
    )


def get_power_color(power: float) -> str:
    """
    Return color based on power value.
    
    Args:
        power: Power value (MW)
    
    Returns:
        Color name (used for marker icon)
    """
    if power == 0:
        return "grey"
    elif power < 50:
        return "green"
    elif power < 100:
        return "yellow"
    elif power < 200:
        return "orange"
    else:
        return "red"


def get_emissions_color(emissions: float) -> str:
    """
    Return color based on emissions value.
    
    Args:
        emissions: Emissions value (t)
    
    Returns:
        Color name (used for marker icon)
    """
    if emissions == 0:
        return "green"
    elif emissions < 10:
        return "yellow"
    elif emissions < 50:
        return "orange"
    else:
        return "red"


def create_facility_marker(
    facility_id: str,
    latitude: float,
    longitude: float,
    name: str,
    power: float,
    emissions: float,
    metadata: dict | None = None,
) -> dl.Marker:
    """
    Create facility marker.
    
    Args:
        facility_id: Facility ID
        latitude: Latitude
        longitude: Longitude
        name: Facility name
        power: Power value
        emissions: Emissions value
        metadata: Metadata dictionary (optional)
    
    Returns:
        Dash Leaflet Marker component
    """
    # Use power color strategy (main indicator)
    color = get_power_color(power)
    
    # Prepare metadata information (for detail display)
    if metadata is None:
        metadata = {}
    
    network_region_raw = metadata.get("network_region", "N/A")
    # Use display mapping, remove "1" suffix to maintain consistency
    network_region = REGION_DISPLAY_MAP.get(network_region_raw, network_region_raw) if network_region_raw != "N/A" else "N/A"
    fuel_technology = metadata.get("fuel_technology", "N/A")
    
    # Create marker
    # Note: dash-leaflet does not support Icon component, use default icon
    marker = dl.Marker(
        position=[latitude, longitude],
        id={"type": "facility-marker", "id": facility_id},
        children=[
            dl.Tooltip(
                html.Div(
                    [
                        html.Strong(name),
                        html.Br(),
                        html.Span(f"Power: {power:.2f} MW"),
                        html.Br(),
                        html.Span(f"Emissions: {emissions:.2f} t"),
                    ],
                    style={"textAlign": "center"},
                ),
            ),
            dl.Popup(
                html.Div(
                    [
                        # Basic information area (always displayed)
                        html.H4(name),
                        html.P(f"Facility ID: {facility_id}"),
                        html.P(f"Network Region: {network_region}"),
                        html.P(f"Fuel Technology: {fuel_technology}"),
                        html.Hr(),
                        html.P(f"Power: {power:.2f} MW"),
                        html.P(f"Emissions: {emissions:.2f} t"),
                    ],
                    style={"padding": "10px", "minWidth": "200px"},
                )
            ),
        ],
        title=name,  # Text displayed when hovering
    )
    
    return marker


def create_markers_from_facilities(
    facilities: Dict[str, dict],
) -> list:
    """
    Create marker list from facility data.
    
    Args:
        facilities: Facility data dictionary
    
    Returns:
        Marker list
    """
    markers = []
    
    for facility_id, facility_data in facilities.items():
        # Get location information
        latitude = facility_data.get("latitude")
        longitude = facility_data.get("longitude")
        
        # If location information is missing, skip
        if latitude is None or longitude is None:
            logger.warning(f"Missing location for facility {facility_id}")
            continue
        
        # Get metadata
        metadata = facility_data.get("metadata", {})
        name = metadata.get("name", facility_id)
        
        # Get power and emissions value
        power = facility_data.get("power", 0.0)
        emissions = facility_data.get("emissions", 0.0)
        
        # Create marker (pass metadata for detail display)
        marker = create_facility_marker(
            facility_id=facility_id,
            latitude=float(latitude),
            longitude=float(longitude),
            name=name,
            power=float(power),
            emissions=float(emissions),
            metadata=metadata,
        )
        
        markers.append(marker)
    
    return markers


def create_map_component(
    markers: Optional[list] = None,
    center: Optional[list] = None,
    zoom: int = 5,
) -> dl.Map:
    """
    Create Leaflet map component.
    
    Args:
        markers: Marker list
        center: Map center coordinates [lat, lng]
        zoom: Initial zoom level
    
    Returns:
        Dash Leaflet Map component
    """
    if center is None:
        center = NEM_REGION_CENTER
    
    if markers is None:
        markers = []
    
    # Create map component list
    map_children = [
        dl.TileLayer(
            url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png",
            attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors',
        ),
    ]
    
    # If there are markers, use LayerGroup to include them
    if markers:
        map_children.append(
            dl.LayerGroup(id="marker-layer", children=markers)
        )
    
    # Create map
    map_component = dl.Map(
        map_children,
        id="map",
        center=center,
        zoom=zoom,
        style={"width": "100%", "height": "100%"},
    )
    
    return map_component


def calculate_statistics(facilities: Dict[str, dict]) -> dict:
    """
    Calculate facility statistics.
    
    Args:
        facilities: Facilities data dictionary
    
    Returns:
        Dictionary with statistics
    """
    if not facilities:
        return {
            "total_power": 0.0,
            "total_emissions": 0.0,
            "avg_power": 0.0,
            "avg_emissions": 0.0,
            "facility_count": 0,
            "by_region": {},
        }
    
    total_power = 0.0
    total_emissions = 0.0
    facility_count = len(facilities)
    
    # Count by region
    by_region: Dict[str, dict] = {}
    
    for facility_id, facility_data in facilities.items():
        # Get basic data
        power = float(facility_data.get("power", 0.0) or 0.0)
        emissions = float(facility_data.get("emissions", 0.0) or 0.0)
        
        total_power += power
        total_emissions += emissions
        
        # Get metadata
        metadata = facility_data.get("metadata", {})
        region = metadata.get("network_region", "Unknown")
        
        # Count by region
        if region not in by_region:
            by_region[region] = {"count": 0, "power": 0.0, "emissions": 0.0}
        by_region[region]["count"] += 1
        by_region[region]["power"] += power
        by_region[region]["emissions"] += emissions
    
    # Calculate average
    avg_power = total_power / facility_count if facility_count > 0 else 0.0
    avg_emissions = total_emissions / facility_count if facility_count > 0 else 0.0
    
    return {
        "total_power": total_power,
        "total_emissions": total_emissions,
        "avg_power": avg_power,
        "avg_emissions": avg_emissions,
        "facility_count": facility_count,
        "by_region": by_region,
    }


def create_statistics_ui(stats: dict) -> html.Div:
    """
    Create statistics UI component.
    
    Args:
        stats: Statistics dictionary
    
    Returns:
        Div component with statistics
    """
    # Basic style definition
    section_style = {
        "marginTop": "15px",
        "padding": "12px",
        "backgroundColor": "#ffffff",
        "borderRadius": "5px",
        "border": "1px solid #e0e0e0",
        "fontSize": "11px",
    }
    
    title_style = {
        "fontWeight": "bold",
        "fontSize": "12px",
        "marginBottom": "10px",
        "color": "#333",
    }
    
    stat_item_style = {
        "marginBottom": "8px",
        "display": "flex",
        "justifyContent": "space-between",
        "alignItems": "center",
    }
    
    stat_label_style = {
        "fontWeight": "500",
        "color": "#555",
        "fontSize": "11px",
    }
    
    stat_value_style = {
        "color": "#333",
        "fontFamily": "monospace",
        "fontSize": "11px",
        "fontWeight": "bold",
    }
    
    divider_style = {
        "margin": "10px 0",
        "borderTop": "1px solid #e0e0e0",
    }
    
    # Overview section
    overview_items = [
        html.Div(
            [
                html.Span("Total Power:", style=stat_label_style),
                html.Span(f"{stats['total_power']:,.1f} MW", style=stat_value_style),
            ],
            style=stat_item_style,
        ),
        html.Div(
            [
                html.Span("Total Emissions:", style=stat_label_style),
                html.Span(f"{stats['total_emissions']:,.1f} t", style=stat_value_style),
            ],
            style=stat_item_style,
        ),
        html.Div(
            [
                html.Span("Avg Power:", style=stat_label_style),
                html.Span(f"{stats['avg_power']:.1f} MW", style=stat_value_style),
            ],
            style=stat_item_style,
        ),
        html.Div(
            [
                html.Span("Avg Emissions:", style=stat_label_style),
                html.Span(f"{stats['avg_emissions']:.2f} t", style=stat_value_style),
            ],
            style=stat_item_style,
        ),
    ]
    
    overview_section = html.Div(
        [
            html.H5("Overview of Facilities", style=title_style),
            html.Hr(style=divider_style),
            *overview_items,
        ],
        style=section_style,
    )
    
    # By Region section
    by_region_items = []
    by_region_data = stats.get("by_region", {})
    
    # Sort by region code (ensure display order)
    sorted_regions = sorted(by_region_data.keys())
    
    for region in sorted_regions:
        region_data = by_region_data[region]
        # Use display mapping to remove "1" suffix
        display_region = REGION_DISPLAY_MAP.get(region, region)
        
        by_region_items.append(
            html.Div(
                [
                    html.Span(f"{display_region}:", style=stat_label_style),
                    html.Span(
                        f"{region_data['count']} facilities",
                        style=stat_value_style,
                    ),
                ],
                style=stat_item_style,
            ),
        )
        by_region_items.append(
            html.Div(
                [
                    html.Span("  Power:", style={**stat_label_style, "paddingLeft": "10px"}),
                    html.Span(f"{region_data['power']:,.1f} MW", style=stat_value_style),
                ],
                style=stat_item_style,
            ),
        )
        by_region_items.append(
            html.Div(
                [
                    html.Span("  Emissions:", style={**stat_label_style, "paddingLeft": "10px"}),
                    html.Span(f"{region_data['emissions']:,.1f} t", style=stat_value_style),
                ],
                style=stat_item_style,
            ),
        )
    
    # Prepare By Region section children
    by_region_children = [html.H5("By Region of Facilities", style=title_style), html.Hr(style=divider_style)]
    if by_region_items:
        by_region_children.extend(by_region_items)
    else:
        by_region_children.append(html.Div("No data", style={"color": "#999", "fontSize": "10px"}))
    
    by_region_section = html.Div(
        by_region_children,
        style=section_style,
    )
    
    # Combine all sections
    return html.Div(
        [
            overview_section,
            by_region_section,
        ],
        style={
            "width": "100%",
        },
    )

# ========== Section 7: Dash Application ==========
# Initialize Dash application
dash_app = dash.Dash(__name__)
dash_app.title = "Electricity Facility Dashboard"

# Initialize frontend state managers
frontend_state_manager = FrontendStateManager()
frontend_market_state_manager = FrontendMarketStateManager(max_history=None)

# WebSocket client
websocket_client: WebSocketClient | None = None

# Add charts state store ids
dash_app.server.config["charts_snapshot"] = {}
dash_app.server.config["charts_latest"] = {}


# ========== Section 7.1: WebSocket Message Handling ==========
def websocket_message_handler(message: dict) -> None:
    """
    WebSocket message handling function.
    
    Args:
        message: Received message dictionary
    """
    message_type = message.get("type")
    data = message.get("data", {})
    
    if message_type == "facility_update":
        frontend_state_manager.update_facility(data)
    elif message_type == "initial_data":
        facilities = data.get("facilities", {})
        frontend_state_manager.update_facilities_batch(facilities)
        logger.info(f"Loaded initial data: {len(facilities)} facilities")
    elif message_type == "market_update":
        network_code = data.get("network_code")
        metric = data.get("metric")
        event_time = data.get("event_time")
        value = data.get("value")
        if network_code and metric and event_time and value is not None:
            if isinstance(value, float) and math.isnan(value):
                logger.warning(f"Received NaN value for {network_code}/{metric}, skipping update")
            else:
                frontend_market_state_manager.update_metric(network_code, metric, event_time, value)
    elif message_type == "initial_market_data":
        metrics = data.get("metrics", {})
        if metrics:
            frontend_market_state_manager.update_metrics_batch(metrics)
            logger.info(f"Loaded initial market data: {len(metrics)} networks")
    elif message_type == "initial_charts":
        # Store snapshot to Dash Store via clientside trigger (global temp cache)
        dash_app.server.config["charts_snapshot"] = data
    elif message_type == "charts_update":
        # Update the latest charts payload in server memory; Dash callbacks pull every second
        dash_app.server.config["charts_latest"] = data


def setup_websocket_client() -> None:
    """Setup WebSocket client."""
    global websocket_client
    
    try:
        client = WebSocketClient(
            uri="ws://localhost:8000/ws/realtime",
            message_callback=websocket_message_handler,
        )
        
        async def run_client() -> None:
            """Run WebSocket client."""
            max_retries = 5
            retry_delay = 5
            
            try:
                for attempt in range(max_retries):
                    try:
                        await client.connect()
                        await client.listen()
                        break
                    except Exception as e:
                        logger.warning(f"WebSocket connection attempt {attempt + 1}/{max_retries} failed: {e}")
                        if attempt < max_retries - 1:
                            await asyncio.sleep(retry_delay)
                        else:
                            logger.error("Failed to connect to WebSocket server after max retries")
            finally:
                if client.websocket:
                    await client.disconnect()
        
        # Run in new thread
        def run_in_thread() -> None:
            """Run WebSocket client in a separate event loop thread.
            
            Creates a new event loop for the WebSocket client to run in,
            since it needs to run asynchronously in a separate thread from
            the main Dash application.
            """
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                loop.run_until_complete(run_client())
            except Exception as e:
                logger.error(f"WebSocket client thread error: {e}")
            finally:
                loop.close()
        
        # Delay start, wait for FastAPI server to start
        def delayed_start() -> None:
            """Start WebSocket client after a delay to allow FastAPI server to initialize.
            
            Waits 2 seconds for the FastAPI server to start before attempting
            to connect the WebSocket client, preventing connection failures.
            """
            time.sleep(2)  # Wait for backend to start
            thread = threading.Thread(target=run_in_thread, daemon=True)
            thread.start()
        
        thread = threading.Thread(target=delayed_start, daemon=True)
        thread.start()
        websocket_client = client
        
        logger.info("WebSocket client setup initiated")
    except Exception as e:
        logger.error(f"Failed to setup WebSocket client: {e}")


# Setup WebSocket client (when application starts)
setup_websocket_client()


# ========== Section 7.2: Dash Application Layout ==========
dash_app.layout = html.Div(
    [
        # State storage
        dcc.Store(id="facilities-store", data={}),
        dcc.Store(id="market-metrics-store", data={}),
        dcc.Store(id="charts-store", data={}),
        
        # Title
        html.H1("Electricity Facility Dashboard", style={"padding": "20px", "textAlign": "center"}),
        
        # Tab container
        dcc.Tabs(
            id="main-tabs",
            value="tab-map",  # Default selected map view
            children=[
                # Tab 1: Map view
                dcc.Tab(
                    label="Map view",
                    value="tab-map",
                    children=html.Div(
                        [
                            # Map view layout
                            html.Div(
                                [
                                    # Left filter panel
                                    create_filter_panel(),
                                    
                                    # Right map area
                                    html.Div(
                                        [
                                            html.H2("Facilities Map", style={"padding": "10px", "textAlign": "center"}),
                                            
                                            # Map container
                                            html.Div(
                                                [
                                                    html.Div(
                                                        id="map-container",
                                                        children=create_map_component(center=NEM_REGION_CENTER),
                                                        style={
                                                            "width": "100%",
                                                            "height": "100%",
                                                            "position": "relative",
                                                        },
                                                    ),
                                                ],
                                                style={
                                                    "width": "100%",
                                                    "height": "calc(100vh - 200px)",
                                                    "minHeight": "500px",
                                                    "flexShrink": "0",
                                                    "position": "relative",
                                                },
                                            ),
                                        ],
                                        style={"flex": "1", "position": "relative", "display": "flex", "flexDirection": "column"},
                                    ),
                                ],
                                style={"display": "flex", "flexDirection": "row", "height": "calc(100vh - 150px)"},
                            ),
                        ],
                        style={"padding": "10px"},
                    ),
                ),
                
                # Tab 2: Market charts
                dcc.Tab(
                    label="Market charts",
                    value="tab-market",
                    children=html.Div(
                        [
                            html.H2("Market Metrics", style={"padding": "20px", "textAlign": "center"}),
                            
                            # Network selector area
                            html.Div(
                                [
                                    html.Label("Network:", style={"fontWeight": "bold", "marginRight": "10px"}),
                                    dcc.Dropdown(
                                        id="market-network-selector",
                                        options=[
                                            {"label": "NEM", "value": "NEM"},
                                        ],
                                        value="NEM",
                                        style={"width": "200px", "display": "inline-block"},
                                    ),
                                ],
                                style={"padding": "20px", "textAlign": "center"},
                            ),
                            
                            # Chart area (vertical layout, display Price and Demand charts)
                            html.Div(
                                id="market-charts-container",
                                children=[],
                                style={
                                    "display": "flex",
                                    "flexDirection": "column",
                                    "gap": "20px",
                                    "width": "100%",
                                    "height": "calc(100vh - 250px)",
                                    "minHeight": "500px",
                                    "padding": "20px",
                                },
                            ),
                        ],
                    ),
                ),
                
                # Tab 3: Facilities charts
                dcc.Tab(
                    label="Facilities charts",
                    value="tab-facilities-charts",
                    children=html.Div(
                        [
                            html.H2("Facilities Charts", style={"padding": "20px", "textAlign": "center"}),
                            html.Div(
                                [
                                    html.Div(dcc.Graph(id="fac-line-chart"), style={"flex": "1", "minHeight": "350px"}),
                                    html.Div(dcc.Graph(id="fac-donut-chart"), style={"flex": "1", "minHeight": "350px"}),
                                ],
                                style={"display": "flex", "gap": "20px", "padding": "10px"},
                            ),
                            html.Div(
                                [
                                    html.Div(dcc.Graph(id="fac-region-bar"), style={"flex": "1", "minHeight": "400px"}),
                                    html.Div(dcc.Graph(id="fac-topn-bar"), style={"flex": "1", "minHeight": "400px"}),
                                ],
                                style={"display": "flex", "gap": "20px", "padding": "10px"},
                            ),
                        ],
                    ),
                ),
            ],
            style={"width": "100%", "marginBottom": "20px"},
        ),
        
        # Periodic update component (for triggering callbacks)
        dcc.Interval(
            id="interval-component",
            interval=1000,  # Update once per second
            n_intervals=0,
        ),
    ]
)


# ========== Section 7.3: Dash Callback Functions ==========
@dash_app.callback(
    Output("facilities-store", "data"),
    Input("interval-component", "n_intervals"),
    State("facilities-store", "data"),
    State("main-tabs", "value"),  # Add tab state
)
def update_facilities_store(
    n_intervals: int,
    current_store: dict,
    current_tab: str,  # Current selected tab
) -> dict:
    """
    Update facilities data store (read from state manager).
    
    Only update when "map view" tab is active, to optimize performance.
    
    Args:
        n_intervals: Interval counter
        current_store: Current stored data
        current_tab: Current selected tab
    
    Returns:
        Updated facilities data
    """
    # Only update when "map view" tab is active
    if current_tab != "tab-map":
        # If not "map view", keep current data unchanged
        return current_store or {}
    
    # Get all facilities from state manager (apply filters)
    facilities = frontend_state_manager.get_all_facilities()
    
    return facilities


@dash_app.callback(
    Output("map-container", "children"),
    Input("facilities-store", "data"),
    Input("filter-region", "value"),
    Input("filter-fuel-type", "value"),
    State("main-tabs", "value"),  # Add tab state
)
def update_map(
    facilities_store: dict,
    filter_region: list | None,
    filter_fuel_type: list | None,
    current_tab: str,  # Current selected tab
) -> Any:
    """
    Update map component.
    
    Only update when "map view" tab is active, to optimize performance.
    
    Args:
        facilities_store: Facilities data store
        filter_region: Region filter value
        filter_fuel_type: Fuel type filter value
        current_tab: Current selected tab
    
    Returns:
        Updated map component
    """
    # Only update when "map view" tab is active
    if current_tab != "tab-map":
        # If not "map view", return no update
        return no_update
    
    # Update filters and get filtered facilities
    if filter_region is not None:
        frontend_state_manager.set_filters(regions=filter_region)
    if filter_fuel_type is not None:
        frontend_state_manager.set_filters(fuel_types=filter_fuel_type)
    
    facilities = frontend_state_manager.get_all_facilities()
    
    # Create markers (Tooltip display power and emissions)
    markers = create_markers_from_facilities(facilities)
    
    # Create map component
    map_component = create_map_component(
        markers=markers,
        center=NEM_REGION_CENTER,
        zoom=5,
    )
    
    return map_component


@dash_app.callback(
    Output("status-text", "children"),
    Input("facilities-store", "data"),
)
def update_status(facilities_store: dict) -> str:
    """
    Update status text.
    
    Args:
        facilities_store: Facilities data store
    
    Returns:
        Status text
    """
    facility_count = len(facilities_store)
    return f"Facilities: {facility_count} | Status: Active"


@dash_app.callback(
    Output("statistics-container", "children"),
    Input("facilities-store", "data"),
    Input("filter-region", "value"),
    Input("filter-fuel-type", "value"),
)
def update_statistics(
    facilities_store: dict,
    selected_regions: Optional[list],
    selected_fuel_types: Optional[list],
) -> html.Div:
    """
    Update statistics.
    
    Calculate and display statistics based on facility data and filters, including:
    - Overview: Total power, total emissions, average power, average emissions
    - By Region: Count facilities by region, power, emissions
    
    Args:
        facilities_store: Facilities data store
        selected_regions: Selected region list (optional)
        selected_fuel_types: Selected fuel type list (optional)
    
    Returns:
        Div component with statistics
    """
    if not facilities_store:
        # If no data, display empty state
        return html.Div(
            [
                html.Div(
                    [
                        html.H5("Overview of Facilities", style={"fontWeight": "bold", "fontSize": "12px"}),
                        html.Div("No data available", style={"color": "#999", "fontSize": "11px"}),
                    ],
                    style={
                        "padding": "12px",
                        "backgroundColor": "#ffffff",
                        "borderRadius": "5px",
                        "border": "1px solid #e0e0e0",
                    },
                ),
            ],
        )
    
    # Update filters (affect subsequent get_all_facilities calls)
    if selected_regions is not None:
        frontend_state_manager.set_filters(regions=selected_regions)
    if selected_fuel_types is not None:
        frontend_state_manager.set_filters(fuel_types=selected_fuel_types)
    
    # Get filtered facilities and calculate statistics
    filtered_facilities = frontend_state_manager.get_all_facilities()
    stats = calculate_statistics(filtered_facilities)
    
    # Create and return statistics UI
    return create_statistics_ui(stats)


@dash_app.callback(
    Output("market-charts-container", "children"),
    Input("market-network-selector", "value"),
    Input("interval-component", "n_intervals"),
    Input("main-tabs", "value"),  # Change to Input, trigger update when switching tabs
)
def update_market_charts(
    network_code: str,
    n_intervals: int,
    current_tab: str,  # Current selected tab
) -> Any:
    """
    Update market data charts (fixed window: latest 50 data points).
    
    Display both Price and Demand charts, using vertical layout.
    This callback triggers once per second (through interval-component),
    re-rendering the charts to display the latest time series data.
    The charts use a fixed window mode, only displaying the latest 50 data points, implementing a sliding window effect.
    Only update when "market charts" tab is active, to optimize performance.
    
    Args:
        network_code: Network code (e.g. "NEM")
        n_intervals: Interval counter (for triggering update)
        current_tab: Current selected tab
    
    Returns:
        Vertical layout container with Price and Demand charts
    """
    if not network_code:
        return []
    
    # Only update when "market charts" tab is active
    if current_tab != "tab-market":
        return []
    
    # Create Price and Demand charts (fixed window: latest 50 data points)
    charts_container = html.Div(
        [
            html.Div(
                create_market_chart(network_code=network_code, metric="price", market_state_manager=frontend_market_state_manager),
                style={"flex": "1", "minHeight": "400px", "width": "100%"},
            ),
            html.Div(
                create_market_chart(network_code=network_code, metric="demand", market_state_manager=frontend_market_state_manager),
                style={"flex": "1", "minHeight": "400px", "width": "100%"},
            ),
        ],
        style={
            "display": "flex",
            "flexDirection": "column",
            "gap": "20px",
            "width": "100%",
            "height": "100%",
        },
    )
    
    return charts_container


@dash_app.callback(
    Output("charts-store", "data"),
    Input("interval-component", "n_intervals"),
    State("charts-store", "data"),
)
def pull_charts_state(n: int, current: dict) -> dict:
    """Pull latest charts payload delivered via WebSocket and cached on server.
    
    This callback runs every second (via interval-component) to fetch the
    latest chart data from the server's memory cache, which is updated
    by the WebSocket message handler.
    
    Args:
        n: Interval counter (unused, but required by Dash callback)
        current: Current charts store data
    
    Returns:
        Latest charts payload from server cache, or current/empty dict if unavailable
    """
    # Try to get latest update first, fallback to initial snapshot
    latest = dash_app.server.config.get("charts_latest") or dash_app.server.config.get("charts_snapshot")
    return latest or current or {}


@dash_app.callback(
    Output("fac-line-chart", "figure"),
    Output("fac-donut-chart", "figure"),
    Output("fac-region-bar", "figure"),
    Output("fac-topn-bar", "figure"),
    Input("charts-store", "data"),
    Input("main-tabs", "value"),
)
def render_facilities_charts(charts: dict, current_tab: str):
    """Render four facilities charts from charts-store.
    
    Creates four Plotly figures:
    1. Line chart: Total power and emissions over time
    2. Donut chart: Power distribution by fuel technology
    3. Region bar chart: Power and emissions by network region
    4. Top-N bar chart: Top facilities by power
    
    Args:
        charts: Charts payload dictionary from charts-store
        current_tab: Currently selected tab (only render when facilities-charts tab is active)
    
    Returns:
        Tuple of four Plotly Figure objects (line, donut, region_bar, top_bar)
    """
    # Only render when facilities-charts tab is active to optimize performance
    if current_tab != "tab-facilities-charts" or not charts:
        return go.Figure(), go.Figure(), go.Figure(), go.Figure()
    
    # Line chart: Total power and emissions over time
    line_fig = go.Figure()
    for series in charts.get("line", []):
        xs = [pt.get("t") for pt in series.get("points", [])]
        ys = [pt.get("v") for pt in series.get("points", [])]
        raw_name = series.get("name")
        # Map internal names to display names
        if raw_name == "total_power":
            trace_name = "Total Power (MW)"
        elif raw_name == "total_emissions":
            trace_name = "Total Emissions (t)"
        else:
            trace_name = raw_name
        line_fig.add_trace(go.Scatter(x=xs, y=ys, mode="lines", name=trace_name))
    line_fig.update_layout(title="Total Power & Emissions", template="plotly_white")
    
    # Donut chart: Power distribution by fuel technology
    labels = [s.get("fuel") for s in charts.get("donut", [])]
    values = [s.get("power", 0.0) for s in charts.get("donut", [])]
    donut_fig = go.Figure(go.Pie(labels=labels, values=values, hole=0.5, name="Power (MW)"))
    donut_fig.update_layout(title="Power Share by Fuel", template="plotly_white")
    
    # Region bar chart: Power & emissions side-by-side for each region
    regions_raw = [item.get("region") for item in charts.get("bar_by_region", [])]
    # Apply display mapping to remove "1" suffix from region codes
    regions = [REGION_DISPLAY_MAP.get(r, r) for r in regions_raw]
    power_vals = [item.get("power", 0.0) for item in charts.get("bar_by_region", [])]
    emissions_vals = [item.get("emissions", 0.0) for item in charts.get("bar_by_region", [])]
    region_bar = go.Figure()
    region_bar.add_trace(go.Bar(x=regions, y=power_vals, name="Power (MW)", marker_color="#1f77b4"))
    region_bar.add_trace(go.Bar(x=regions, y=emissions_vals, name="Emissions (t)", marker_color="#ff7f0e"))
    region_bar.update_layout(barmode="group", title="By Region: Power & Emissions", template="plotly_white", xaxis_title="Region")
    
    # Top-N facilities bar chart: Sorted by power descending
    names = [item.get("name") for item in charts.get("bar_top_facilities", [])]
    top_power = [item.get("power", 0.0) for item in charts.get("bar_top_facilities", [])]
    top_emissions = [item.get("emissions", 0.0) for item in charts.get("bar_top_facilities", [])]
    top_bar = go.Figure()
    top_bar.add_trace(go.Bar(x=names, y=top_power, name="Power (MW)", marker_color="#2ca02c"))
    top_bar.add_trace(go.Bar(x=names, y=top_emissions, name="Emissions (t)", marker_color="#d62728"))
    top_bar.update_layout(barmode="group", title="Top-10 Facilities: Power & Emissions", template="plotly_white", xaxis_title="Facility")
    
    return line_fig, donut_fig, region_bar, top_bar


# ========== Section 8: Main Entry Point ==========
def run_fastapi() -> None:
    """Run FastAPI server in background thread.
    
    Starts the FastAPI application using uvicorn ASGI server.
    This function is intended to be run in a separate daemon thread
    to allow the Dash application to run in the main process.
    """
    import uvicorn
    uvicorn.run(
        fastapi_app,
        host="0.0.0.0",  # Listen on all network interfaces
        port=8000,  # Backend API port
        log_level="info",
    )


if __name__ == "__main__":
    # Start FastAPI in background thread (daemon)
    # Daemon threads automatically terminate when main process exits
    fastapi_thread = threading.Thread(target=run_fastapi, daemon=True)
    fastapi_thread.start()
    logger.info("FastAPI server started in background thread")
    
    # Run Dash application in main process
    # This blocks until the application is stopped
    dash_app.run(debug=True, host="0.0.0.0", port=8050)  # Frontend dashboard port

