"""FastAPI backend service merges all backend logic."""

from __future__ import annotations

import asyncio
import json
import logging
from contextlib import asynccontextmanager
from datetime import datetime
from pathlib import Path
from typing import Callable, Dict, Optional, Set

import pandas as pd
import paho.mqtt.client as mqtt
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from starlette.websockets import WebSocketState

try:
    from schemas import FacilityUpdate, MQTTMessage, MarketMetricsMessage, MarketUpdate
except ImportError:
    # If running directly, use relative import
    import sys
    from pathlib import Path
    sys.path.insert(0, str(Path(__file__).parent))
    from schemas import FacilityUpdate, MQTTMessage, MarketMetricsMessage, MarketUpdate

# ========== Logging configuration ==========
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# ========== Data models ==========
# All data models defined in schemas.py

# ========== State manager ==========
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
        """Clear all state."""
        self._state.clear()
        logger.info("State cleared")


# ========== Market state manager ==========
class MarketStateManager:
    """Manage market metrics state (in memory)."""
    
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
        """Clear all state."""
        self._state.clear()
        logger.info("Market state cleared")


# ========== Market data processor ==========
class MarketDataProcessor:
    """Process market MQTT messages and update state."""
    
    def __init__(
        self,
        market_state_manager: MarketStateManager,
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


# ========== Metadata loader ==========
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


# ========== Data processor ==========
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


# ========== MQTT subscriber service ==========
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
        """Check if connected."""
        return self._connected and self.client is not None and self.client.is_connected()


# ========== WebSocket manager ==========
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


# ========== Global objects ==========
state_manager = StateManager()
market_state_manager = MarketStateManager()
metadata_loader: MetadataLoader | None = None
data_processor: DataProcessor | None = None
market_data_processor: MarketDataProcessor | None = None
websocket_manager = WebSocketManager()
mqtt_subscriber: MQTTSubscriber | None = None
market_mqtt_subscriber: MQTTSubscriber | None = None
_global_event_loop: asyncio.AbstractEventLoop | None = None


# ========== Initialization functions ==========
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


# ========== FastAPI application lifecycle ==========
@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifecycle management."""
    global metadata_loader, data_processor, market_data_processor, mqtt_subscriber, market_mqtt_subscriber, _global_event_loop
    
    # Initialize when starting
    logger.info("Starting FastAPI application...")
    
    _global_event_loop = asyncio.get_running_loop()
    metadata_loader = setup_metadata_loader()
    data_processor = DataProcessor(state_manager, metadata_loader)
    market_data_processor = MarketDataProcessor(market_state_manager)
    
    # Start MQTT subscriber service
    mqtt_subscriber = setup_mqtt_subscriber()
    mqtt_subscriber.start()
    market_mqtt_subscriber = setup_market_mqtt_subscriber()
    market_mqtt_subscriber.start()
    
    logger.info("FastAPI application started")
    
    yield
    
    # Clean up when closing
    logger.info("Shutting down FastAPI application...")
    if market_mqtt_subscriber:
        market_mqtt_subscriber.stop()
    if mqtt_subscriber:
        mqtt_subscriber.stop()
    _global_event_loop = None


# ========== FastAPI application initialization ==========
app = FastAPI(
    title="Electricity Dashboard Backend",
    description="FastAPI backend for real-time electricity facility monitoring",
    version="1.0.0",
    lifespan=lifespan,
)


# ========== Route definitions ==========
@app.get("/")
async def root() -> dict:
    """Root endpoint."""
    return {
        "message": "Electricity Dashboard Backend API",
        "status": "running",
        "websocket_endpoint": "/ws/realtime",
    }


@app.get("/api/facilities")
async def get_all_facilities() -> dict:
    """
    Get all facilities current status.
    
    Returns:
        All facilities status dictionary
    """
    facilities = state_manager.get_all_facilities()
    return {
        "facilities": facilities,
        "count": len(facilities),
    }


@app.get("/api/facilities/{facility_id}")
async def get_facility(facility_id: str) -> dict:
    """
    Get the status of a specified facility.
    
    Args:
        facility_id: Facility ID
    
    Returns:
        Facility status dictionary
    """
    facility = state_manager.get_facility(facility_id)
    if not facility:
        return {"error": "Facility not found"}
    return facility


@app.get("/api/metadata")
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


@app.get("/api/market/latest")
async def get_latest_market_metrics() -> dict:
    """
    Get latest market metrics.
    
    Returns:
        All market metrics latest values
    """
    metrics = market_state_manager.get_all_metrics()
    return {
        "metrics": metrics,
        "count": sum(len(network_metrics) for network_metrics in metrics.values()),
    }


@app.get("/api/market/{network_code}/{metric}")
async def get_market_metric(network_code: str, metric: str) -> dict:
    """
    Get latest value of a specified network and metric.
    
    Args:
        network_code: Network code (e.g. "NEM")
        metric: Metric type (e.g. "price", "demand")
    
    Returns:
        Metric data dictionary
    """
    metric_data = market_state_manager.get_metric(network_code, metric)
    if not metric_data:
        return {"error": f"Metric not found: {network_code}/{metric}"}
    return metric_data


@app.websocket("/ws/realtime")
async def websocket_endpoint(websocket: WebSocket) -> None:
    """
    WebSocket endpoint, for real-time data push.
    
    Args:
        websocket: WebSocket connection object
    """
    await websocket_manager.connect(websocket)
    
    try:
        # Send initial data (all facilities current status)
        facilities = state_manager.get_all_facilities()
        initial_message: FacilityUpdate = FacilityUpdate(
            type="initial_data",
            data={"facilities": facilities},
        )
        await websocket_manager.send_personal_message(initial_message.model_dump(), websocket)
        
        # Send initial market data
        market_metrics = market_state_manager.get_all_metrics()
        market_initial_message: MarketUpdate = MarketUpdate(
            type="initial_market_data",
            data={"metrics": market_metrics},
        )
        await websocket_manager.send_personal_message(market_initial_message.model_dump(), websocket)
        
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


# ========== Main program entry ==========
if __name__ == "__main__":
    import uvicorn
    
    # Run FastAPI server
    uvicorn.run(
        "backend:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info",
    )

