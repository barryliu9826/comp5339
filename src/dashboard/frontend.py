"""Dash frontend application - merge all frontend logic."""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta
import json
import logging
import math
import threading
import time
from typing import Any, Callable, Dict, Optional

import dash
import dash_leaflet as dl
import plotly.graph_objects as go
import websockets
from dash import dcc, html, no_update
from dash.dependencies import Input, Output, State
from websockets.exceptions import WebSocketException

# ========== Logging configuration ==========
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# ========== Constants definition ==========
# NEM network regions
# NEM region code (actual values with "1" suffix in data)
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

# ========== Frontend state manager ==========
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


# ========== Market state manager ==========
class MarketStateManager:
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


# ========== WebSocket client ==========
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
        """Stop listening."""
        self._running = False


# ========== UI component creation functions ==========
def create_market_chart(network_code: str = "NEM", metric: str = "price") -> dcc.Graph:
    """
    Create real-time scrolling time series chart component (fixed window: latest 50 data points).
    
    Args:
        network_code: Network code
        metric: Metric type (e.g. "price", "demand")
    
    Returns:
        Dash Graph component (fixed window display latest 50 data points)
    """
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
    
    # Fixed window mode: only display latest 50 data points
    max_display_points = 50
    display_history = history[-max_display_points:] if len(history) > max_display_points else history
    
    # Extract time and value (only display latest 50 points)
    # Filter out invalid values (nan, None, etc.)
    valid_data = [
        (item["event_time"], item["value"])
        for item in display_history
        if item.get("value") is not None 
        and not (isinstance(item["value"], float) and math.isnan(item["value"]))
    ]
    
    # De-duplicate and aggregate by timestamp to avoid multiple traces caused by repeated points
    if valid_data:
        aggregated: dict[str, list[float]] = {}
        for ts, val in valid_data:
            aggregated.setdefault(ts, []).append(float(val))
        # Average values per timestamp and sort by time
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
    
    # X axis uses automatic range (Plotly will automatically adapt to data range)
    
    # Create time series line chart
    # Format time string to ISO format (Plotly requirement)
    formatted_times = []
    for time_str in times:
        try:
            dt = datetime.fromisoformat(time_str.replace("Z", "+00:00"))
            formatted_times.append(dt.isoformat())
        except Exception:
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
    
    # Update layout
    unit = "($/MWh)" if metric == "price" else "(MW)"
    
    # X axis configuration (automatic range, Plotly will automatically adapt to data range)
    xaxis_config = dict(
        type="date",
        autorange=True,
        automargin=True,
        rangeslider=dict(visible=False),
        showspikes=True,
        spikecolor="gray",
        spikethickness=1,
        fixedrange=False,
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
        # Use latest data point timestamp as uirevision, ensure view focuses on latest data when new data is added
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


# ========== Dash application initialization ==========
app = dash.Dash(__name__)
app.title = "Electricity Facility Dashboard"

# Initialize state manager
state_manager = FrontendStateManager()
# Initialize market state manager (infinite scrolling mode, supports real-time append)
market_state_manager = MarketStateManager(max_history=None)

# WebSocket client
websocket_client: WebSocketClient | None = None

# Add charts state store ids
app.server.config["charts_snapshot"] = {}
app.server.config["charts_latest"] = {}


# ========== WebSocket message handling ==========
def websocket_message_handler(message: dict) -> None:
    """
    WebSocket message handling function.
    
    Args:
        message: Received message dictionary
    """
    message_type = message.get("type")
    data = message.get("data", {})
    
    if message_type == "facility_update":
        state_manager.update_facility(data)
    elif message_type == "initial_data":
        facilities = data.get("facilities", {})
        state_manager.update_facilities_batch(facilities)
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
                market_state_manager.update_metric(network_code, metric, event_time, value)
    elif message_type == "initial_market_data":
        metrics = data.get("metrics", {})
        if metrics:
            market_state_manager.update_metrics_batch(metrics)
            logger.info(f"Loaded initial market data: {len(metrics)} networks")
    elif message_type == "initial_charts":
        # Store snapshot to Dash Store via clientside trigger (global temp cache)
        app.server.config["charts_snapshot"] = data
    elif message_type == "charts_update":
        # Update the latest charts payload in server memory; Dash callbacks pull every second
        app.server.config["charts_latest"] = data


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

# ========== Application layout ==========
app.layout = html.Div(
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


# ========== Callback function definition ==========
@app.callback(
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
    facilities = state_manager.get_all_facilities()
    
    return facilities


@app.callback(
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
        state_manager.set_filters(regions=filter_region)
    if filter_fuel_type is not None:
        state_manager.set_filters(fuel_types=filter_fuel_type)
    
    facilities = state_manager.get_all_facilities()
    
    # Create markers (Tooltip display power and emissions)
    markers = create_markers_from_facilities(facilities)
    
    # Create map component
    map_component = create_map_component(
        markers=markers,
        center=NEM_REGION_CENTER,
        zoom=5,
    )
    
    return map_component


@app.callback(
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


@app.callback(
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
        state_manager.set_filters(regions=selected_regions)
    if selected_fuel_types is not None:
        state_manager.set_filters(fuel_types=selected_fuel_types)
    
    # Get filtered facilities and calculate statistics
    filtered_facilities = state_manager.get_all_facilities()
    stats = calculate_statistics(filtered_facilities)
    
    # Create and return statistics UI
    return create_statistics_ui(stats)


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


@app.callback(
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
                create_market_chart(network_code=network_code, metric="price"),
                style={"flex": "1", "minHeight": "400px", "width": "100%"},
            ),
            html.Div(
                create_market_chart(network_code=network_code, metric="demand"),
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


@app.callback(
    Output("charts-store", "data"),
    Input("interval-component", "n_intervals"),
    State("charts-store", "data"),
)
def pull_charts_state(n: int, current: dict) -> dict:
    """Pull latest charts payload delivered via WS and cached on server."""
    latest = app.server.config.get("charts_latest") or app.server.config.get("charts_snapshot")
    return latest or current or {}


@app.callback(
    Output("fac-line-chart", "figure"),
    Output("fac-donut-chart", "figure"),
    Output("fac-region-bar", "figure"),
    Output("fac-topn-bar", "figure"),
    Input("charts-store", "data"),
    Input("main-tabs", "value"),
)
def render_facilities_charts(charts: dict, current_tab: str):
    """Render four facilities charts from charts-store (line, donut, region bar, Top-10 bar)."""
    import plotly.graph_objects as go
    
    if current_tab != "tab-facilities-charts" or not charts:
        return go.Figure(), go.Figure(), go.Figure(), go.Figure()
    
    # Line
    line_fig = go.Figure()
    for series in charts.get("line", []):
        xs = [pt.get("t") for pt in series.get("points", [])]
        ys = [pt.get("v") for pt in series.get("points", [])]
        raw_name = series.get("name")
        if raw_name == "total_power":
            trace_name = "Total Power (MW)"
        elif raw_name == "total_emissions":
            trace_name = "Total Emissions (t)"
        else:
            trace_name = raw_name
        line_fig.add_trace(go.Scatter(x=xs, y=ys, mode="lines", name=trace_name))
    line_fig.update_layout(title="Total Power & Emissions", template="plotly_white")
    
    # Donut
    labels = [s.get("fuel") for s in charts.get("donut", [])]
    values = [s.get("power", 0.0) for s in charts.get("donut", [])]
    donut_fig = go.Figure(go.Pie(labels=labels, values=values, hole=0.5, name="Power (MW)"))
    donut_fig.update_layout(title="Power Share by Fuel", template="plotly_white")
    
    # Region bar (power & emissions side-by-side for each region)
    regions_raw = [item.get("region") for item in charts.get("bar_by_region", [])]
    regions = [REGION_DISPLAY_MAP.get(r, r) for r in regions_raw]
    power_vals = [item.get("power", 0.0) for item in charts.get("bar_by_region", [])]
    emissions_vals = [item.get("emissions", 0.0) for item in charts.get("bar_by_region", [])]
    region_bar = go.Figure()
    region_bar.add_trace(go.Bar(x=regions, y=power_vals, name="Power (MW)", marker_color="#1f77b4"))
    region_bar.add_trace(go.Bar(x=regions, y=emissions_vals, name="Emissions (t)", marker_color="#ff7f0e"))
    region_bar.update_layout(barmode="group", title="By Region: Power & Emissions", template="plotly_white", xaxis_title="Region")
    
    # Top-10 facilities bar (sorted by power desc)
    names = [item.get("name") for item in charts.get("bar_top_facilities", [])]
    top_power = [item.get("power", 0.0) for item in charts.get("bar_top_facilities", [])]
    top_emissions = [item.get("emissions", 0.0) for item in charts.get("bar_top_facilities", [])]
    top_bar = go.Figure()
    top_bar.add_trace(go.Bar(x=names, y=top_power, name="Power (MW)", marker_color="#2ca02c"))
    top_bar.add_trace(go.Bar(x=names, y=top_emissions, name="Emissions (t)", marker_color="#d62728"))
    top_bar.update_layout(barmode="group", title="Top-10 Facilities: Power & Emissions", template="plotly_white", xaxis_title="Facility")
    
    return line_fig, donut_fig, region_bar, top_bar


# ========== Main program entry ==========
if __name__ == "__main__":
    # Run Dash application
    app.run(debug=True, host="0.0.0.0", port=8050)

