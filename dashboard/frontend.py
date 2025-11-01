"""Dash 前端应用 - 合并所有前端逻辑."""

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

# ========== 日志配置 ==========
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# ========== 常量定义 ==========
# NEM 网络区域
NEM_REGIONS = [
    "QLD1",  # Queensland
    "NSW1",  # New South Wales
    "VIC1",  # Victoria
    "SA1",   # South Australia
    "TAS1",  # Tasmania
]

# 常见燃料技术类型
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

# 澳大利亚 NEM 区域中心坐标
AUSTRALIA_CENTER = [-25.2744, 133.7751]  # 澳大利亚中心
NEM_REGION_CENTER = [-28.0, 145.0]  # NEM 区域中心（更偏向东部）

# ========== 前端状态管理器 ==========
class FrontendStateManager:
    """管理前端状态（设施数据、过滤条件、显示模式）."""
    
    def __init__(self) -> None:
        """初始化前端状态管理器."""
        self.facilities: Dict[str, dict] = {}
        self.filters: Dict[str, list] = {
            "regions": [],
            "fuel_types": [],
        }
        self.display_mode: str = "power"  # "power" 或 "emissions"
        self.selected_facility: Optional[str] = None
    
    def get_all_facilities(self) -> Dict[str, dict]:
        """
        获取所有设施的数据（应用过滤）.
        
        Returns:
            过滤后的设施数据字典
        """
        if not self.filters["regions"] and not self.filters["fuel_types"]:
            return self.facilities.copy()
        
        filtered = {}
        for facility_id, facility_data in self.facilities.items():
            metadata = facility_data.get("metadata", {})
            
            # 区域过滤
            if self.filters["regions"]:
                region = metadata.get("network_region", "")
                if region not in self.filters["regions"]:
                    continue
            
            # 燃料类型过滤
            if self.filters["fuel_types"]:
                fuel_type = metadata.get("fuel_technology", "")
                if fuel_type not in self.filters["fuel_types"]:
                    continue
            
            filtered[facility_id] = facility_data
        
        return filtered
    
    def set_filters(self, regions: Optional[list] = None, fuel_types: Optional[list] = None) -> None:
        """
        设置过滤条件.
        
        Args:
            regions: 网络区域列表
            fuel_types: 燃料类型列表
        """
        if regions is not None:
            self.filters["regions"] = regions
        if fuel_types is not None:
            self.filters["fuel_types"] = fuel_types
    
    def set_display_mode(self, mode: str) -> None:
        """
        设置显示模式.
        
        Args:
            mode: 显示模式（"power" 或 "emissions"）
        """
        if mode in ["power", "emissions"]:
            self.display_mode = mode
        else:
            logger.warning(f"Invalid display mode: {mode}")
    
    def set_selected_facility(self, facility_id: Optional[str]) -> None:
        """
        设置当前选中的设施.
        
        Args:
            facility_id: 设施 ID
        """
        self.selected_facility = facility_id
    
    def update_facility(self, facility_data: dict) -> None:
        """
        更新单个设施数据.
        
        Args:
            facility_data: 设施数据字典，必须包含 facility_id
        """
        facility_id = facility_data.get("facility_id")
        if not facility_id:
            logger.warning("Missing facility_id in facility_data")
            return
        
        self.facilities[facility_id] = facility_data
    
    def update_facilities_batch(self, facilities: Dict[str, dict]) -> None:
        """
        批量更新设施数据.
        
        Args:
            facilities: 设施数据字典 {facility_id: facility_data}
        """
        self.facilities.update(facilities)
    
    def get_facility(self, facility_id: str) -> Optional[dict]:
        """
        获取指定设施的数据.
        
        Args:
            facility_id: 设施 ID
        
        Returns:
            设施数据字典，如果不存在则返回 None
        """
        return self.facilities.get(facility_id)


# ========== 市场状态管理器 ==========
class MarketStateManager:
    """管理前端市场指标状态（支持无限滚动时间序列）."""
    
    def __init__(self, max_history: Optional[int] = None) -> None:
        """
        初始化市场状态管理器.
        
        Args:
            max_history: 最大历史数据数量，None 表示无限滚动（推荐用于实时图表）
        """
        # 存储格式: {network_code: {metric: [{"event_time": str, "value": float}]}}
        self._metrics: Dict[str, Dict[str, list]] = {}
        # 保留最近的历史数据数量，None 表示不限制（无限滚动）
        self._max_history: Optional[int] = max_history
    
    def update_metric(self, network_code: str, metric: str, event_time: str, value: float) -> None:
        """
        更新市场指标（支持无限滚动追加）.
        
        Args:
            network_code: 网络代码
            metric: 指标类型（如 "price", "demand"）
            event_time: 事件时间
            value: 指标值
        """
        if network_code not in self._metrics:
            self._metrics[network_code] = {}
        
        if metric not in self._metrics[network_code]:
            self._metrics[network_code][metric] = []
        
        # 添加到历史记录（实时追加）
        history = self._metrics[network_code][metric]
        history.append({"event_time": event_time, "value": value})
        
        # 仅在 max_history 不为 None 时限制历史数据数量
        if self._max_history is not None and len(history) > self._max_history:
            history.pop(0)
    
    def update_metrics_batch(self, metrics: Dict[str, Dict[str, dict | list]]) -> None:
        """
        批量更新市场指标.
        
        Args:
            metrics: 市场指标字典 {network_code: {metric: {event_time, value}} 或 [{"event_time", "value"}]}
            如果 metric_data 是字典，则作为单个数据点更新
            如果 metric_data 是列表，则作为历史数据批量更新
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
                    # 如果是列表，批量更新历史数据
                    if metric_data:
                        for data_point in metric_data:
                            if isinstance(data_point, dict):
                                event_time = data_point.get("event_time")
                                value = data_point.get("value")
                                if event_time and value is not None:
                                    self.update_metric(network_code, metric, event_time, value)
                                    count += 1
                        logger.debug(f"Updated {len(metric_data)} history points for {network_code}/{metric}")
                elif isinstance(metric_data, dict):
                    # 如果是字典，作为单个数据点更新
                    event_time = metric_data.get("event_time")
                    value = metric_data.get("value")
                    if event_time and value is not None:
                        self.update_metric(network_code, metric, event_time, value)
                        count += 1
                    else:
                        logger.debug(f"Missing event_time or value for {network_code}/{metric}: event_time={event_time}, value={value}")
                else:
                    logger.warning(f"Invalid metric_data format for {network_code}/{metric}: {type(metric_data)}")
        
        if count > 0:
            logger.info(f"Updated {count} market metrics from batch")
        else:
            logger.warning(f"update_metrics_batch processed {len(metrics)} networks but updated 0 metrics")
    
    def get_metric_history(self, network_code: str, metric: str) -> list:
        """
        获取指定指标的历史数据.
        
        Args:
            network_code: 网络代码
            metric: 指标类型
        
        Returns:
            历史数据列表
        """
        if network_code not in self._metrics:
            return []
        if metric not in self._metrics[network_code]:
            return []
        return self._metrics[network_code][metric].copy()
    
    def get_latest_value(self, network_code: str, metric: str) -> Optional[float]:
        """
        获取指定指标的最新值.
        
        Args:
            network_code: 网络代码
            metric: 指标类型
        
        Returns:
            最新值，如果不存在则返回 None
        """
        history = self.get_metric_history(network_code, metric)
        if not history:
            return None
        return history[-1]["value"]
    
    def get_all_metrics(self) -> Dict[str, Dict[str, list]]:
        """
        获取所有市场指标的历史数据.
        
        Returns:
            所有市场指标的历史数据
        """
        return self._metrics.copy()


# ========== WebSocket 客户端 ==========
class WebSocketClient:
    """WebSocket 客户端，连接到 FastAPI 后端."""
    
    def __init__(
        self,
        uri: str = "ws://localhost:8000/ws/realtime",
        message_callback: Optional[Callable[[dict], None]] = None,
    ) -> None:
        """
        初始化 WebSocket 客户端.
        
        Args:
            uri: WebSocket 服务器 URI
            message_callback: 消息回调函数
        """
        self.uri = uri
        self.message_callback = message_callback
        self.websocket: Optional[websockets.WebSocketClientProtocol] = None
        self._running = False
        self._loop: Optional[asyncio.AbstractEventLoop] = None
    
    async def connect(self) -> None:
        """连接到 WebSocket 服务器."""
        try:
            self.websocket = await websockets.connect(self.uri)
            logger.info(f"Connected to WebSocket server: {self.uri}")
        except Exception as e:
            logger.error(f"Failed to connect to WebSocket server: {e}")
            raise
    
    async def disconnect(self) -> None:
        """断开 WebSocket 连接."""
        if self.websocket:
            await self.websocket.close()
            logger.info("Disconnected from WebSocket server")
    
    async def send_message(self, message: dict) -> None:
        """
        发送消息到服务器.
        
        Args:
            message: 消息字典
        """
        if not self.websocket:
            logger.warning("WebSocket not connected")
            return
        
        try:
            await self.websocket.send(json.dumps(message))
        except Exception as e:
            logger.error(f"Failed to send message: {e}")
    
    async def listen(self) -> None:
        """监听 WebSocket 消息."""
        if not self.websocket:
            logger.error("WebSocket not connected")
            return
        
        self._running = True
        try:
            while self._running:
                try:
                    message = await self.websocket.recv()
                    data = json.loads(message)
                    
                    # 调用回调函数
                    if self.message_callback:
                        self.message_callback(data)
                    
                    logger.debug(f"Received message: {data.get('type', 'unknown')}")
                except WebSocketException as e:
                    logger.error(f"WebSocket error: {e}")
                    break
                except Exception as e:
                    logger.error(f"Error processing message: {e}")
                    break
        finally:
            self._running = False
    
    def stop(self) -> None:
        """停止监听."""
        self._running = False


# ========== UI 组件创建函数 ==========
def create_market_chart(network_code: str = "NEM", metric: str = "price") -> dcc.Graph:
    """
    创建实时滚动时间序列图表组件（固定窗口：最新50个数据点）.
    
    Args:
        network_code: 网络代码
        metric: 指标类型（如 "price", "demand"）
    
    Returns:
        Dash Graph 组件（固定窗口显示最新50个数据点）
    """
    history = market_state_manager.get_metric_history(network_code, metric)
    logger.debug(f"Creating chart: {network_code}/{metric}, history count: {len(history)}")
    
    if not history:
        # 如果没有数据，显示空图表（等待数据）
        fig = go.Figure()
        fig.add_annotation(
            text="等待数据中...<br><br>请确保：<br>1. MQTT发布器正在运行<br>2. 后端已连接MQTT代理<br>3. 市场数据正在发布",
            xref="paper",
            yref="paper",
            x=0.5,
            y=0.5,
            showarrow=False,
            font=dict(size=12),
        )
        fig.update_layout(
            title=f"{metric.capitalize()} - {network_code} (Latest 50 points)",
            xaxis_title="Time",
            yaxis_title=f"{metric.capitalize()}",
            height=400,
            xaxis=dict(type="date", autorange=True),
            yaxis=dict(autorange=True),
        )
        return dcc.Graph(id=f"market-chart-{network_code}-{metric}", figure=fig)
    
    # 固定窗口模式：只显示最新50个数据点（滑动窗口）
    max_display_points = 50
    if len(history) > max_display_points:
        # 只保留最新50个数据点
        display_history = history[-max_display_points:]
        logger.debug(f"Displaying {max_display_points} points (out of {len(history)} total) for {network_code}/{metric}")
    else:
        display_history = history
        logger.debug(f"Displaying {len(history)} points (less than {max_display_points}) for {network_code}/{metric}")
    
    # 提取时间和值（只显示最新50个点）
    # 过滤掉无效值（nan, None等）
    valid_data = [
        (item["event_time"], item["value"])
        for item in display_history
        if item.get("value") is not None 
        and not (isinstance(item["value"], float) and math.isnan(item["value"]))
    ]
    
    if not valid_data:
        # 如果没有有效数据，显示提示
        logger.warning(f"⚠️  No valid data for chart: {network_code}/{metric} (all values are NaN or None)")
        fig = go.Figure()
        fig.add_annotation(
            text="数据包含无效值<br>请检查数据源",
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
    
    # 调试：验证数据格式
    if times:
        logger.info(f"📊 Chart data: {len(times)} valid points (filtered from {len(display_history)}), first: {times[0]}, last: {times[-1]}")
        logger.info(f"📊 Chart values: min={min(values):.2f}, max={max(values):.2f}, avg={sum(values)/len(values):.2f}")
    
    # 计算X轴范围（固定窗口：基于最新50个数据点的时间范围）
    # Plotly 的 range 参数对于 type="date" 的 X 轴，可以直接使用日期字符串或时间戳
    try:
        if len(times) > 1:
            # 直接使用时间字符串（ISO 格式），Plotly 会自动解析
            # 添加一些边距（10%的时间范围）
            # 先解析时间戳计算边距
            time_objects = []
            for time_str in times:
                try:
                    # 尝试解析 ISO 格式时间戳
                    dt = datetime.fromisoformat(time_str.replace("Z", "+00:00"))
                    time_objects.append(dt)
                except Exception:
                    # 如果解析失败，跳过
                    continue
            
            if len(time_objects) >= 2:
                min_time = min(time_objects)
                max_time = max(time_objects)
                # 计算时间范围并添加10%边距
                time_range = (max_time - min_time).total_seconds()
                time_padding = time_range * 0.1 if time_range > 0 else 3600 * 0.1
                
                # 对于Plotly type="date"，range应该使用日期字符串或datetime对象
                # 创建带边距的datetime对象
                range_min = min_time - timedelta(seconds=time_padding)
                range_max = max_time + timedelta(seconds=time_padding)
                
                # 使用ISO格式字符串作为range（Plotly会自动解析）
                xaxis_range = [
                    range_min.isoformat(),
                    range_max.isoformat(),
                ]
                logger.info(f"📏 Calculated X-axis range: {len(display_history)} points, time range: {time_range:.0f}s, range: [{xaxis_range[0]}, {xaxis_range[1]}]")
            else:
                # 如果只有一个或没有有效时间点，使用自动范围
                xaxis_range = None
        elif len(times) == 1:
            # 只有一个数据点，使用该点前后的时间范围
            try:
                dt = datetime.fromisoformat(times[0].replace("Z", "+00:00"))
                # 使用当前时间点前后1小时作为范围
                time_padding = timedelta(hours=1)  # 1小时
                xaxis_range = [
                    (dt - time_padding).isoformat(),
                    (dt + time_padding).isoformat(),
                ]
            except Exception:
                xaxis_range = None
        else:
            xaxis_range = None
    except Exception as e:
        logger.warning(f"Failed to calculate X-axis range: {e}, using autorange")
        xaxis_range = None
    
    # 创建时间序列折线图
    # 确保时间字符串格式正确（Plotly需要ISO格式）
    formatted_times = []
    for time_str in times:
        try:
            # 确保格式统一：ISO 8601格式
            dt = datetime.fromisoformat(time_str.replace("Z", "+00:00"))
            # 使用ISO格式字符串（Plotly会自动解析）
            formatted_times.append(dt.isoformat())
        except Exception:
            # 如果解析失败，使用原始字符串
            formatted_times.append(time_str)
    
    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=formatted_times,  # 使用格式化后的时间字符串
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
    
    logger.info(f"📈 Created chart with {len(formatted_times)} time points, {len(values)} value points")
    
    # 更新布局（固定窗口配置）
    unit = "($/MWh)" if metric == "price" else "(MW)"
    
    # 构建 X 轴配置
    xaxis_config = dict(
        type="date",  # 时间轴类型
        automargin=True,  # 自动边距
        rangeslider=dict(visible=False),  # 隐藏范围滑块
        showspikes=True,  # 显示时间轴上的尖峰指示器
        spikecolor="gray",
        spikethickness=1,
        fixedrange=False,  # 允许用户缩放查看（可选）
    )
    
    # 使用自动范围（更可靠，Plotly会自动适应数据范围）
    # 注释掉固定范围，使用autorange确保图表能正确显示
    xaxis_config["autorange"] = True  # 自动范围（适应最新50个点的时间范围）
    if xaxis_range is not None:
        logger.info(f"📊 Calculated X-axis range but using autorange: [{xaxis_range[0]}, {xaxis_range[1]}]")
    else:
        logger.info(f"🔓 Using autorange for X-axis (no fixed range calculated)")
    
    fig.update_layout(
        title=f"{metric.capitalize()} - {network_code} {unit} (Latest {len(display_history)} points)",
        xaxis_title="Time",
        yaxis_title=f"{metric.capitalize()} {unit}",
        height=400,  # 增加图表高度，便于查看
        margin=dict(l=50, r=20, t=50, b=50),
        hovermode="x unified",
        template="plotly_white",
        # 固定窗口配置：X轴固定范围或自动范围（基于最新50个点）
        xaxis=xaxis_config,
        # 使用 uirevision 确保每次更新时视图重置到最新数据
        # 使用最新数据点的时间戳作为 uirevision，确保新数据到来时视图重置
        # 对于固定窗口，每次新数据到来时，最新数据点的时间戳会变化，从而重置视图
        uirevision=(
            display_history[-1].get("event_time", "") if display_history else ""
        ),
        # Y 轴自动调整范围
        yaxis=dict(
            autorange=True,  # 自动调整 Y 轴范围（基于显示的数据）
            automargin=True,  # 自动边距
            showspikes=True,  # 显示 Y 轴尖峰指示器
            spikecolor="gray",
            spikethickness=1,
        ),
        # 允许用户交互（缩放、平移），但视图会始终聚焦最新数据
        dragmode="pan",  # 默认平移模式
    )
    
    return dcc.Graph(
        id=f"market-chart-{network_code}-{metric}",
        figure=fig,
        config={
            "displayModeBar": True,  # 显示工具栏
            "displaylogo": False,  # 隐藏 Plotly 徽标
            "scrollZoom": True,  # 启用滚动缩放（便于查看历史数据）
            "doubleClick": "reset",  # 双击重置视图
            "modeBarButtonsToRemove": ["lasso2d", "select2d"],  # 移除不需要的工具
        },
    )


def create_filter_panel() -> html.Div:
    """
    创建过滤面板组件.
    
    Returns:
        Dash Div 组件，包含所有过滤控件
    """
    return html.Div(
        [
            html.H4("Filters", style={"marginBottom": "20px"}),
            
            # 网络区域过滤
            html.Div(
                [
                    html.Label("Network Region:", style={"fontWeight": "bold"}),
                    dcc.Dropdown(
                        id="filter-region",
                        options=[{"label": region, "value": region} for region in NEM_REGIONS],
                        multi=True,
                        placeholder="Select regions...",
                        style={"marginBottom": "20px"},
                    ),
                ]
            ),
            
            # 燃料技术过滤
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
            
            # 显示模式切换
            html.Div(
                [
                    html.Label("Display Mode:", style={"fontWeight": "bold"}),
                    dcc.RadioItems(
                        id="display-mode",
                        options=[
                            {"label": "Power (MW)", "value": "power"},
                            {"label": "Emissions (t)", "value": "emissions"},
                        ],
                        value="power",
                        style={"marginBottom": "20px"},
                    ),
                ]
            ),
            
            # 状态显示
            html.Div(
                [
                    html.Hr(),
                    html.P(id="status-text", children="Ready", style={"fontSize": "12px", "color": "gray"}),
                ]
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
    根据功率值返回颜色.
    
    Args:
        power: 功率值（MW）
    
    Returns:
        颜色名称（用于 marker 图标）
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
    根据排放值返回颜色.
    
    Args:
        emissions: 排放值（t）
    
    Returns:
        颜色名称（用于 marker 图标）
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
    display_mode: str = "power",
) -> dl.Marker:
    """
    创建设施标记.
    
    Args:
        facility_id: 设施 ID
        latitude: 纬度
        longitude: 经度
        name: 设施名称
        power: 功率值
        emissions: 排放值
        display_mode: 显示模式（"power" 或 "emissions"）
    
    Returns:
        Dash Leaflet Marker 组件
    """
    # 根据显示模式选择值和颜色
    if display_mode == "power":
        value = power
        color = get_power_color(power)
        unit = "MW"
        label_text = f"{name}<br>{power:.2f} {unit}"
    else:
        value = emissions
        color = get_emissions_color(emissions)
        unit = "t"
        label_text = f"{name}<br>{emissions:.2f} {unit}"
    
    # 创建标记
    # 注意：dash-leaflet 不支持 Icon 组件，使用默认图标
    marker = dl.Marker(
        position=[latitude, longitude],
        id={"type": "facility-marker", "id": facility_id},
        children=[
            dl.Tooltip(label_text),
            dl.Popup(
                html.Div(
                    [
                        html.H4(name),
                        html.P(f"Facility ID: {facility_id}"),
                        html.Hr(),
                        html.P(f"Power: {power:.2f} MW"),
                        html.P(f"Emissions: {emissions:.2f} t"),
                        html.P("Click to view details"),
                    ],
                    style={"padding": "10px"},
                )
            ),
        ],
        title=name,  # 鼠标悬停时显示的文本
    )
    
    return marker


def create_markers_from_facilities(
    facilities: Dict[str, dict],
    display_mode: str = "power",
) -> list:
    """
    从设施数据创建标记列表.
    
    Args:
        facilities: 设施数据字典
        display_mode: 显示模式（"power" 或 "emissions"）
    
    Returns:
        标记列表
    """
    markers = []
    
    for facility_id, facility_data in facilities.items():
        # 获取位置信息
        latitude = facility_data.get("latitude")
        longitude = facility_data.get("longitude")
        
        # 如果位置信息缺失，跳过
        if latitude is None or longitude is None:
            logger.warning(f"Missing location for facility {facility_id}")
            continue
        
        # 获取元数据
        metadata = facility_data.get("metadata", {})
        name = metadata.get("name", facility_id)
        
        # 获取功率和排放值
        power = facility_data.get("power", 0.0)
        emissions = facility_data.get("emissions", 0.0)
        
        # 创建标记
        marker = create_facility_marker(
            facility_id=facility_id,
            latitude=float(latitude),
            longitude=float(longitude),
            name=name,
            power=float(power),
            emissions=float(emissions),
            display_mode=display_mode,
        )
        
        markers.append(marker)
    
    return markers


def create_map_component(
    markers: Optional[list] = None,
    center: Optional[list] = None,
    zoom: int = 5,
) -> dl.Map:
    """
    创建 Leaflet 地图组件.
    
    Args:
        markers: 标记列表
        center: 地图中心坐标 [lat, lng]
        zoom: 初始缩放级别
    
    Returns:
        Dash Leaflet Map 组件
    """
    if center is None:
        center = NEM_REGION_CENTER
    
    if markers is None:
        markers = []
    
    # 创建地图组件列表
    map_children = [
        dl.TileLayer(
            url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png",
            attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors',
        ),
    ]
    
    # 如果有标记，使用 LayerGroup 来包含它们
    if markers:
        map_children.append(
            dl.LayerGroup(id="marker-layer", children=markers)
        )
    
    # 创建地图
    map_component = dl.Map(
        map_children,
        id="map",
        center=center,
        zoom=zoom,
        style={"width": "100%", "height": "100%"},
    )
    
    return map_component


# ========== Dash 应用初始化 ==========
app = dash.Dash(__name__)
app.title = "Electricity Facility Dashboard"

# 初始化状态管理器
state_manager = FrontendStateManager()
# 初始化市场状态管理器（无限滚动模式，支持实时追加）
market_state_manager = MarketStateManager(max_history=None)

# WebSocket 客户端
websocket_client: WebSocketClient | None = None


# ========== WebSocket 消息处理 ==========
def websocket_message_handler(message: dict) -> None:
    """
    WebSocket 消息处理函数.
    
    Args:
        message: 收到的消息字典
    """
    message_type = message.get("type")
    data = message.get("data", {})
    
    if message_type == "facility_update":
        # 更新单个设施
        state_manager.update_facility(data)
        logger.debug(f"Updated facility: {data.get('facility_id')}")
    
    elif message_type == "initial_data":
        # 批量更新所有设施
        facilities = data.get("facilities", {})
        state_manager.update_facilities_batch(facilities)
        logger.info(f"Loaded initial data: {len(facilities)} facilities")
    
    elif message_type == "market_update":
        # 更新单个市场指标
        network_code = data.get("network_code")
        metric = data.get("metric")
        event_time = data.get("event_time")
        value = data.get("value")
        if network_code and metric and event_time and value is not None:
            # 检查值是否为NaN
            if isinstance(value, float) and math.isnan(value):
                logger.warning(f"⚠️  Received NaN value for {network_code}/{metric}, skipping update")
            else:
                market_state_manager.update_metric(network_code, metric, event_time, value)
                # 验证数据是否已存储
                history_count = len(market_state_manager.get_metric_history(network_code, metric))
                logger.info(f"✓ Market data received: {network_code}/{metric} = {value} at {event_time} (history: {history_count} points)")
        else:
            logger.warning(f"✗ Invalid market_update message: network_code={network_code}, metric={metric}, event_time={event_time}, value={value}")
    
    elif message_type == "initial_market_data":
        # 批量更新所有市场指标
        metrics = data.get("metrics", {})
        if metrics:
            market_state_manager.update_metrics_batch(metrics)
            logger.info(f"Loaded initial market data: {len(metrics)} networks")
        else:
            # 空数据是正常的（连接时可能还没有MQTT数据）
            logger.debug("Received empty initial market data (this is normal if MQTT data hasn't arrived yet)")


def setup_websocket_client() -> None:
    """设置 WebSocket 客户端."""
    global websocket_client
    
    try:
        client = WebSocketClient(
            uri="ws://localhost:8000/ws/realtime",
            message_callback=websocket_message_handler,
        )
        
        async def run_client() -> None:
            """运行 WebSocket 客户端."""
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
        
        # 在新线程中运行
        def run_in_thread() -> None:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                loop.run_until_complete(run_client())
            except Exception as e:
                logger.error(f"WebSocket client thread error: {e}")
            finally:
                loop.close()
        
        # 延迟启动，等待 FastAPI 服务器启动
        def delayed_start() -> None:
            time.sleep(2)  # 等待后端启动
            thread = threading.Thread(target=run_in_thread, daemon=True)
            thread.start()
        
        thread = threading.Thread(target=delayed_start, daemon=True)
        thread.start()
        websocket_client = client
        
        logger.info("WebSocket client setup initiated")
    except Exception as e:
        logger.error(f"Failed to setup WebSocket client: {e}")


# 设置 WebSocket 客户端（在应用启动时）
setup_websocket_client()

# ========== 应用布局 ==========
app.layout = html.Div(
    [
        # 状态存储
        dcc.Store(id="facilities-store", data={}),
        dcc.Store(id="display-mode-store", data="power"),
        dcc.Store(id="market-metrics-store", data={}),
        
        # 标题
        html.H1("Electricity Facility Dashboard", style={"padding": "20px", "textAlign": "center"}),
        
        # 标签页容器
        dcc.Tabs(
            id="main-tabs",
            value="tab-map",  # 默认选中地图视图
            children=[
                # 标签页1: 地图视图
                dcc.Tab(
                    label="地图视图",
                    value="tab-map",
                    children=html.Div(
                        [
                            # 地图视图布局
                            html.Div(
                                [
                                    # 左侧过滤面板
                                    create_filter_panel(),
                                    
                                    # 右侧地图区域
                                    html.Div(
                                        [
                                            html.H2("Facilities Map", style={"padding": "10px"}),
                                            
                                            # 地图容器
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
                
                # 标签页2: 市场图表
                dcc.Tab(
                    label="市场图表",
                    value="tab-market",
                    children=html.Div(
                        [
                            html.H2("Market Metrics", style={"padding": "20px", "textAlign": "center"}),
                            
                            # 指标选择器区域
                            html.Div(
                                [
                                    html.Label("Metric:", style={"fontWeight": "bold", "marginRight": "10px"}),
                                    dcc.Dropdown(
                                        id="market-metric-selector",
                                        options=[
                                            {"label": "Price", "value": "price"},
                                            {"label": "Demand", "value": "demand"},
                                        ],
                                        value="price",
                                        style={"width": "200px", "display": "inline-block", "marginRight": "20px"},
                                    ),
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
                            
                            # 图表区域
                            html.Div(
                                id="market-charts-container",
                                children=[],
                                style={
                                    "width": "100%",
                                    "height": "calc(100vh - 250px)",
                                    "minHeight": "500px",
                                    "padding": "20px",
                                },
                            ),
                        ],
                    ),
                ),
            ],
            style={"width": "100%", "marginBottom": "20px"},
        ),
        
        # 定期更新组件（用于触发回调）
        dcc.Interval(
            id="interval-component",
            interval=1000,  # 每秒更新一次
            n_intervals=0,
        ),
    ]
)


# ========== 回调函数定义 ==========
@app.callback(
    Output("facilities-store", "data"),
    Output("display-mode-store", "data"),
    Input("interval-component", "n_intervals"),
    Input("display-mode", "value"),
    State("facilities-store", "data"),
    State("main-tabs", "value"),  # 添加标签页状态
)
def update_facilities_store(
    n_intervals: int,
    display_mode: str,
    current_store: dict,
    current_tab: str,  # 当前选中的标签页
) -> tuple[dict, str]:
    """
    更新设施数据存储（从状态管理器读取）.
    
    仅在"地图视图"标签页激活时更新，以优化性能。
    
    Args:
        n_intervals: 间隔计数器
        display_mode: 显示模式
        current_store: 当前存储的数据
        current_tab: 当前选中的标签页
    
    Returns:
        (更新后的设施数据, 显示模式)
    """
    # 仅在地图视图标签页激活时更新设施数据
    if current_tab != "tab-map":
        # 如果不是地图视图，保持当前数据不变
        return current_store or {}, display_mode or "power"
    
    # 从状态管理器获取所有设施（应用过滤）
    facilities = state_manager.get_all_facilities()
    
    # 更新显示模式
    if display_mode:
        state_manager.set_display_mode(display_mode)
    
    return facilities, display_mode or "power"


@app.callback(
    Output("map-container", "children"),
    Input("facilities-store", "data"),
    Input("display-mode-store", "data"),
    Input("filter-region", "value"),
    Input("filter-fuel-type", "value"),
    State("main-tabs", "value"),  # 添加标签页状态
)
def update_map(
    facilities_store: dict,
    display_mode: str,
    filter_region: list | None,
    filter_fuel_type: list | None,
    current_tab: str,  # 当前选中的标签页
) -> Any:
    """
    更新地图组件.
    
    仅在"地图视图"标签页激活时更新，以优化性能。
    
    Args:
        facilities_store: 设施数据存储
        display_mode: 显示模式
        filter_region: 区域过滤值
        filter_fuel_type: 燃料类型过滤值
        current_tab: 当前选中的标签页
    
    Returns:
        更新的地图组件
    """
    # 仅在地图视图标签页激活时更新地图
    if current_tab != "tab-map":
        # 如果不是地图视图，返回不更新
        return no_update
    
    # 更新过滤条件
    if filter_region is not None:
        state_manager.set_filters(regions=filter_region)
    if filter_fuel_type is not None:
        state_manager.set_filters(fuel_types=filter_fuel_type)
    
    # 获取过滤后的设施
    facilities = state_manager.get_all_facilities()
    
    # 获取显示模式
    mode = display_mode or "power"
    
    # 创建标记
    markers = create_markers_from_facilities(facilities, display_mode=mode)
    
    # 创建地图组件
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
    更新状态文本.
    
    Args:
        facilities_store: 设施数据存储
    
    Returns:
        状态文本
    """
    facility_count = len(facilities_store)
    return f"Facilities: {facility_count} | Status: Active"


@app.callback(
    Output("market-charts-container", "children"),
    Input("market-metric-selector", "value"),
    Input("market-network-selector", "value"),
    Input("interval-component", "n_intervals"),
    Input("main-tabs", "value"),  # 改为Input，切换标签页时触发更新
)
def update_market_charts(
    metric: str,
    network_code: str,
    n_intervals: int,
    current_tab: str,  # 当前选中的标签页
) -> Any:
    """
    更新市场数据图表（固定窗口：最新50个数据点）.
    
    该回调函数每秒触发一次（通过 interval-component），
    重新渲染图表以显示最新的时间序列数据。
    图表采用固定窗口模式，只显示最新50个数据点，实现滑动窗口效果。
    仅在"市场图表"标签页激活时更新，以优化性能。
    
    Args:
        metric: 指标类型（如 "price", "demand"）
        network_code: 网络代码（如 "NEM"）
        n_intervals: 间隔计数器（用于触发更新）
        current_tab: 当前选中的标签页
    
    Returns:
        更新的图表组件（包含最新50个数据点）
    """
    if not metric or not network_code:
        return []
    
    # 仅在图表视图标签页激活时更新图表
    if current_tab != "tab-market":
        # 如果不是图表视图，返回空组件（不显示图表）
        logger.info(f"⏭️  Chart update skipped: current tab is '{current_tab}', not 'tab-market'")
        return []  # 返回空组件，而不是no_update，确保切换标签页时能清除旧图表
    
    # 获取当前历史数据，用于调试
    history = market_state_manager.get_metric_history(network_code, metric)
    logger.info(f"🔄 Updating chart: {network_code}/{metric}, history count: {len(history)}, tab: {current_tab}, intervals: {n_intervals}")
    
    # 每次更新时重新创建图表，显示最新50个数据点（固定窗口）
    chart = create_market_chart(network_code=network_code, metric=metric)
    logger.info(f"✅ Chart updated successfully: {network_code}/{metric}, data points: {len(history)}")
    return chart


# ========== 主程序入口 ==========
if __name__ == "__main__":
    # 运行 Dash 应用
    app.run(debug=True, host="0.0.0.0", port=8050)

