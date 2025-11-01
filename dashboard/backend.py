"""FastAPI 后端服务 - 合并所有后端逻辑."""

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
    # 如果直接运行，使用相对导入
    import sys
    from pathlib import Path
    sys.path.insert(0, str(Path(__file__).parent))
    from schemas import FacilityUpdate, MQTTMessage, MarketMetricsMessage, MarketUpdate

# ========== 日志配置 ==========
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# ========== 数据模型 ==========
# 所有数据模型定义在 schemas.py 中

# ========== 状态管理器 ==========
class StateManager:
    """管理设施状态（内存中）."""
    
    def __init__(self) -> None:
        """初始化状态管理器."""
        self._state: Dict[str, dict] = {}
    
    def update_facility(
        self,
        facility_id: str,
        mqtt_message: MQTTMessage,
        metadata: Optional[dict] = None,
    ) -> dict:
        """
        更新设施状态.
        
        Args:
            facility_id: 设施 ID
            mqtt_message: MQTT 消息
            metadata: 设施元数据（可选）
        
        Returns:
            更新后的完整设施数据
        """
        # 解析事件时间
        try:
            event_time = datetime.fromisoformat(
                mqtt_message.event_time.replace("Z", "+00:00")
            )
        except Exception as e:
            logger.warning(f"Failed to parse event_time: {e}, using current time")
            event_time = datetime.now()
        
        # 构建最新数据
        latest_data = {
            "event_time": mqtt_message.event_time,
            "power": mqtt_message.power,
            "emissions": mqtt_message.emissions,
            "longitude": mqtt_message.longitude,
            "latitude": mqtt_message.latitude,
        }
        
        # 合并元数据（如果提供）
        if metadata:
            # 如果 MQTT 消息中没有地理位置，使用元数据中的
            if latest_data["longitude"] is None and metadata.get("longitude"):
                latest_data["longitude"] = metadata["longitude"]
            if latest_data["latitude"] is None and metadata.get("latitude"):
                latest_data["latitude"] = metadata["latitude"]
        
        # 更新状态
        if facility_id not in self._state:
            self._state[facility_id] = {
                "latest": latest_data,
                "metadata": metadata or {},
            }
        else:
            self._state[facility_id]["latest"] = latest_data
            if metadata:
                self._state[facility_id]["metadata"].update(metadata)
        
        # 返回完整数据（用于 WebSocket 推送）
        return {
            "facility_id": facility_id,
            **latest_data,
            "metadata": self._state[facility_id]["metadata"],
        }
    
    def get_facility(self, facility_id: str) -> Optional[dict]:
        """
        获取指定设施的状态.
        
        Args:
            facility_id: 设施 ID
        
        Returns:
            设施数据字典，如果不存在则返回 None
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
        获取所有设施的状态.
        
        Returns:
            所有设施的数据字典
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
        """清空所有状态."""
        self._state.clear()
        logger.info("State cleared")


# ========== 市场状态管理器 ==========
class MarketStateManager:
    """管理市场指标状态（内存中）."""
    
    def __init__(self) -> None:
        """初始化市场状态管理器."""
        # 存储格式: {network_code: {metric: {latest_value, latest_time, history}}}
        self._state: Dict[str, Dict[str, dict]] = {}
        # 历史数据保留数量（可选）
        self._max_history: int = 100
    
    def update_metric(
        self,
        network_code: str,
        metric: str,
        event_time: str,
        value: float,
    ) -> dict:
        """
        更新市场指标.
        
        Args:
            network_code: 网络代码
            metric: 指标类型（如 "price", "demand"）
            event_time: 事件时间
            value: 指标值
        
        Returns:
            更新后的指标数据
        """
        if network_code not in self._state:
            self._state[network_code] = {}
        
        if metric not in self._state[network_code]:
            # 初始化指标
            self._state[network_code][metric] = {
                "latest_value": value,
                "latest_time": event_time,
                "history": [{"event_time": event_time, "value": value}],  # 初始数据也添加到历史记录
            }
        else:
            # 更新最新值和时间
            self._state[network_code][metric]["latest_value"] = value
            self._state[network_code][metric]["latest_time"] = event_time
            
            # 添加到历史记录（限制数量）
            history = self._state[network_code][metric]["history"]
            history.append({"event_time": event_time, "value": value})
            if len(history) > self._max_history:
                history.pop(0)
        
        # 返回最新数据
        return {
            "network_code": network_code,
            "metric": metric,
            "event_time": event_time,
            "value": value,
        }
    
    def get_metric(self, network_code: str, metric: str) -> Optional[dict]:
        """
        获取指定市场指标.
        
        Args:
            network_code: 网络代码
            metric: 指标类型
        
        Returns:
            指标数据字典，如果不存在则返回 None
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
        获取所有市场指标的历史数据（用于前端图表显示）.
        
        Returns:
            所有市场指标的历史数据字典 {network_code: {metric: [{"event_time": str, "value": float}]}}
        """
        result = {}
        for network_code, metrics in self._state.items():
            result[network_code] = {}
            for metric, data in metrics.items():
                # 构建历史数据列表（包含最新值）
                history = data.get("history", [])
                if history:
                    # 如果有历史数据，直接返回
                    result[network_code][metric] = history.copy()
                else:
                    # 如果没有历史数据但有最新值，创建一个包含最新值的列表
                    if "latest_time" in data and "latest_value" in data:
                        result[network_code][metric] = [
                            {
                                "event_time": data["latest_time"],
                                "value": data["latest_value"],
                            }
                        ]
                    else:
                        # 如果既没有历史数据也没有最新值，跳过这个指标
                        continue
        return result
    
    def clear(self) -> None:
        """清空所有状态."""
        self._state.clear()
        logger.info("Market state cleared")


# ========== 市场数据处理器 ==========
class MarketDataProcessor:
    """处理市场MQTT消息并更新状态."""
    
    def __init__(
        self,
        market_state_manager: MarketStateManager,
    ) -> None:
        """
        初始化市场数据处理器.
        
        Args:
            market_state_manager: 市场状态管理器
        """
        self.market_state_manager = market_state_manager
    
    def process_message(self, mqtt_message: MarketMetricsMessage) -> Optional[dict]:
        """
        处理市场MQTT消息.
        
        Args:
            mqtt_message: 市场MQTT消息
        
        Returns:
            处理后的市场指标数据（包含最新值），如果处理失败则返回 None
        """
        try:
            # 验证消息格式
            if not mqtt_message.network_code:
                logger.warning("Missing network_code in market MQTT message")
                return None
            if not mqtt_message.metric:
                logger.warning("Missing metric in market MQTT message")
                return None
            
            # 更新状态并返回完整数据
            metric_data = self.market_state_manager.update_metric(
                mqtt_message.network_code,
                mqtt_message.metric,
                mqtt_message.event_time,
                mqtt_message.value,
            )
            
            logger.debug(
                f"Processed market message: network_code={mqtt_message.network_code}, "
                f"metric={mqtt_message.metric}, value={mqtt_message.value}"
            )
            
            return metric_data
        except Exception as e:
            logger.error(f"Failed to process market MQTT message: {e}", exc_info=True)
            return None


# ========== 元数据加载器 ==========
class MetadataLoader:
    """加载设施元数据（从 facilities.csv）."""
    
    def __init__(self, csv_path: str | Path) -> None:
        """
        初始化元数据加载器.
        
        Args:
            csv_path: facilities.csv 文件路径
        """
        self.csv_path = Path(csv_path)
        self._metadata: Dict[str, dict] = {}
        self._load_metadata()
    
    def _load_metadata(self) -> None:
        """从 CSV 文件加载元数据."""
        if not self.csv_path.exists():
            logger.warning(f"Facilities CSV not found: {self.csv_path}")
            return
        
        try:
            df = pd.read_csv(self.csv_path)
            logger.info(f"Loading metadata from {self.csv_path}, found {len(df)} facilities")
            
            # 保留 code 列用于匹配，但也要支持 facility_id
            if "facility_id" not in df.columns and "code" in df.columns:
                df["facility_id"] = df["code"]
            
            for _, row in df.iterrows():
                # 支持 code 或 facility_id 列
                facility_id = str(row.get("facility_id") or row.get("code", ""))
                if not facility_id:
                    continue
                try:
                    # 使用 lat/lng 作为 latitude/longitude 的别名
                    metadata_dict = row.to_dict()
                    if "lat" in metadata_dict and "latitude" not in metadata_dict:
                        metadata_dict["latitude"] = metadata_dict["lat"]
                    if "lng" in metadata_dict and "longitude" not in metadata_dict:
                        metadata_dict["longitude"] = metadata_dict["lng"]
                    
                    # 创建元数据字典（兼容 MQTT 消息格式）
                    # 处理 lat/lng 和 latitude/longitude 的兼容性
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
        """从描述或类型中提取燃料技术类型."""
        # 尝试从 description 中提取
        description = str(metadata_dict.get("description", "")).lower()
        name = str(metadata_dict.get("name", "")).lower()
        
        # 常见燃料技术类型
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
            if any(keyword in description or keyword in name for keyword in keywords):
                return fuel.capitalize()
        
        return "Unknown"
    
    def get_metadata(self, facility_id: str) -> Optional[dict]:
        """
        获取指定设施的元数据.
        
        Args:
            facility_id: 设施 ID
        
        Returns:
            元数据字典，如果不存在则返回 None
        """
        return self._metadata.get(facility_id)
    
    def get_all_metadata(self) -> Dict[str, dict]:
        """
        获取所有设施的元数据.
        
        Returns:
            所有设施的元数据字典
        """
        return self._metadata.copy()


# ========== 数据处理器 ==========
class DataProcessor:
    """处理 MQTT 消息并合并元数据."""
    
    def __init__(
        self,
        state_manager: StateManager,
        metadata_loader: MetadataLoader,
    ) -> None:
        """
        初始化数据处理器.
        
        Args:
            state_manager: 状态管理器
            metadata_loader: 元数据加载器
        """
        self.state_manager = state_manager
        self.metadata_loader = metadata_loader
    
    def process_message(self, mqtt_message: MQTTMessage) -> Optional[dict]:
        """
        处理 MQTT 消息.
        
        Args:
            mqtt_message: MQTT 消息
        
        Returns:
            处理后的设施数据（包含元数据），如果处理失败则返回 None
        """
        try:
            # 验证消息格式
            if not mqtt_message.facility_id:
                logger.warning("Missing facility_id in MQTT message")
                return None
            
            # 获取元数据
            metadata = self.metadata_loader.get_metadata(mqtt_message.facility_id)
            
            # 如果元数据不存在，使用默认值
            if not metadata:
                logger.warning(
                    f"Metadata not found for facility {mqtt_message.facility_id}, using defaults"
                )
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
            
            # 更新状态并返回完整数据
            facility_data = self.state_manager.update_facility(
                mqtt_message.facility_id,
                mqtt_message,
                metadata,
            )
            
            logger.debug(
                f"Processed message for facility {mqtt_message.facility_id}: "
                f"power={mqtt_message.power}, emissions={mqtt_message.emissions}"
            )
            
            return facility_data
        except Exception as e:
            logger.error(f"Failed to process MQTT message: {e}", exc_info=True)
            return None


# ========== MQTT 订阅服务 ==========
class MQTTSubscriber:
    """MQTT 订阅服务."""
    
    def __init__(
        self,
        broker_host: str = "broker.hivemq.com",
        broker_port: int = 1883,
        topic: str = "a02/facility_metrics/v1/stream",
        qos: int = 0,
        message_callback: Optional[Callable[[MQTTMessage], None]] = None,
    ) -> None:
        """
        初始化 MQTT 订阅服务.
        
        Args:
            broker_host: MQTT 代理主机
            broker_port: MQTT 代理端口
            topic: 订阅主题
            qos: QoS 级别
            message_callback: 消息回调函数
        """
        self.broker_host = broker_host
        self.broker_port = broker_port
        self.topic = topic
        self.qos = qos
        self.message_callback = message_callback
        self.client: Optional[mqtt.Client] = None
        self._connected = False
    
    def _on_connect(self, client: mqtt.Client, userdata: dict, flags: dict, rc: int) -> None:
        """MQTT 连接回调."""
        if rc == 0:
            logger.info(f"Connected to MQTT broker {self.broker_host}:{self.broker_port}")
            self._connected = True
            # 订阅主题
            client.subscribe(self.topic, self.qos)
            logger.info(f"Subscribed to topic: {self.topic}")
        else:
            logger.error(f"Failed to connect to MQTT broker, rc={rc}")
            self._connected = False
    
    def _on_disconnect(self, client: mqtt.Client, userdata: dict, rc: int) -> None:
        """MQTT 断开连接回调."""
        logger.warning(f"Disconnected from MQTT broker, rc={rc}")
        self._connected = False
    
    def _on_message(self, client: mqtt.Client, userdata: dict, msg: mqtt.MQTTMessage) -> None:
        """MQTT 消息接收回调."""
        try:
            # 解析 JSON 消息
            payload_str = msg.payload.decode("utf-8")
            payload_dict = json.loads(payload_str)
            
            # 转换为 Pydantic 模型
            mqtt_message = MQTTMessage(**payload_dict)
            
            # 调用回调函数
            if self.message_callback:
                self.message_callback(mqtt_message)
            
            logger.debug(
                f"Received MQTT message: facility_id={mqtt_message.facility_id}, "
                f"power={mqtt_message.power}, emissions={mqtt_message.emissions}"
            )
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse MQTT message JSON: {e}")
        except Exception as e:
            logger.error(f"Error processing MQTT message: {e}", exc_info=True)
    
    def start(self) -> None:
        """启动 MQTT 订阅服务."""
        if self.client is not None:
            logger.warning("MQTT client already started")
            return
        
        # 创建 MQTT 客户端
        self.client = mqtt.Client(
            client_id=f"dashboard_subscriber_{id(self)}",
            protocol=mqtt.MQTTv311,
        )
        
        # 设置回调
        self.client.on_connect = self._on_connect
        self.client.on_disconnect = self._on_disconnect
        self.client.on_message = self._on_message
        
        # 连接到代理
        try:
            rc = self.client.connect(self.broker_host, self.broker_port, keepalive=60)
            if rc != 0:
                raise RuntimeError(f"MQTT connect failed, rc={rc}")
            
            # 启动网络循环（非阻塞）
            self.client.loop_start()
            logger.info("MQTT subscriber started")
        except Exception as e:
            logger.error(f"Failed to start MQTT subscriber: {e}")
            raise
    
    def stop(self) -> None:
        """停止 MQTT 订阅服务."""
        if self.client is None:
            return
        
        self.client.loop_stop()
        self.client.disconnect()
        self._connected = False
        self.client = None
        logger.info("MQTT subscriber stopped")
    
    def is_connected(self) -> bool:
        """检查是否已连接."""
        return self._connected and self.client is not None and self.client.is_connected()


# ========== WebSocket 管理器 ==========
class WebSocketManager:
    """管理 WebSocket 连接并广播消息."""
    
    def __init__(self) -> None:
        """初始化 WebSocket 管理器."""
        self.active_connections: Set[WebSocket] = set()
    
    async def connect(self, websocket: WebSocket) -> None:
        """
        接受 WebSocket 连接.
        
        Args:
            websocket: WebSocket 连接对象
        """
        await websocket.accept()
        self.active_connections.add(websocket)
        logger.info(f"WebSocket client connected. Total connections: {len(self.active_connections)}")
    
    def disconnect(self, websocket: WebSocket) -> None:
        """
        移除 WebSocket 连接.
        
        Args:
            websocket: WebSocket 连接对象
        """
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)
            logger.info(f"WebSocket client disconnected. Total connections: {len(self.active_connections)}")
    
    async def send_personal_message(self, message: dict, websocket: WebSocket) -> None:
        """
        向单个客户端发送消息.
        
        Args:
            message: 消息字典
            websocket: WebSocket 连接对象
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
        向所有连接的客户端广播消息.
        
        Args:
            message: 消息字典
        """
        if not self.active_connections:
            return
        
        # 构建消息 JSON
        try:
            message_json = json.dumps(message, ensure_ascii=False)
        except Exception as e:
            logger.error(f"Failed to serialize message: {e}")
            return
        
        # 广播到所有连接
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
        
        # 移除断开的连接
        for connection in disconnected:
            self.disconnect(connection)
        
        if len(self.active_connections) > 0:
            logger.debug(f"Broadcasted message to {len(self.active_connections)} clients")


# ========== 全局对象 ==========
state_manager = StateManager()
market_state_manager = MarketStateManager()
metadata_loader: MetadataLoader | None = None
data_processor: DataProcessor | None = None
market_data_processor: MarketDataProcessor | None = None
websocket_manager = WebSocketManager()
mqtt_subscriber: MQTTSubscriber | None = None
market_mqtt_subscriber: MQTTSubscriber | None = None
_global_event_loop: asyncio.AbstractEventLoop | None = None


# ========== 初始化函数 ==========
def setup_metadata_loader() -> MetadataLoader:
    """设置元数据加载器."""
    # CSV 文件路径：data 和 dashboard 是同级目录
    # 从 dashboard/backend.py -> dashboard -> 父目录 -> data/csv_output/facilities.csv
    project_root = Path(__file__).parent.parent
    default_csv_path = project_root / "data" / "csv_output" / "facilities.csv"
    
    if not default_csv_path.exists():
        logger.warning(f"Facilities CSV not found at {default_csv_path}")
        # 尝试相对路径（从当前工作目录）
        alt_path = Path("data/csv_output/facilities.csv")
        if alt_path.exists():
            default_csv_path = alt_path
            logger.info(f"Using facilities CSV from relative path: {default_csv_path}")
        else:
            logger.error("Cannot find facilities.csv, metadata loading may fail")
    
    return MetadataLoader(default_csv_path)


def setup_mqtt_subscriber() -> MQTTSubscriber:
    """设置 MQTT 订阅服务（设施数据）."""
    
    def on_message_callback(mqtt_message: MQTTMessage) -> None:
        """MQTT 消息回调."""
        global _global_event_loop
        
        if data_processor is None:
            logger.error("Data processor not initialized")
            return
        
        # 处理消息
        facility_data = data_processor.process_message(mqtt_message)
        
        if facility_data:
            # 构建 WebSocket 消息
            ws_message: FacilityUpdate = FacilityUpdate(
                type="facility_update",
                data=facility_data,
            )
            
            # 从同步上下文调用异步函数
            # MQTT 回调在 paho-mqtt 的线程中执行，需要使用 run_coroutine_threadsafe
            # 将协程调度到主事件循环中执行
            if _global_event_loop is not None and _global_event_loop.is_running():
                try:
                    asyncio.run_coroutine_threadsafe(
                        websocket_manager.broadcast(ws_message.model_dump()),
                        _global_event_loop
                    )
                except Exception as e:
                    logger.error(f"Failed to schedule broadcast task: {e}")
            else:
                logger.warning("Event loop not available for broadcasting message")
    
    # 创建 MQTT 订阅器
    subscriber = MQTTSubscriber(
        broker_host="broker.hivemq.com",
        broker_port=1883,
        topic="a02/facility_metrics/v1/stream",
        qos=0,
        message_callback=on_message_callback,
    )
    
    return subscriber


def setup_market_mqtt_subscriber() -> MQTTSubscriber:
    """设置市场数据MQTT订阅服务."""
    
    def on_market_message_callback(message_dict: dict) -> None:
        """市场MQTT消息回调."""
        global _global_event_loop
        
        if market_data_processor is None:
            logger.error("Market data processor not initialized")
            return
        
        try:
            # 解析为MarketMetricsMessage
            mqtt_message = MarketMetricsMessage(**message_dict)
            
            # 处理消息
            metric_data = market_data_processor.process_message(mqtt_message)
            
            if metric_data:
                # 构建 WebSocket 消息
                ws_message: MarketUpdate = MarketUpdate(
                    type="market_update",
                    data=metric_data,
                )
                
                # 从同步上下文调用异步函数
                if _global_event_loop is not None and _global_event_loop.is_running():
                    try:
                        asyncio.run_coroutine_threadsafe(
                            websocket_manager.broadcast(ws_message.model_dump()),
                            _global_event_loop
                        )
                    except Exception as e:
                        logger.error(f"Failed to schedule market broadcast task: {e}")
                else:
                    logger.warning("Event loop not available for broadcasting market message")
        except Exception as e:
            logger.error(f"Failed to process market message: {e}", exc_info=True)
    
    # 创建市场MQTT订阅器（复用MQTTSubscriber，但使用不同的消息格式处理）
    # 需要自定义_on_message来处理市场消息格式
    class MarketMQTTSubscriber(MQTTSubscriber):
        """市场MQTT订阅器（继承并重写消息处理）."""
        
        def _on_message(self, client: mqtt.Client, userdata: dict, msg: mqtt.MQTTMessage) -> None:
            """MQTT消息接收回调（市场数据格式）."""
            try:
                # 解析JSON消息
                payload_str = msg.payload.decode("utf-8")
                payload_dict = json.loads(payload_str)
                
                # 调用回调函数（传递dict而非Pydantic模型）
                if self.message_callback:
                    self.message_callback(payload_dict)
                
                logger.debug(
                    f"Received market MQTT message: network_code={payload_dict.get('network_code')}, "
                    f"metric={payload_dict.get('metric')}, value={payload_dict.get('value')}"
                )
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


# ========== FastAPI 应用生命周期 ==========
@asynccontextmanager
async def lifespan(app: FastAPI):
    """应用生命周期管理."""
    global metadata_loader, data_processor, market_data_processor, mqtt_subscriber, market_mqtt_subscriber, _global_event_loop
    
    # 启动时初始化
    logger.info("Starting FastAPI application...")
    
    # 保存当前事件循环的引用（用于 MQTT 回调中的异步调用）
    _global_event_loop = asyncio.get_running_loop()
    
    # 初始化元数据加载器
    metadata_loader = setup_metadata_loader()
    logger.info("Metadata loader initialized")
    
    # 初始化状态管理器和数据处理器（设施数据）
    data_processor = DataProcessor(state_manager, metadata_loader)
    logger.info("Data processor initialized")
    
    # 初始化市场数据处理器
    market_data_processor = MarketDataProcessor(market_state_manager)
    logger.info("Market data processor initialized")
    
    # 启动设施数据MQTT订阅服务
    mqtt_subscriber = setup_mqtt_subscriber()
    mqtt_subscriber.start()
    logger.info("Facility MQTT subscriber started")
    
    # 启动市场数据MQTT订阅服务
    market_mqtt_subscriber = setup_market_mqtt_subscriber()
    market_mqtt_subscriber.start()
    logger.info("Market MQTT subscriber started")
    
    yield
    
    # 关闭时清理
    logger.info("Shutting down FastAPI application...")
    
    if market_mqtt_subscriber:
        market_mqtt_subscriber.stop()
        logger.info("Market MQTT subscriber stopped")
    
    if mqtt_subscriber:
        mqtt_subscriber.stop()
        logger.info("Facility MQTT subscriber stopped")
    
    # 清理事件循环引用
    _global_event_loop = None


# ========== FastAPI 应用初始化 ==========
app = FastAPI(
    title="Electricity Dashboard Backend",
    description="FastAPI backend for real-time electricity facility monitoring",
    version="1.0.0",
    lifespan=lifespan,
)


# ========== 路由定义 ==========
@app.get("/")
async def root() -> dict:
    """根端点."""
    return {
        "message": "Electricity Dashboard Backend API",
        "status": "running",
        "websocket_endpoint": "/ws/realtime",
    }


@app.get("/api/facilities")
async def get_all_facilities() -> dict:
    """
    获取所有设施的当前状态.
    
    Returns:
        所有设施的状态字典
    """
    facilities = state_manager.get_all_facilities()
    return {
        "facilities": facilities,
        "count": len(facilities),
    }


@app.get("/api/facilities/{facility_id}")
async def get_facility(facility_id: str) -> dict:
    """
    获取指定设施的状态.
    
    Args:
        facility_id: 设施 ID
    
    Returns:
        设施状态字典
    """
    facility = state_manager.get_facility(facility_id)
    if not facility:
        return {"error": "Facility not found"}
    return facility


@app.get("/api/metadata")
async def get_metadata() -> dict:
    """
    获取所有设施的元数据.
    
    Returns:
        所有设施的元数据字典
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
    获取最新的市场指标.
    
    Returns:
        所有市场指标的最新值
    """
    metrics = market_state_manager.get_all_metrics()
    return {
        "metrics": metrics,
        "count": sum(len(network_metrics) for network_metrics in metrics.values()),
    }


@app.get("/api/market/{network_code}/{metric}")
async def get_market_metric(network_code: str, metric: str) -> dict:
    """
    获取指定网络和指标的最新值.
    
    Args:
        network_code: 网络代码（如 "NEM"）
        metric: 指标类型（如 "price", "demand"）
    
    Returns:
        指标数据字典
    """
    metric_data = market_state_manager.get_metric(network_code, metric)
    if not metric_data:
        return {"error": f"Metric not found: {network_code}/{metric}"}
    return metric_data


@app.websocket("/ws/realtime")
async def websocket_endpoint(websocket: WebSocket) -> None:
    """
    WebSocket 端点，用于实时数据推送.
    
    Args:
        websocket: WebSocket 连接对象
    """
    await websocket_manager.connect(websocket)
    
    try:
        # 发送初始数据（所有设施的当前状态）
        facilities = state_manager.get_all_facilities()
        initial_message: FacilityUpdate = FacilityUpdate(
            type="initial_data",
            data={"facilities": facilities},
        )
        await websocket_manager.send_personal_message(initial_message.model_dump(), websocket)
        
        # 发送初始市场数据
        market_metrics = market_state_manager.get_all_metrics()
        market_initial_message: MarketUpdate = MarketUpdate(
            type="initial_market_data",
            data={"metrics": market_metrics},
        )
        await websocket_manager.send_personal_message(market_initial_message.model_dump(), websocket)
        
        # 保持连接并处理客户端消息
        while True:
            try:
                # 接收客户端消息（可选）
                data = await websocket.receive_json()
                logger.debug(f"Received message from client: {data}")
                
                # 处理订阅/过滤请求（如果需要）
                # 这里可以扩展实现过滤功能
                
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


# ========== 主程序入口 ==========
if __name__ == "__main__":
    import uvicorn
    
    # 运行 FastAPI 服务器
    uvicorn.run(
        "backend:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info",
    )

