#!/usr/bin/env python3
"""
OpenElectricity 统一 REST API 客户端
整合 Network、Metrics、Market 三个API的功能
使用统一的架构和错误处理机制
"""

import os
import json
import asyncio
import logging
import time
import math
from datetime import datetime
from typing import Dict, Any, Optional, List, Union
from aiohttp import ClientSession, ClientResponse
from pathlib import Path

# 设置环境变量
os.environ["OPENELECTRICITY_API_KEY"] = "oe_3ZVGZZG6UcWimHS6rF7BPK6e"

# 确保日志目录存在
os.makedirs('logs', exist_ok=True)

# 日志配置
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('logs/api_client.log', encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)


def load_facilities_data(file_path: str = "data/facilities_data.json") -> Optional[Dict[str, Any]]:
    """
    加载设施数据JSON文件
    
    Args:
        file_path: JSON文件路径
        
    Returns:
        解析后的JSON数据，失败时返回None
    """
    try:
        facilities_file = Path(file_path)
        if not facilities_file.exists():
            logger.error(f"设施数据文件不存在: {file_path}")
            return None
            
        with open(facilities_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
            
        logger.info(f"成功加载设施数据文件: {file_path}")
        return data
        
    except json.JSONDecodeError as e:
        logger.error(f"JSON解析错误: {e}")
        return None
    except Exception as e:
        logger.error(f"加载设施数据失败: {e}")
        return None


def extract_facility_codes(data: Dict[str, Any], 
                         network_filter: Optional[str] = None,
                         status_filter: Optional[str] = None,
                         limit: Optional[int] = None) -> List[str]:
    """
    从设施数据中提取设施代码
    
    Args:
        data: 设施数据JSON
        network_filter: 网络过滤条件 (如 "NEM")
        status_filter: 状态过滤条件 (如 "operating")
        limit: 限制返回数量
        
    Returns:
        设施代码列表
    """
    try:
        if not data or 'data' not in data:
            logger.error("设施数据格式错误或为空")
            return []
            
        facilities = data['data']
        facility_codes = []
        
        for facility in facilities:
            # 应用网络过滤
            if network_filter and facility.get('network_id') != network_filter:
                continue
                
            # 应用状态过滤 (检查units中的status)
            if status_filter:
                units = facility.get('units', [])
                has_matching_status = any(
                    unit.get('status_id') == status_filter 
                    for unit in units
                )
                if not has_matching_status:
                    continue
            
            # 提取设施代码
            facility_code = facility.get('code')
            if facility_code:
                facility_codes.append(facility_code)
                
            # 应用数量限制
            if limit and len(facility_codes) >= limit:
                break
        
        logger.info(f"成功提取 {len(facility_codes)} 个设施代码")
        if limit:
            logger.info(f"应用数量限制: {limit}")
        if network_filter:
            logger.info(f"应用网络过滤: {network_filter}")
        if status_filter:
            logger.info(f"应用状态过滤: {status_filter}")
            
        return facility_codes
        
    except Exception as e:
        logger.error(f"提取设施代码失败: {e}")
        return []


def update_metrics_config(facility_codes: List[str], 
                         config: Dict[str, Any]) -> Dict[str, Any]:
    """
    更新METRICS_CONFIG中的facility_codes
    
    Args:
        facility_codes: 设施代码列表
        config: 原始配置字典
        
    Returns:
        更新后的配置字典
    """
    try:
        # 创建配置副本
        updated_config = config.copy()
        updated_config['facility_codes'] = facility_codes
        
        logger.info(f"更新METRICS_CONFIG: {len(facility_codes)} 个设施代码")
        logger.debug(f"设施代码列表: {facility_codes[:10]}{'...' if len(facility_codes) > 10 else ''}")
        
        return updated_config
        
    except Exception as e:
        logger.error(f"更新METRICS_CONFIG失败: {e}")
        return config


def create_batches(facility_codes: List[str], batch_size: int = 30) -> List[List[str]]:
    """
    将设施代码列表分片
    
    Args:
        facility_codes: 设施代码列表
        batch_size: 每片大小
        
    Returns:
        分片后的设施代码列表
    """
    try:
        batches = []
        total_facilities = len(facility_codes)
        total_batches = math.ceil(total_facilities / batch_size)
        
        for i in range(0, total_facilities, batch_size):
            batch = facility_codes[i:i + batch_size]
            batches.append(batch)
        
        logger.info(f"创建分片: 总设施数 {total_facilities}, 分片大小 {batch_size}, 总分片数 {total_batches}")
        return batches
        
    except Exception as e:
        logger.error(f"创建分片失败: {e}")
        return []


def process_batch_metrics(batch_index: int, batch_codes: List[str], 
                         base_config: Dict[str, Any], 
                         output_dir: str = "data/batch_data") -> bool:
    """
    处理单个分片的metrics数据
    
    Args:
        batch_index: 分片索引
        batch_codes: 分片设施代码
        base_config: 基础配置
        output_dir: 输出目录
        
    Returns:
        处理是否成功
    """
    try:
        logger.info(f"处理分片 {batch_index + 1}: {len(batch_codes)} 个设施")
        
        # 更新配置
        metrics_config = update_metrics_config(batch_codes, base_config)
        
        # 调用API获取数据
        metrics_data = fetch_metrics_data(**metrics_config)
        if metrics_data is None:
            logger.error(f"分片 {batch_index + 1} API调用失败")
            return False
        
        # 处理响应数据
        processed_metrics = process_response(metrics_data, "metrics")
        if processed_metrics is None:
            logger.error(f"分片 {batch_index + 1} 数据处理失败")
            return False
        
        # 保存分片数据
        output_path = Path(output_dir)
        output_path.mkdir(exist_ok=True)
        
        batch_filename = f"batch_{batch_index + 1:03d}_metrics.json"
        batch_file_path = output_path / batch_filename
        
        batch_data = {
            "batch_index": batch_index + 1,
            "facility_codes": batch_codes,
            "timestamp": datetime.now().isoformat(),
            "data": processed_metrics
        }
        
        with open(batch_file_path, 'w', encoding='utf-8') as f:
            json.dump(batch_data, f, indent=2, ensure_ascii=False)
        
        logger.info(f"分片 {batch_index + 1} 数据已保存: {batch_file_path}")
        return True
        
    except Exception as e:
        logger.error(f"处理分片 {batch_index + 1} 失败: {e}")
        return False


def aggregate_batch_data(output_dir: str = "data/batch_data") -> Optional[Dict[str, Any]]:
    """
    聚合所有分片数据
    
    Args:
        output_dir: 分片数据目录
        
    Returns:
        聚合后的数据，失败时返回None
    """
    try:
        output_path = Path(output_dir)
        if not output_path.exists():
            logger.error(f"分片数据目录不存在: {output_dir}")
            return None
        
        # 查找所有分片文件
        batch_files = sorted(output_path.glob("batch_*_metrics.json"))
        if not batch_files:
            logger.error("未找到分片数据文件")
            return None
        
        logger.info(f"找到 {len(batch_files)} 个分片文件")
        
        # 聚合数据
        aggregated_data = {
            "version": "4.3.0",
            "created_at": datetime.now().isoformat(),
            "success": True,
            "total_batches": len(batch_files),
            "data": []
        }
        
        for batch_file in batch_files:
            try:
                with open(batch_file, 'r', encoding='utf-8') as f:
                    batch_data = json.load(f)
                
                # 提取实际数据
                if 'data' in batch_data and 'data' in batch_data['data']:
                    aggregated_data['data'].extend(batch_data['data']['data'])
                    
            except Exception as e:
                logger.warning(f"读取分片文件失败 {batch_file}: {e}")
                continue
        
        logger.info(f"数据聚合完成: 总记录数 {len(aggregated_data['data'])}")
        return aggregated_data
        
    except Exception as e:
        logger.error(f"聚合分片数据失败: {e}")
        return None


def save_aggregated_data(data: Dict[str, Any], filename: str = "aggregated_metrics.json") -> bool:
    """
    保存聚合后的数据
    
    Args:
        data: 聚合数据
        filename: 输出文件名
        
    Returns:
        保存是否成功
    """
    try:
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        
        file_size = os.path.getsize(filename)
        logger.info(f"聚合数据已保存: {filename}, 文件大小: {file_size} 字节")
        return True
        
    except Exception as e:
        logger.error(f"保存聚合数据失败: {e}")
        return False


class UnifiedAPIClient:
    """统一 OpenElectricity API 客户端"""

    def __init__(self, api_key: str, base_url: str = "https://api.openelectricity.org.au/v4"):
        self.api_key = api_key
        self.base_url = base_url.rstrip("/") + "/"
        self.headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

    async def _handle_response(self, response: ClientResponse) -> Dict[str, Any]:
        """处理 API 响应"""
        if not response.ok:
            try:
                error_data = await response.json()
                detail = error_data.get("detail", response.reason)
            except Exception:
                detail = response.reason
            logger.error(f"API 错误: {response.status} - {detail}")
            raise Exception(f"API Error {response.status}: {detail or ''}")
        return await response.json()

    async def _async_get_network_data(self, network_id: List[str] = None) -> Dict[str, Any]:
        """异步获取网络设施数据"""
        logger.debug("Getting network facilities data for %s", network_id)
        params = {"network_id": network_id} if network_id else {}
        
        async with ClientSession(base_url=self.base_url, headers=self.headers) as session:
            async with session.get("/facilities/", params=params) as response:
                return await self._handle_response(response)

    async def _async_get_metrics_data(
        self,
        network_code: str,
        facility_code: Union[str, List[str]],
        metrics: List[str],
        interval: str = None,
        date_start: datetime = None,
        date_end: datetime = None,
    ) -> Dict[str, Any]:
        """异步获取设施指标数据"""
        logger.debug(
            "Getting facility metrics for %s/%s (metrics: %s, interval: %s)",
            network_code,
            facility_code,
            metrics,
            interval,
        )
        
        params = {
            "facility_code": facility_code,
            "metrics": metrics,
            "interval": interval,
            "date_start": date_start.isoformat() if date_start else None,
            "date_end": date_end.isoformat() if date_end else None,
        }
        params = {k: v for k, v in params.items() if v is not None}
        logger.debug("Request parameters: %s", params)

        async with ClientSession(base_url=self.base_url, headers=self.headers) as session:
            async with session.get(f"/data/facilities/{network_code}", params=params) as response:
                return await self._handle_response(response)

    async def _async_get_market_data(
        self,
        network_code: str,
        metrics: List[str],
        interval: str = None,
        date_start: datetime = None,
        date_end: datetime = None,
        primary_grouping: str = None,
        network_region: str = None,
    ) -> Dict[str, Any]:
        """异步获取市场数据"""
        logger.debug(
            "Getting market data for %s (metrics: %s, interval: %s, region: %s)",
            network_code,
            metrics,
            interval,
            network_region,
        )
        
        params = {
            "metrics": metrics,
            "interval": interval,
            "date_start": date_start.isoformat() if date_start else None,
            "date_end": date_end.isoformat() if date_end else None,
            "primary_grouping": primary_grouping,
            "network_region": network_region,
        }
        params = {k: v for k, v in params.items() if v is not None}
        logger.debug("Request parameters: %s", params)

        async with ClientSession(base_url=self.base_url, headers=self.headers) as session:
            async with session.get(f"/market/network/{network_code}", params=params) as response:
                return await self._handle_response(response)

    def get_network_data(self, network_id: List[str] = None) -> Dict[str, Any]:
        """同步获取网络设施数据接口"""
        async def _run():
            return await self._async_get_network_data(network_id=network_id)
        return asyncio.run(_run())

    def get_metrics_data(
        self,
        network_code: str,
        facility_code: Union[str, List[str]],
        metrics: List[str],
        interval: str = None,
        date_start: datetime = None,
        date_end: datetime = None,
    ) -> Dict[str, Any]:
        """同步获取设施指标数据接口"""
        async def _run():
            return await self._async_get_metrics_data(
                network_code=network_code,
                facility_code=facility_code,
                metrics=metrics,
                interval=interval,
                date_start=date_start,
                date_end=date_end
            )
        return asyncio.run(_run())

    def get_market_data(
        self,
        network_code: str,
        metrics: List[str],
        interval: str = None,
        date_start: datetime = None,
        date_end: datetime = None,
        primary_grouping: str = None,
        network_region: str = None,
    ) -> Dict[str, Any]:
        """同步获取市场数据接口"""
        async def _run():
            return await self._async_get_market_data(
                network_code=network_code,
                metrics=metrics,
                interval=interval,
                date_start=date_start,
                date_end=date_end,
                primary_grouping=primary_grouping,
                network_region=network_region
            )
        return asyncio.run(_run())


class UnifiedDataProcessor:
    """统一数据处理和验证类"""

    @staticmethod
    def validate_network_response(data: Dict[str, Any]) -> bool:
        """验证网络API响应数据格式"""
        try:
            required_fields = ['version', 'created_at', 'success', 'data']
            for field in required_fields:
                if field not in data:
                    logger.error(f"网络响应数据缺少必需字段: {field}")
                    return False
            if not data.get('success', False):
                error_msg = data.get('error', '未知错误')
                logger.error(f"网络API 返回错误: {error_msg}")
                return False
            data_list = data.get('data', [])
            if not isinstance(data_list, list):
                logger.error("网络data字段不是列表格式")
                return False
            logger.info(f"网络数据验证成功，包含 {len(data_list)} 条设施记录")
            return True
        except Exception as e:
            logger.error(f"网络数据验证失败: {e}")
            return False

    @staticmethod
    def validate_metrics_response(data: Dict[str, Any]) -> bool:
        """验证指标API响应数据格式"""
        try:
            if not isinstance(data, dict):
                logger.error("指标响应数据不是字典格式")
                return False
            if 'data' not in data:
                logger.error("指标响应数据缺少data字段")
                return False
            data_list = data.get('data', [])
            if not isinstance(data_list, list):
                logger.error("指标data字段不是列表格式")
                return False
            logger.info(f"指标数据验证成功，包含 {len(data_list)} 条记录")
            return True
        except Exception as e:
            logger.error(f"指标数据验证失败: {e}")
            return False

    @staticmethod
    def validate_market_response(data: Dict[str, Any]) -> bool:
        """验证市场API响应数据格式"""
        try:
            if not isinstance(data, dict):
                logger.error("市场响应数据不是字典格式")
                return False
            if 'data' not in data:
                logger.error("市场响应数据缺少data字段")
                return False
            data_list = data.get('data', [])
            if not isinstance(data_list, list):
                logger.error("市场data字段不是列表格式")
                return False
            logger.info(f"市场数据验证成功，包含 {len(data_list)} 条记录")
            return True
        except Exception as e:
            logger.error(f"市场数据验证失败: {e}")
            return False

    @staticmethod
    def transform_data(raw_data: Dict[str, Any], data_type: str) -> Dict[str, Any]:
        """转换和标准化数据格式"""
        try:
            transformed_data = raw_data.copy()
            transformed_data['processed_at'] = datetime.now().isoformat()
            transformed_data['data_type'] = data_type
            if 'total_records' not in transformed_data:
                transformed_data['total_records'] = len(transformed_data.get('data', []))
            logger.info(f"{data_type}数据转换完成")
            return transformed_data
        except Exception as e:
            logger.error(f"{data_type}数据转换失败: {e}")
            raise Exception(f"{data_type}数据转换失败: {e}")


class UnifiedFileManager:
    """统一文件操作管理类"""

    @staticmethod
    def save_to_file(data: Dict[str, Any], filename: str) -> bool:
        """将数据保存为 JSON 文件"""
        try:
            os.makedirs('data', exist_ok=True)
            filepath = f'data/{filename}'
            temp_filepath = f'{filepath}.tmp'
            
            # 自定义JSON序列化器，处理datetime对象
            def json_serializer(obj):
                if isinstance(obj, datetime):
                    return obj.isoformat()
                elif hasattr(obj, '__dict__'):
                    return obj.__dict__
                else:
                    return str(obj)
            
            with open(temp_filepath, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2, default=json_serializer)
            os.rename(temp_filepath, filepath)
            logger.info(f"数据已成功保存到文件: {filepath}")
            file_size = os.path.getsize(filepath)
            logger.info(f"文件大小: {file_size} 字节")
            return True
        except Exception as e:
            logger.error(f"文件保存失败: {e}")
            temp_filepath = f'data/{filename}.tmp'
            if os.path.exists(temp_filepath):
                os.remove(temp_filepath)
            raise Exception(f"文件保存失败: {e}")


def fetch_network_data(network_id: List[str] = None) -> Optional[Dict[str, Any]]:
    """调用 OpenElectricity API 获取网络设施数据"""
    try:
        logger.info("开始获取网络设施数据...")
        if network_id:
            logger.info(f"网络ID: {network_id}")
        
        api_key = os.getenv("OPENELECTRICITY_API_KEY")
        if not api_key:
            logger.error("未找到 API 密钥，请设置 OPENELECTRICITY_API_KEY 环境变量")
            return None
        
        client = UnifiedAPIClient(api_key=api_key)
        logger.info("正在连接OpenElectricity API...")
        
        response = client.get_network_data(network_id=network_id)
        logger.info("网络API调用成功")
        logger.info(f"成功获取数据，设施总数: {len(response.get('data', []))}")
        return response
        
    except Exception as e:
        logger.error(f"网络数据获取失败: {e}")
        return None


def fetch_metrics_data(
    facility_codes: List[str],
    network_code: str = "NEM",
    metrics: List[str] = ["power", "emissions"],
    interval: str = "5m",
    date_start: Optional[datetime] = None,
    date_end: Optional[datetime] = None
) -> Optional[Dict[str, Any]]:
    """调用 OpenElectricity API 获取设施指标数据"""
    try:
        logger.info(f"开始获取设施 {facility_codes} 的指标数据...")
        logger.info(f"网络代码: {network_code}, 指标: {metrics}")
        logger.info(f"数据间隔: {interval}")
        if date_start:
            logger.info(f"开始日期: {date_start.strftime('%Y-%m-%d %H:%M:%S')}")
        if date_end:
            logger.info(f"结束日期: {date_end.strftime('%Y-%m-%d %H:%M:%S')}")
        
        api_key = os.getenv("OPENELECTRICITY_API_KEY")
        if not api_key:
            logger.error("未找到 API 密钥，请设置 OPENELECTRICITY_API_KEY 环境变量")
            return None
        
        client = UnifiedAPIClient(api_key=api_key)
        logger.info("正在连接OpenElectricity API...")
        
        response = client.get_metrics_data(
            network_code=network_code,
            facility_code=facility_codes,
            metrics=metrics,
            interval=interval,
            date_start=date_start,
            date_end=date_end
        )
        
        logger.info("指标API调用成功")
        logger.info(f"成功获取数据，记录总数: {len(response.get('data', []))}")
        return response
        
    except Exception as e:
        logger.error(f"指标数据获取失败: {e}")
        return None


def fetch_market_data(
    network_code: str = "NEM",
    metrics: List[str] = ["price", "demand"],
    interval: str = "1h",
    date_start: Optional[datetime] = None,
    date_end: Optional[datetime] = None,
    primary_grouping: Optional[str] = None,
    network_region: Optional[str] = None
) -> Optional[Dict[str, Any]]:
    """调用 OpenElectricity API 获取市场数据"""
    try:
        logger.info(f"开始获取 {network_code} 网络的市场数据...")
        logger.info(f"网络代码: {network_code}, 指标: {metrics}")
        logger.info(f"数据间隔: {interval}")
        if date_start:
            logger.info(f"开始日期: {date_start.strftime('%Y-%m-%d %H:%M:%S')}")
        if date_end:
            logger.info(f"结束日期: {date_end.strftime('%Y-%m-%d %H:%M:%S')}")
        if primary_grouping:
            logger.info(f"主要分组: {primary_grouping}")
        if network_region:
            logger.info(f"网络区域: {network_region}")
        
        api_key = os.getenv("OPENELECTRICITY_API_KEY")
        if not api_key:
            logger.error("未找到 API 密钥，请设置 OPENELECTRICITY_API_KEY 环境变量")
            return None
        
        client = UnifiedAPIClient(api_key=api_key)
        logger.info("正在连接OpenElectricity API...")
        
        response = client.get_market_data(
            network_code=network_code,
            metrics=metrics,
            interval=interval,
            date_start=date_start,
            date_end=date_end,
            primary_grouping=primary_grouping,
            network_region=network_region
        )
        
        logger.info("市场API调用成功")
        logger.info(f"成功获取数据，记录总数: {len(response.get('data', []))}")
        return response
        
    except Exception as e:
        logger.error(f"市场数据获取失败: {e}")
        return None


def process_response(response_data: Dict[str, Any], data_type: str) -> Optional[Dict[str, Any]]:
    """处理和验证API响应数据"""
    try:
        # 根据数据类型选择验证方法
        if data_type == "network":
            if not UnifiedDataProcessor.validate_network_response(response_data):
                return None
        elif data_type == "metrics":
            if not UnifiedDataProcessor.validate_metrics_response(response_data):
                return None
        elif data_type == "market":
            if not UnifiedDataProcessor.validate_market_response(response_data):
                return None
        else:
            logger.error(f"未知的数据类型: {data_type}")
            return None
            
        processed_data = UnifiedDataProcessor.transform_data(response_data, data_type)
        return processed_data
    except Exception as e:
        logger.error(f"{data_type}数据处理失败: {e}")
        return None


def process_and_save_data(data: Optional[Dict[str, Any]], data_type: str, filename: str) -> bool:
    """通用数据处理和保存函数"""
    if data is None:
        logger.error(f"{data_type}数据获取失败")
        return False
    
    processed_data = process_response(data, data_type)
    if processed_data is None:
        logger.error(f"{data_type}数据处理失败")
        return False
    
    save_success = UnifiedFileManager.save_to_file(processed_data, filename)
    if save_success:
        logger.info(f"{data_type}数据保存成功")
        return True
    else:
        logger.error(f"{data_type}数据保存失败")
        return False




def main():
    """主控制函数，协调整个数据获取流程"""
    logger.info("=" * 60)
    logger.info("OpenElectricity 统一 REST API 数据获取工具启动")
    logger.info("=" * 60)

    # 配置参数
    NETWORK_CONFIG = {
        "network_id": ["NEM"]
    }
    
    # 通用配置
    COMMON_DATE_START = datetime(2025, 10, 1)
    COMMON_DATE_END = datetime(2025, 10, 2)
    COMMON_INTERVAL = "5m"

    BASE_METRICS_CONFIG = {
        "facility_codes": [],  # 将从facilities_data.json动态获取
        "network_code": "NEM",
        "metrics": ["power", "emissions"],
        "interval": COMMON_INTERVAL,
        "date_start": COMMON_DATE_START,
        "date_end": COMMON_DATE_END
    }

    MARKET_CONFIG = {
        "network_code": "NEM",
        "metrics": ["price", "demand"],
        "interval": COMMON_INTERVAL,
        "date_start": COMMON_DATE_START,
        "date_end": COMMON_DATE_END,
        "primary_grouping": None,
        "network_region": None
    }

    success_count = 0
    total_tasks = 3

    try:
        # 步骤1: 获取网络设施数据
        logger.info("步骤1: 获取网络设施数据")
        network_data = fetch_network_data(**NETWORK_CONFIG)
        if process_and_save_data(network_data, "network", 'facilities_data.json'):
            success_count += 1

        # 步骤2: 处理设施指标数据 (基于步骤1的网络数据)
        logger.info("步骤2: 处理设施指标数据")
        
        # 从步骤1保存的facilities_data.json加载设施数据
        facilities_data = load_facilities_data("data/facilities_data.json")
        if facilities_data is None:
            logger.error("无法加载设施数据，使用默认配置")
            all_facility_codes = ["BAYSW1", "ERARING"]  # 默认值
        else:
            # 提取全部设施代码
            all_facility_codes = extract_facility_codes(
                data=facilities_data,
                network_filter="NEM",  # 只获取NEM网络的设施
                status_filter="operating",  # 只获取运行中的设施
                limit=None  # 获取全部设施代码
            )
            
            # 如果没有提取到设施代码，使用默认值
            if not all_facility_codes:
                logger.warning("未提取到设施代码，使用默认配置")
                all_facility_codes = ["BAYSW1", "ERARING"]
        
        logger.info(f"成功提取 {len(all_facility_codes)} 个设施代码")
        
        # 创建分片处理
        BATCH_SIZE = 30  # 每片30个设施
        
        # 生产模式：处理全部数据
        TEST_MODE = False  # 设置为True进行测试，False处理全部数据
        if TEST_MODE:
            # 只处理前60个设施用于测试
            test_facility_codes = all_facility_codes[:60]
            logger.info(f"测试模式：只处理前 {len(test_facility_codes)} 个设施")
            batches = create_batches(test_facility_codes, BATCH_SIZE)
        else:
            # 处理全部设施
            logger.info(f"生产模式：处理全部 {len(all_facility_codes)} 个设施")
            batches = create_batches(all_facility_codes, BATCH_SIZE)
        
        total_batches = len(batches)
        
        logger.info(f"分片创建完成: 总分片数 {total_batches}, 每片大小 {BATCH_SIZE}")
        
        # 串行处理每个分片
        logger.info("开始串行处理分片...")
        successful_batches = 0
        failed_batches = 0
        
        for batch_index, batch_codes in enumerate(batches):
            logger.info(f"处理分片 {batch_index + 1}/{total_batches}")
            
            # 处理单个分片
            success = process_batch_metrics(
                batch_index=batch_index,
                batch_codes=batch_codes,
                base_config=BASE_METRICS_CONFIG,
                output_dir="data/batch_data"
            )
            
            if success:
                successful_batches += 1
                logger.info(f"分片 {batch_index + 1} 处理成功")
            else:
                failed_batches += 1
                logger.error(f"分片 {batch_index + 1} 处理失败")
            
            # 批次间延迟 (避免API限制)
            if batch_index < total_batches - 1:  # 不是最后一个分片
                logger.info("等待2秒后处理下一个分片...")
                time.sleep(2)
        
        logger.info(f"分片处理完成: 成功 {successful_batches}, 失败 {failed_batches}")
        
        # 聚合所有分片数据
        if successful_batches > 0:
            logger.info("开始聚合分片数据...")
            aggregated_data = aggregate_batch_data("data/batch_data")
            
            if aggregated_data is not None:
                # 保存聚合数据
                save_success = save_aggregated_data(aggregated_data, "data/facility_metrics.json")
                if save_success:
                    logger.info("聚合数据保存成功")
                    success_count += 1
                else:
                    logger.error("聚合数据保存失败")
            else:
                logger.error("数据聚合失败")
        else:
            logger.error("没有成功处理的分片，跳过数据聚合")

        # 步骤3: 获取市场数据
        logger.info("步骤3: 获取市场数据")
        market_data = fetch_market_data(**MARKET_CONFIG)
        if process_and_save_data(market_data, "market", 'market_data.json'):
            success_count += 1

        # 结果汇总
        logger.info("=" * 60)
        logger.info("数据获取流程完成！")
        logger.info(f"成功完成任务: {success_count}/{total_tasks}")
        logger.info(f"处理时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info("=" * 60)
        
        return success_count == total_tasks

    except Exception as e:
        logger.error(f"程序执行失败: {e}")
        return False


if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)
