#!/usr/bin/env python3
"""
基于 OpenElectricity 官方 SDK (OEClient) 的数据采集程序 v2
使用方案A方法2（客户端聚合 + facility-unit 映射）获取 facility 级别数据

功能：
- 获取网络设施数据（Network）
- 批量获取设施指标数据（Metrics，支持分片处理）
- 聚合 unit 级别数据为 facility 级别数据（新增）
- 获取市场数据（Market）
"""

import os
import json
import time
import math
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional, List, Tuple
from collections import defaultdict

# 设置环境变量
os.environ["OPENELECTRICITY_API_KEY"] = "oe_3ZVGZZG6UcWimHS6rF7BPK6e"

# SDK 导入
from openelectricity import OEClient
from openelectricity.models.facilities import FacilityResponse, Facility
from openelectricity.models.timeseries import TimeSeriesResponse, NetworkTimeSeries, TimeSeriesResult
from openelectricity.types import (
    DataMetric,
    MarketMetric,
    NetworkCode,
    DataInterval,
    DataPrimaryGrouping,
    UnitStatusType,
)

# 确保日志目录存在
os.makedirs('logs', exist_ok=True)

# 日志配置
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('logs/sdk_client_v2.log', encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)


class ConfigLoader:
    """配置管理器，从环境变量加载配置"""
    
    @staticmethod
    def load_api_key() -> str:
        """加载 API 密钥（必需）"""
        api_key = os.getenv("OPENELECTRICITY_API_KEY")
        if not api_key:
            raise ValueError("OPENELECTRICITY_API_KEY 环境变量未设置")
        return api_key
    
    @staticmethod
    def load_base_url() -> Optional[str]:
        """加载 API 基础 URL（可选）"""
        return os.getenv("OE_BASE_URL")
    
    @staticmethod
    def load_time_window() -> Tuple[datetime, datetime]:
        """加载时间窗口（可选，默认值）"""
        
        date_start = datetime(2025, 10, 1)
        date_end = datetime(2025, 10, 2)
        
        return date_start, date_end
    
    @staticmethod
    def load_interval() -> str:
        """加载数据间隔（可选，默认 '5m'）"""
        return os.getenv("INTERVAL", "5m")
    
    @staticmethod
    def load_batch_config() -> Dict[str, Any]:
        """加载批量处理配置"""
        batch_size = int(os.getenv("BATCH_SIZE", "30"))
        batch_delay = float(os.getenv("BATCH_DELAY_SECS", "0.5"))
        return {
            "batch_size": batch_size,
            "batch_delay": batch_delay
        }
    
    @staticmethod
    def load_network_filter() -> Optional[str]:
        """加载网络过滤条件（可选，默认 'NEM'）"""
        return os.getenv("NETWORK_FILTER", "NEM")
    
    @staticmethod
    def load_status_filter() -> Optional[str]:
        """加载状态过滤条件（可选，默认 'operating'）"""
        return os.getenv("STATUS_FILTER", "operating")


class DataConverter:
    """数据转换器，将 Pydantic 模型转换为字典格式"""
    
    @staticmethod
    def facility_response_to_dict(response: FacilityResponse) -> Dict[str, Any]:
        """将 FacilityResponse 转换为字典"""
        return response.model_dump()
    
    @staticmethod
    def timeseries_response_to_dict(
        response: TimeSeriesResponse,
        data_type: str = "metrics"
    ) -> Dict[str, Any]:
        """将 TimeSeriesResponse 转换为字典"""
        data = response.model_dump()
        data['data_type'] = data_type
        return data
    
    @staticmethod
    def add_metadata(
        data: Dict[str, Any],
        data_type: str
    ) -> Dict[str, Any]:
        """添加处理元数据"""
        data['processed_at'] = datetime.now().isoformat()
        if 'data_type' not in data:
            data['data_type'] = data_type
        if 'total_records' not in data:
            data_list = data.get('data', [])
            if isinstance(data_list, list):
                data['total_records'] = len(data_list)
        return data
    
    @staticmethod
    def aggregated_facility_data_to_dict(
        aggregated_data: Dict[str, Any],
        metadata: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        将聚合后的 facility 数据转换为字典格式
        
        Args:
            aggregated_data: 聚合后的 facility 级别数据
            metadata: 额外的元数据
            
        Returns:
            格式化的字典数据
        """
        result = {
            "version": "1.0",
            "aggregated_at": datetime.now().isoformat(),
            "aggregation_method": "facility_unit_mapping",
        }
        
        # 添加额外的元数据
        if metadata:
            result.update(metadata)
        
        # 添加聚合数据
        result["data"] = aggregated_data
        
        return result


class FacilityCodeExtractor:
    """设施代码提取器"""
    
    @staticmethod
    def extract_codes(
        facilities_response: FacilityResponse,
        network_filter: Optional[str] = None,
        status_filter: Optional[str] = None
    ) -> List[str]:
        """从设施响应中提取设施代码"""
        if not facilities_response.success:
            raise ValueError(f"设施数据获取失败: {facilities_response.error}")
        
        facilities = facilities_response.data
        if not facilities:
            logger.warning("设施数据为空")
            return []
        
        # 过滤设施
        filtered = FacilityCodeExtractor.filter_facilities(
            facilities,
            network_filter=network_filter,
            status_filter=status_filter
        )
        
        # 提取代码
        codes = []
        for facility in filtered:
            code = facility.code
            if code:
                codes.append(code)
        
        logger.info(f"成功提取 {len(codes)} 个设施代码")
        if network_filter:
            logger.info(f"应用网络过滤: {network_filter}")
        if status_filter:
            logger.info(f"应用状态过滤: {status_filter}")
        
        return codes
    
    @staticmethod
    def filter_facilities(
        facilities: List[Facility],
        network_filter: Optional[str] = None,
        status_filter: Optional[str] = None
    ) -> List[Facility]:
        """根据网络和状态过滤设施"""
        filtered = facilities
        
        # 网络过滤
        if network_filter:
            filtered = [
                f for f in filtered
                if f.network_id == network_filter
            ]
        
        # 状态过滤（检查 units 中的 status_id）
        if status_filter:
            result = []
            for facility in filtered:
                units = facility.units if facility.units else []
                # status_id 是 UnitStatusType 枚举，需要比较 value
                has_matching_status = any(
                    unit.status_id.value == status_filter
                    for unit in units
                )
                if has_matching_status:
                    result.append(facility)
            filtered = result
        
        return filtered


class FacilityUnitMappingManager:
    """
    Facility-Unit 映射关系管理器
    
    负责管理 facility 和 unit 之间的映射关系，用于聚合 unit 级别数据为 facility 级别数据
    """
    
    @staticmethod
    def build_mapping_from_facilities(
        facilities_response: FacilityResponse
    ) -> Dict[str, List[str]]:
        """
        从 FacilityResponse 构建 facility-unit 映射关系
        
        Args:
            facilities_response: 设施响应数据
            
        Returns:
            facility_code -> [unit_codes] 映射字典
        """
        if not facilities_response.success:
            raise ValueError(f"设施数据获取失败: {facilities_response.error}")
        
        mapping: Dict[str, List[str]] = {}
        
        for facility in facilities_response.data:
            facility_code = facility.code
            if not facility_code:
                continue
            
            # 提取 facility 下的所有 units
            unit_codes = []
            if facility.units:
                for unit in facility.units:
                    unit_code = unit.code
                    if unit_code:
                        unit_codes.append(unit_code)
            
            mapping[facility_code] = unit_codes
            logger.debug(f"Facility {facility_code}: {len(unit_codes)} units")
        
        logger.info(f"成功构建映射关系: {len(mapping)} 个 facilities")
        return mapping
    
    @staticmethod
    def get_units_for_facility(
        mapping: Dict[str, List[str]],
        facility_code: str
    ) -> List[str]:
        """
        获取指定 facility 的所有 units
        
        Args:
            mapping: facility-unit 映射关系
            facility_code: facility 代码
            
        Returns:
            unit 代码列表
        """
        return mapping.get(facility_code, [])
    
    @staticmethod
    def save_mapping(
        mapping: Dict[str, List[str]],
        filepath: str
    ) -> None:
        """
        保存映射关系到文件
        
        Args:
            mapping: facility-unit 映射关系
            filepath: 文件路径
        """
        FileManager.save_json(mapping, filepath)
        logger.info(f"映射关系已保存到: {filepath}")
    
    @staticmethod
    def load_mapping(filepath: str) -> Dict[str, List[str]]:
        """
        从文件加载映射关系
        
        Args:
            filepath: 文件路径
            
        Returns:
            facility-unit 映射关系
        """
        mapping = FileManager.load_json(filepath)
        logger.info(f"映射关系已从文件加载: {filepath}")
        return mapping


class FacilityAggregator:
    """
    Facility 数据聚合器
    
    负责将 unit 级别的时序数据聚合为 facility 级别的时序数据
    """
    
    @staticmethod
    def aggregate_to_facility_level(
        timeseries_response: TimeSeriesResponse,
        facility_unit_mapping: Dict[str, List[str]]
    ) -> Dict[str, Any]:
        """
        将 TimeSeriesResponse（unit 级别数据）聚合为 facility 级别数据
        
        Args:
            timeseries_response: TimeSeriesResponse 对象，包含 unit 级别数据
            facility_unit_mapping: facility-unit 映射关系
            
        Returns:
            聚合后的 facility 级别数据
        """
        if not timeseries_response.success:
            raise ValueError(f"时序数据获取失败: {timeseries_response.error}")
        
        # 构建反向映射：unit_code -> facility_code
        unit_to_facility: Dict[str, str] = {}
        for facility_code, unit_codes in facility_unit_mapping.items():
            for unit_code in unit_codes:
                unit_to_facility[unit_code] = facility_code
        
        aggregated = {}
        
        # 遍历每个 metric
        for time_series_item in timeseries_response.data:
            metric = time_series_item.metric
            interval = time_series_item.interval
            unit = time_series_item.unit
            
            # 按 facility_code 组织数据
            facility_data: Dict[str, Dict[str, List[float]]] = defaultdict(lambda: defaultdict(list))
            
            # 遍历每个 unit 的结果
            for result in time_series_item.results:
                unit_code = result.columns.unit_code
                if not unit_code:
                    logger.warning(f"结果 {result.name} 没有 unit_code，跳过")
                    continue
                
                # 使用映射关系获取 facility_code
                facility_code = unit_to_facility.get(unit_code)
                if not facility_code:
                    logger.warning(f"Unit {unit_code} 没有找到对应的 facility，跳过")
                    continue
                
                # 聚合同一 facility 下的所有 units 的数据
                for data_point in result.data:
                    timestamp = data_point.timestamp
                    value = data_point.value
                    
                    # 按时间戳分组，同一时间点的值进行求和
                    facility_data[facility_code][timestamp.isoformat()].append(value)
            
            # 对同一时间点的多个 units 的值进行求和
            aggregated_series: Dict[str, Dict[str, float]] = {}
            for facility_code, time_values in facility_data.items():
                aggregated_series[facility_code] = {}
                for timestamp, values in time_values.items():
                    # 过滤 None 值并求和
                    valid_values = [v for v in values if v is not None]
                    if valid_values:
                        aggregated_series[facility_code][timestamp] = sum(valid_values)
            
            aggregated[metric] = {
                "interval": interval,
                "unit": unit,
                "facilities": aggregated_series
            }
        
        # 统计 facilities 数量
        all_facilities = set()
        for metric_data in aggregated.values():
            facilities = metric_data.get("facilities", {})
            all_facilities.update(facilities.keys())
        
        logger.info(f"成功聚合数据: {len(aggregated)} 个 metrics, {len(all_facilities)} 个 facilities")
        return aggregated


class BatchHandler:
    """批量处理器，处理数据分片"""
    
    def __init__(self, batch_size: int, batch_delay: float):
        self.batch_size = batch_size
        self.batch_delay = batch_delay
    
    def create_batches(self, items: List[Any]) -> List[List[Any]]:
        """将列表分片"""
        batches = []
        total = len(items)
        num_batches = math.ceil(total / self.batch_size)
        
        for i in range(num_batches):
            start = i * self.batch_size
            end = min(start + self.batch_size, total)
            batches.append(items[start:end])
        
        logger.info(f"创建了 {len(batches)} 个分片 (每片最多 {self.batch_size} 个)")
        return batches
    
    def aggregate_batches(
        self,
        batch_dir: str,
        output_file: str
    ) -> None:
        """聚合所有分片数据"""
        batch_files = sorted(Path(batch_dir).glob("batch_*.json"))
        if not batch_files:
            logger.warning(f"没有找到分片文件: {batch_dir}")
            return
        
        all_data = []
        for batch_file in batch_files:
            batch_data = FileManager.load_json(str(batch_file))
            if batch_data.get("data"):
                all_data.append(batch_data)
        
        FileManager.save_json(all_data, output_file)
        logger.info(f"聚合了 {len(all_data)} 个分片到: {output_file}")


class DataValidator:
    """数据验证器"""
    
    @staticmethod
    def validate_metrics(response: TimeSeriesResponse) -> None:
        """验证指标数据响应"""
        if not response.success:
            raise ValueError(f"指标数据获取失败: {response.error}")
        
        if not response.data:
            raise ValueError("指标数据为空")
        
        logger.debug(f"验证通过: {len(response.data)} 个指标")


class FileManager:
    """文件管理器"""
    
    @staticmethod
    def save_json(data: Dict[str, Any] | List[Dict[str, Any]], filepath: str) -> None:
        """保存 JSON 数据到文件"""
        FileManager.ensure_directory(os.path.dirname(filepath))
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False, default=str)
        
        logger.debug(f"数据已保存到: {filepath}")
    
    @staticmethod
    def load_json(filepath: str) -> Dict[str, Any] | List[Dict[str, Any]]:
        """从文件加载 JSON 数据"""
        with open(filepath, 'r', encoding='utf-8') as f:
            return json.load(f)
    
    @staticmethod
    def ensure_directory(dirpath: str) -> None:
        """确保目录存在"""
        if dirpath:
            Path(dirpath).mkdir(parents=True, exist_ok=True)
    
    @staticmethod
    def save_aggregated_data(
        data: Dict[str, Any],
        filepath: str
    ) -> None:
        """
        保存聚合后的 facility 级别数据
        
        Args:
            data: 聚合后的 facility 级别数据
            filepath: 文件路径
        """
        FileManager.save_json(data, filepath)
        logger.info(f"聚合数据已保存到: {filepath}")


def _parse_data_interval(interval_str: str) -> DataInterval:
    """解析字符串为 DataInterval Literal 类型"""
    # DataInterval 是 Literal['5m', '1h', '1d', '7d', '1M', '3M', 'season', '1y', 'fy']
    # 在运行时，Literal 类型就是字符串值，直接返回即可
    # 验证字符串是否是有效的 Literal 值
    valid_intervals = ['5m', '1h', '1d', '7d', '1M', '3M', 'season', '1y', 'fy']
    
    if interval_str in valid_intervals:
        return interval_str  # type: ignore[return-value]
    else:
        # 如果不在有效值中，使用默认值 '5m'
        logger.warning(f"无效的间隔值 '{interval_str}'，使用默认值 '5m'")
        return '5m'  # type: ignore[return-value]


def fetch_and_save_network_data(
    client: OEClient,
    network_id: List[str] | None = None,
    status_id: List[UnitStatusType] | None = None,
    output_file: str = "data/facilities_data.json"
) -> FacilityResponse:
    """获取并保存网络设施数据"""
    logger.info(f"步骤1: 获取网络设施数据")
    
    response = client.get_facilities(
        network_id=network_id,
        status_id=status_id
    )
    
    # 转换为字典并保存
    data = DataConverter.facility_response_to_dict(response)
    data = DataConverter.add_metadata(data, "network")
    
    FileManager.save_json(data, output_file)
    logger.info("网络设施数据保存成功")
    
    return response


def build_facility_unit_mapping(
    client: OEClient,
    facilities_response: FacilityResponse
) -> Dict[str, List[str]]:
    """
    构建 facility-unit 映射关系
    
    Args:
        client: OEClient 实例
        facilities_response: 设施响应数据
        
    Returns:
        facility_code -> [unit_codes] 映射字典
    """
    logger.info("步骤1.5: 构建 facility-unit 映射关系")
    
    mapping = FacilityUnitMappingManager.build_mapping_from_facilities(facilities_response)
    
    # 保存映射关系到文件
    mapping_file = "data/facility_unit_mapping.json"
    FacilityUnitMappingManager.save_mapping(mapping, mapping_file)
    
    return mapping


def aggregate_batch_to_facility_level(
    batch_info: Dict[str, Any],
    facility_unit_mapping: Dict[str, List[str]]
) -> Dict[str, Any]:
    """
    将批量获取的 unit 级别数据聚合为 facility 级别数据
    
    Args:
        batch_info: 批量数据信息（包含 data 字段，data 是 TimeSeriesResponse 的字典格式）
        facility_unit_mapping: facility-unit 映射关系
        
    Returns:
        聚合后的 facility 级别数据
    """
    # 解析 batch_info 中的 TimeSeriesResponse
    data_dict = batch_info.get("data", {})
    if not data_dict:
        logger.warning(f"批量数据中没有数据，跳过聚合 (batch_index: {batch_info.get('batch_index')})")
        return {}
    
    # 从字典构建 TimeSeriesResponse
    # batch_info['data'] 是 TimeSeriesResponse.model_dump() 的结果
    try:
        timeseries_response = TimeSeriesResponse.model_validate(data_dict)
    except Exception as e:
        logger.error(f"无法解析 TimeSeriesResponse: {e}", exc_info=True)
        return {}
    
    # 使用 FacilityAggregator 进行聚合
    aggregated_data = FacilityAggregator.aggregate_to_facility_level(
        timeseries_response,
        facility_unit_mapping
    )
    
    return aggregated_data


def merge_aggregated_batches(
    aggregated_batches: List[Dict[str, Any]]
) -> Dict[str, Any]:
    """
    合并多个 batch 的聚合结果
    
    Args:
        aggregated_batches: 多个 batch 的聚合结果
        
    Returns:
        合并后的 facility 级别数据
    """
    if not aggregated_batches:
        return {}
    
    merged: Dict[str, Dict[str, Dict[str, float]]] = defaultdict(lambda: defaultdict(dict))
    
    # 合并所有 batch 的数据
    for batch_data in aggregated_batches:
        if not batch_data:
            continue
        
        # batch_data 是 {metric: {interval, unit, facilities: {...}}}
        for metric, metric_data in batch_data.items():
            facilities = metric_data.get("facilities", {})
            
            # 合并 facility 数据
            for facility_code, time_series in facilities.items():
                # 合并时间序列数据（如果有重复时间点，求和）
                for timestamp, value in time_series.items():
                    if timestamp in merged[metric][facility_code]:
                        # 如果有重复，求和
                        merged[metric][facility_code][timestamp] += value
                    else:
                        merged[metric][facility_code][timestamp] = value
    
    # 构建最终格式
    result = {}
    for metric, facilities_data in merged.items():
        # 从第一个 batch 获取 interval 和 unit（假设所有 batch 相同）
        if aggregated_batches and metric in aggregated_batches[0]:
            interval = aggregated_batches[0][metric].get("interval")
            unit = aggregated_batches[0][metric].get("unit")
        else:
            interval = None
            unit = None
        
        result[metric] = {
            "interval": interval,
            "unit": unit,
            "facilities": dict(facilities_data)
        }
    
    logger.info(f"成功合并 {len(aggregated_batches)} 个 batch 的聚合结果")
    return result


def fetch_and_save_metrics_batch(
    client: OEClient,
    facility_codes: List[str],
    network_code: NetworkCode,
    metrics: List[DataMetric],
    interval: DataInterval,
    date_start: datetime,
    date_end: datetime,
    batch_size: int,
    batch_delay: float,
    facility_unit_mapping: Dict[str, List[str]],
    aggregate_to_facility: bool = True
) -> None:
    """
    批量获取并保存设施指标数据，同时聚合为 facility 级别数据
    
    Args:
        client: OEClient 实例
        facility_codes: 设施代码列表
        network_code: 网络代码
        metrics: 指标列表
        interval: 时间间隔
        date_start: 开始时间
        date_end: 结束时间
        batch_size: 批次大小
        batch_delay: 批次延迟
        facility_unit_mapping: facility-unit 映射关系
        aggregate_to_facility: 是否聚合到 facility 级别
    """
    logger.info(f"步骤2: 批量获取设施指标数据 (共 {len(facility_codes)} 个设施)")
    
    batch_handler = BatchHandler(batch_size, batch_delay)
    batches = batch_handler.create_batches(facility_codes)
    total_batches = len(batches)
    
    # 确保输出目录存在
    FileManager.ensure_directory("data/batch_data")
    
    successful_batches = 0
    failed_batches = 0
    aggregated_batches = []  # 用于收集聚合结果
    
    for idx, batch_codes in enumerate(batches):
        logger.info(f"处理分片 {idx + 1}/{total_batches}: {len(batch_codes)} 个设施")
        
        try:
            # 调用 SDK 获取数据
            response = client.get_facility_data(
                network_code=network_code,
                facility_code=batch_codes,
                metrics=metrics,
                interval=interval,
                date_start=date_start,
                date_end=date_end
            )
            
            DataValidator.validate_metrics(response)
            
            # 转换为字典并保存原始 unit 级别数据
            data = DataConverter.timeseries_response_to_dict(response, "metrics")
            
            batch_filename = f"data/batch_data/batch_{idx + 1:03d}_metrics.json"
            batch_info = {
                "batch_index": idx + 1,
                "facility_codes": batch_codes,
                "timestamp": datetime.now().isoformat(),
                "data": data
            }
            
            FileManager.save_json(batch_info, batch_filename)
            successful_batches += 1
            logger.info(f"分片 {idx + 1} 处理成功")
            
            # 如果启用聚合，对当前 batch 进行聚合
            if aggregate_to_facility:
                try:
                    aggregated_data = aggregate_batch_to_facility_level(
                        batch_info,
                        facility_unit_mapping
                    )
                    if aggregated_data:
                        aggregated_batches.append(aggregated_data)
                        logger.info(f"分片 {idx + 1} 聚合成功")
                except Exception as e:
                    logger.error(f"分片 {idx + 1} 聚合失败: {e}")
            
        except Exception as e:
            failed_batches += 1
            logger.error(f"分片 {idx + 1} 处理失败: {e}")
        
        # 批次间延迟（最后一个分片不需要延迟）
        if idx < total_batches - 1:
            logger.info(f"等待 {batch_delay} 秒后处理下一个分片...")
            time.sleep(batch_delay)
    
    logger.info(f"分片处理完成: 成功 {successful_batches}, 失败 {failed_batches}")
    
    # 如果启用聚合，合并所有 batch 的聚合结果并保存
    if aggregate_to_facility and aggregated_batches:
        logger.info("步骤2.5: 合并所有 batch 的聚合结果...")
        try:
            merged_data = merge_aggregated_batches(aggregated_batches)
            
            if merged_data:
                # 统计 facilities 数量
                all_facilities = set()
                for metric_data in merged_data.values():
                    facilities = metric_data.get("facilities", {})
                    all_facilities.update(facilities.keys())
                
                # 添加元数据
                metadata = {
                    "source_batches": list(range(1, successful_batches + 1)),
                    "facility_count": len(all_facilities),
                    "date_range": {
                        "start": date_start.isoformat(),
                        "end": date_end.isoformat()
                    }
                }
                
                # 转换为标准格式
                formatted_data = DataConverter.aggregated_facility_data_to_dict(
                    merged_data,
                    metadata
                )
                
                # 保存聚合后的 facility 级别数据
                aggregated_file = "data/facility_metrics.json"
                FileManager.save_aggregated_data(formatted_data, aggregated_file)
                logger.info(f"聚合数据已保存到: {aggregated_file}")
            else:
                logger.warning("合并后的聚合数据为空")
        except Exception as e:
            logger.error(f"合并聚合结果失败: {e}", exc_info=True)
    
    # 聚合所有分片数据（原始功能保持不变）
    if successful_batches > 0:
        logger.info("开始聚合分片数据（原始 unit 级别数据）...")
        batch_handler.aggregate_batches(
            "data/batch_data",
            "data/facility_units_metrics.json"
        )
    else:
        logger.error("没有成功处理的分片，跳过数据聚合")


def fetch_and_save_market_data(
    client: OEClient,
    network_code: NetworkCode,
    metrics: List[MarketMetric],
    interval: DataInterval,
    date_start: datetime,
    date_end: datetime,
    primary_grouping: Optional[DataPrimaryGrouping] = None,
    network_region: Optional[str] = None,
    output_file: str = "data/market_data.json"
) -> None:
    """获取并保存市场数据"""
    logger.info(f"步骤3: 获取市场数据 (network_code: {network_code})")
    
    response = client.get_market(
        network_code=network_code,
        metrics=metrics,
        interval=interval,
        date_start=date_start,
        date_end=date_end,
        primary_grouping=primary_grouping,
        network_region=network_region
    )
    
    # 转换为字典并保存
    data = DataConverter.timeseries_response_to_dict(response, "market")
    data = DataConverter.add_metadata(data, "market")
    
    FileManager.save_json(data, output_file)
    logger.info("市场数据保存成功")


def main() -> int:
    """主函数"""
    try:
        # 加载配置
        api_key = ConfigLoader.load_api_key()
        base_url = ConfigLoader.load_base_url()
        date_start, date_end = ConfigLoader.load_time_window()
        interval_str = ConfigLoader.load_interval()
        batch_config = ConfigLoader.load_batch_config()
        network_filter = ConfigLoader.load_network_filter()
        status_filter = ConfigLoader.load_status_filter()
        
        # 创建客户端
        if base_url:
            client = OEClient(api_key=api_key, base_url=base_url)
        else:
            client = OEClient(api_key=api_key)
        
        # 确保目录存在
        FileManager.ensure_directory("data")
        FileManager.ensure_directory("data/batch_data")
        
        success_count = 0
        total_tasks = 3
        
        # 步骤1: 获取网络设施数据
        try:
            facilities_response = fetch_and_save_network_data(
                client=client,
                network_id=["NEM"],
                output_file="data/facilities_data.json"
            )
            success_count += 1
        except Exception as e:
            logger.error(f"步骤1失败: {e}", exc_info=True)
            facilities_response = None
        
        # 步骤1.5: 构建 facility-unit 映射关系
        facility_unit_mapping = {}
        if facilities_response:
            try:
                facility_unit_mapping = build_facility_unit_mapping(
                    client=client,
                    facilities_response=facilities_response
                )
                logger.info("步骤1.5完成: 映射关系构建成功")
            except Exception as e:
                logger.error(f"步骤1.5失败: {e}", exc_info=True)
                logger.warning("将继续使用空映射关系，聚合功能可能不准确")
        
        # 步骤2: 处理设施指标数据（批量）
        try:
            # 加载设施数据
            facilities_data = FileManager.load_json("data/facilities_data.json")
            facility_response = FacilityResponse.model_validate(facilities_data)
            
            # 提取设施代码
            facility_codes = FacilityCodeExtractor.extract_codes(
                facility_response,
                network_filter=network_filter,
                status_filter=status_filter
            )
            
            if not facility_codes:
                logger.warning("未提取到设施代码，使用默认值")
                facility_codes = ["BAYSW1", "ERARING"]
            
            # 转换枚举类型
            network_code = "NEM"
            interval = _parse_data_interval(interval_str)
            metrics_list = [DataMetric.POWER, DataMetric.EMISSIONS]
            
            # 批量处理（同时聚合）
            fetch_and_save_metrics_batch(
                client=client,
                facility_codes=facility_codes,
                network_code=network_code,
                metrics=metrics_list,
                interval=interval,
                date_start=date_start,
                date_end=date_end,
                batch_size=batch_config['batch_size'],
                batch_delay=batch_config['batch_delay'],
                facility_unit_mapping=facility_unit_mapping,
                aggregate_to_facility=True  # 启用聚合
            )
            success_count += 1
        except Exception as e:
            logger.error(f"步骤2失败: {e}", exc_info=True)
        
        # 步骤3: 获取市场数据
        try:
            network_code = "NEM"
            interval = _parse_data_interval(interval_str)
            metrics_list = [MarketMetric.PRICE, MarketMetric.DEMAND]
            
            fetch_and_save_market_data(
                client=client,
                network_code=network_code,
                metrics=metrics_list,
                interval=interval,
                date_start=date_start,
                date_end=date_end,
                primary_grouping=None,
                network_region=None,
                output_file="data/market_data.json"
            )
            success_count += 1
        except Exception as e:
            logger.error(f"步骤3失败: {e}", exc_info=True)
        
        # 结果汇总
        logger.info("=" * 60)
        logger.info("数据采集流程完成")
        logger.info(f"成功完成任务: {success_count}/{total_tasks}")
        logger.info(f"处理时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info("=" * 60)
        
        return 0 if success_count == total_tasks else 1
        
    except Exception as e:
        logger.error(f"程序执行失败: {e}", exc_info=True)
        return 1
    finally:
        client.close()


if __name__ == "__main__":
    exit_code = main()
    exit(exit_code)

